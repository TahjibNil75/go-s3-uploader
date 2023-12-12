package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
)

// Constants defining AWS S3 details and file-related parameters
const (
	BucketName  = "your-bucket-name"
	REGION      = "AWS_REGION"
	FILE        = "/200MB.zip"
	PartSize    = 50_000_000
	RETRIES     = 2
	SNSTopicARN = "arn:aws:sns:your-region:your-account-id:your-sns-topic-name"
)

// Global variable to hold the AWS S3 session
var s3session *s3.S3

// Struct to store the result of a part upload
type partUploadResult struct {
	completedPart *s3.CompletedPart
	err           error
}

// Global variables for wait group and channel synchronization
var wg = sync.WaitGroup{}
var ch = make(chan partUploadResult)

// Initialization function to set up the AWS S3 session
func init() {
	// Create a new AWS S3 session
	s3session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(REGION),
	})))
}

// The main function, the entry point of the program
func main() {
	// Get the current working directory and open the file for upload
	currentDirectory, _ := os.Getwd()
	file, _ := os.Open(currentDirectory + "/AWS/S3" + FILE)
	defer file.Close()

	// Get file information and read its content into a buffer
	stat, _ := file.Stat()
	fileSize := stat.Size()
	buffer := make([]byte, fileSize)
	_, _ = file.Read(buffer)

	// Set an expiry date for the S3 upload
	expiryDate := time.Now().AddDate(0, 0, 1)

	// Initiate a multipart upload and handle any errors
	createdResp, err := s3session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:  aws.String(BucketName),
		Key:     aws.String("TestVideo"),
		Expires: &expiryDate,
	})
	if err != nil {
		fmt.Print(err)
		return
	}

	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	var completedParts []*s3.CompletedPart

	// Iterate over file parts and initiate parallel uploads
	for start = 0; remaining > 0; start += PartSize {
		wg.Add(1)
		if remaining < PartSize {
			currentSize = remaining
		} else {
			currentSize = PartSize
		}
		// Start a goroutine to upload a part to S3
		go uploadToS3(createdResp, buffer[start:start+currentSize], partNum, &wg)

		remaining -= currentSize
		fmt.Printf("Uplaodind of part %v started and remaning is %v \n", partNum, remaining)
		partNum++
	}

	// Close the channel when all uploads are finished
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Process the results from the channel and handle errors
	for result := range ch {
		if result.err != nil {
			// Abort the multipart upload in case of an error and handle any errors
			_, err = s3session.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(BucketName),
				Key:      aws.String(file.Name()),
				UploadId: createdResp.UploadId,
			})
			if err != nil {
				fmt.Print(err)
				os.Exit(1)
			}
			// Notify on upload failure using SNS
			sendSNSNotification("Upload Failed", fmt.Sprintf("Error: %v", result.err))
		} else {
			fmt.Printf("Uploading of part %v has been finished \n", *result.completedPart.PartNumber)
			completedParts = append(completedParts, result.completedPart)
		}
	}

	// Order the array based on the PartNumber as each part could be uploaded in a different order
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	if len(completedParts) == 0 {
		// Notify on upload failure if no parts were successfully uploaded
		sendSNSNotification("Upload Failed", "No parts were successfully uploaded.")
		return
	}

	// Signal AWS S3 that the multipart upload is finished
	resp, err := s3session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdResp.Bucket,
		Key:      createdResp.Key,
		UploadId: createdResp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	if err != nil {
		fmt.Print(err)
		// Notify on upload failure using SNS
		sendSNSNotification("Upload Failed", fmt.Sprintf("Error completing upload: %v", err))
		return
	} else {
		fmt.Println(resp.String())
		// Notify on successful upload using SNS
		sendSNSNotification("Upload Successful", "Multipart upload completed successfully.")
	}
}

// Function to upload a part to AWS S3
func uploadToS3(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int, wg *sync.WaitGroup) {
	defer wg.Done()
	var try int
	fmt.Printf("Uploading %v \n", len(fileBytes))
	for try <= RETRIES {
		uploadRes, err := s3session.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		if err != nil {
			fmt.Println(err)
			if try == RETRIES {
				ch <- partUploadResult{nil, err}
				return
			} else {
				try++
				time.Sleep(time.Duration(time.Second * 15))
			}
		} else {
			ch <- partUploadResult{
				&s3.CompletedPart{
					ETag:       uploadRes.ETag,
					PartNumber: aws.Int64(int64(partNum)),
				}, nil,
			}
			return
		}
	}
	ch <- partUploadResult{}
}

// Function to send SNS notifications
func sendSNSNotification(subject, message string) {
	// Create a new session for SNS
	snsSession := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(REGION),
	}))

	// Create an SNS client
	snsClient := sns.New(snsSession)

	// Publish a message to the specified SNS topic
	_, err := snsClient.Publish(&sns.PublishInput{
		Message:  aws.String(message),
		Subject:  aws.String(subject),
		TopicArn: aws.String(SNSTopicARN),
	})

	if err != nil {
		fmt.Printf("Error sending SNS notification: %v\n", err)
	}
}
