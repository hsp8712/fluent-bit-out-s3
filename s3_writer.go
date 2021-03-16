package main

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Writer for write object with multiparts
// Not thread-safe
type S3Writer struct {
	io.WriteCloser
	client          *s3.Client
	region          string
	bucket          string
	key             string
	partSize        int
	partNumber      int
	uploadID        *string
	multipartUpload *types.CompletedMultipartUpload
	buf             *bytes.Buffer
}

// NewS3Writer new s3 writer
func NewS3Writer(region string, bucket string, key string, partSize int) (*S3Writer, error) {
	// new s3 client

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))

	if err != nil {
		return nil, err
	}

	// Create an Amazon S3 service client
	s3Client := s3.NewFromConfig(cfg)

	s3Writer := S3Writer{
		client:          s3Client,
		region:          region,
		bucket:          bucket,
		key:             key,
		partSize:        partSize,
		partNumber:      1,
		multipartUpload: &types.CompletedMultipartUpload{},
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s3Writer.bucket),
		Key:    aws.String(s3Writer.key),
	}

	output, err := s3Writer.client.CreateMultipartUpload(context.TODO(), input)

	if err != nil {
		return nil, err
	}

	s3Writer.uploadID = output.UploadId
	return &s3Writer, nil
}

func (w *S3Writer) uploadPart() error {
	curPartNum := int32(w.partNumber)
	body := bytes.NewReader(w.buf.Bytes())
	input := &s3.UploadPartInput{
		Bucket:     &w.bucket,
		Key:        &w.key,
		PartNumber: curPartNum,
		UploadId:   w.uploadID,
		Body:       body,
	}
	output, err := w.client.UploadPart(context.TODO(), input)
	if err != nil {
		return err
	}
	part := types.CompletedPart{
		ETag:       output.ETag,
		PartNumber: curPartNum,
	}
	w.multipartUpload.Parts = append(w.multipartUpload.Parts, part)
	w.partNumber++
	w.buf = &bytes.Buffer{}
	return nil
}

// Write bytes
func (w *S3Writer) Write(p []byte) (n int, err error) {
	if w.buf == nil {
		w.buf = &bytes.Buffer{}
	}

	if w.buf.Len() >= w.partSize {
		err = w.uploadPart()
		if err != nil {
			return 0, err
		}
	}

	n, err = w.buf.Write(p)
	return
}

// Close s3 writer
func (w *S3Writer) Close() error {
	if w.buf.Len() > 0 {
		w.uploadPart()
	}

	var err error
	if len(w.multipartUpload.Parts) > 0 {
		input := &s3.CompleteMultipartUploadInput{
			Bucket:          &w.bucket,
			Key:             &w.key,
			UploadId:        w.uploadID,
			MultipartUpload: w.multipartUpload,
		}
		_, err = w.client.CompleteMultipartUpload(context.TODO(), input)

	} else {
		// No parts to complete, abort
		input := &s3.AbortMultipartUploadInput{
			Bucket:   &w.bucket,
			Key:      &w.key,
			UploadId: w.uploadID,
		}
		_, err = w.client.AbortMultipartUpload(context.TODO(), input)
	}
	return err
}

/*
Example Code:

func main() {
	w, err := NewS3Writer("us-west-2", "emr-test-flow", "test.txt", 5*1024*1024)
	defer w.Close()

	if err != nil {
		log.Fatal(err)
		return
	}

	sum := 0
	for i := 0; i < 1024*1024; i++ {
		n, err := w.Write([]byte("aaaa\n"))
		if err != nil {
			log.Fatal(err)
			return
		}
		sum += n
	}

	for i := 0; i < 1024*1024; i++ {
		n, err := w.Write([]byte("bbbbb\n"))
		if err != nil {
			log.Fatal(err)
			return
		}
		sum += n
	}

	log.Println("write len: " + strconv.Itoa(sum))
}
*/
