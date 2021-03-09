package main

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Writer struct {
	io.WriteCloser
}

var s3Client *s3.S3

func NewSession(region string) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})

	if err != nil {
		return err
	}

	s3Client = s3.New(sess)
	return nil
}

func NewS3Writer(region string, bucket string, objectKey string) (*S3Writer, error) {

	s3Writer := S3Writer{}

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}

	output, err := s3Client.CreateMultipartUpload(input)
	if err != nil {
		return nil, err
	}

	// TODO

	return nil, nil
}

func (w *S3Writer) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (w *S3Writer) Close() error {
	panic("not implemented") // TODO: Implement
}
