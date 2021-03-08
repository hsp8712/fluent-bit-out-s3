package main

import "io"

type S3Writer struct {
	io.WriteCloser
}

func NewS3Writer(bucket string, objectKey string) (*S3Writer, error) {
	// TODO
	return nil, nil
}

func (w *S3Writer) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (w *S3Writer) Close() error {
	panic("not implemented") // TODO: Implement
}
