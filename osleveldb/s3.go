package osleveldb

import (
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Interface interface {
	DownloadObject(bucketName, key, out string) error
}

type S3Client struct {
	*s3.S3
}

func CreateS3Client(region string) *S3Client {
	s3Client := s3.New(&aws.Config{
		Credentials: aws.DefaultChainCredentials,
		Region:      region,
	})
	client := &S3Client{s3Client}
	return client
}

func (s *S3Client) DownloadObject(bucketName, key, out string) error {

	s3Req := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}

	res, err := s.GetObject(s3Req)
	if err != nil {
		return err
	}

	defer res.Body.Close()
	outDir := path.Dir(out)
	// create directory recursively
	err = os.MkdirAll(outDir, 0777)

	if err != nil {
		return err
	}
	output, err := os.Create(out)
	defer output.Close()
	if err != nil {
		return err
	}
	_, err = io.Copy(output, res.Body)
	return err
}
