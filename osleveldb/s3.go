package osleveldb

import (
	"io"
	"io/ioutil"
	"os"
	"path"

	"gopkg.in/kyokomi/aws-sdk-go.v1/aws"
	"gopkg.in/kyokomi/aws-sdk-go.v1/service/s3"
)

type S3Interface interface {
	DownloadObject(bucketName string, key string, out string) error
}

type S3Client struct {
	*s3.S3
}

func CreateS3Client(region string) client {
	s3Client := s3.New(&aws.Config{
		Credentials: aws.DefaultChainCredentials,
		Region:      region,
	})
	client := &S3Client{s3Client}
}

func (s *S3Client) DownloadObject(bucketName string, key string, out string) error {

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
