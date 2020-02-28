package main

import (
	"bytes"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var uploader *s3manager.Uploader
var s3service *s3.S3

// set up our S3 management objects
func init() {

	sess, err := session.NewSession()
	if err == nil {
		uploader = s3manager.NewUploader(sess)
		s3service = s3.New(sess)
	}
}

// add the buffer to a new S3 object and return the object key
func s3Add(bucket string, key string, contents []byte) error {

	contentSize := len(contents)

	log.Printf("INFO: uploading to s3://%s/%s (%d bytes)", bucket, key, contentSize)

	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(contents),
	}

	start := time.Now()

	// Perform an upload.
	_, err := uploader.Upload(upParams)
	if err != nil {
		log.Printf("ERROR: uploading to s3://%s/%s (%s)", bucket, key, err.Error())
		return err
	}

	// we validate the expected file size against the actually uploaded size
	//if int64( contentSize ) != uploadSize {
	//	return nil, fmt.Errorf("upload failure. expected %d bytes, actual %d bytes", contentSize, uploadSize)
	//}

	duration := time.Since(start)
	log.Printf("INFO: upload of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds())

	return nil
}

//
// end of file
//
