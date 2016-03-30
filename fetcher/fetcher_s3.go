package fetcher

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//S3 uses authenticated HEAD requests to poll the status of a given
//object. If it detects this file has been updated, it will perform
//an object GET and return its io.Reader stream.
type S3 struct {
	Access string
	Secret string
	Region string
	Bucket string
	Key    string
	//UseEmbeddedCert uses a hard-coded certificate (Baltimore CyberTrust Root), which
	//is the AWS root certificate, retrieved from https://www.digicert.com/digicert-root-certificates.htm.
	//Useful on devices with an old or non-existent certificate chain.
	UseEmbeddedCert bool
	//interal state
	Interval     time.Duration
	client       *s3.S3
	http         *http.Client
	initialDelay bool
	gzipFailed   bool
	lastETag     string
}

func (s *S3) Init() error {
	if s.Bucket == "" {
		return errors.New("S3 bucket not set")
	} else if s.Key == "" {
		return errors.New("S3 key not set")
	}
	if s.Region == "" {
		s.Region = "ap-southeast-2"
	}
	creds := credentials.AnonymousCredentials
	if s.Access != "" {
		creds = credentials.NewStaticCredentials(s.Access, s.Secret, "")
	} else if os.Getenv("AWS_ACCESS_KEY") != "" {
		creds = credentials.NewEnvCredentials()
	}
	config := &aws.Config{
		Credentials: creds,
		Region:      &s.Region,
	}
	if s.UseEmbeddedCert {
		config.HTTPClient = CreateHTTPSClient(BaltimoreCyberTrustRoot) //S3's root
	}
	s.client = s3.New(session.New(config))
	//apply defaults
	if s.Interval == 0 {
		s.Interval = 5 * time.Minute
	}
	return nil
}

func (s *S3) Fetch() (io.Reader, error) {
	//delay fetches after first
	if s.initialDelay {
		time.Sleep(s.Interval)
	}
	s.initialDelay = true
	//status check using HEAD
	head, err := s.client.HeadObject(&s3.HeadObjectInput{Bucket: &s.Bucket, Key: &s.Key})
	if err != nil {
		return nil, fmt.Errorf("HEAD request failed (%s)", err)
	}
	if s.lastETag == *head.ETag {
		return nil, nil //skip, file match
	}
	s.lastETag = *head.ETag
	//binary fetch using GET
	get, err := s.client.GetObject(&s3.GetObjectInput{Bucket: &s.Bucket, Key: &s.Key})
	if err != nil {
		return nil, fmt.Errorf("GET request failed (%s)", err)
	}
	reader := get.Body
	//gzip file, not gzip content-encoded -> manually de-gzip
	if !s.gzipFailed && strings.HasSuffix(s.Key, ".gz") && !strings.Contains(*get.ContentEncoding, "gzip") {
		if reader, err = gzip.NewReader(reader); err != nil {
			s.gzipFailed = true
			return nil, err
		}
	}
	//success!
	return reader, nil
}
