package testkit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// S3TestBucket is the bucket name created by NewS3Client.
const S3TestBucket = "test"

// NewS3Client creates an S3 client backed by an in-memory fake server
// (gofakes3) for testing, and creates the S3TestBucket. The fake supports the
// conditional writes (If-Match / If-None-Match) the s3 backend relies on.
func NewS3Client(ctx context.Context, t testing.TB, logging bool) *s3.Client {
	t.Helper()

	faker := gofakes3.New(s3mem.New())
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)

	var httpClient aws.HTTPClient
	if logging {
		httpClient = &http.Client{Transport: logTransport{http.DefaultTransport}}
	}

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(srv.URL),
		// The fake only understands path-style addressing (no virtual host).
		UsePathStyle: true,
		// gofakes3 does not parse the aws-chunked trailing checksums the SDK
		// adds by default, so only send/validate checksums when an operation
		// strictly requires them.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
		HTTPClient:                 httpClient,
	})

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(S3TestBucket),
	}); err != nil {
		t.Fatalf("creating %q bucket: %v", S3TestBucket, err)
	}

	return client
}
