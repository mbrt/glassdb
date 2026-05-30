package testkit

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

	srv := newFakeS3Server(t)

	var httpClient aws.HTTPClient
	if logging {
		httpClient = &http.Client{Transport: logTransport{http.DefaultTransport}}
	}

	client := newS3ClientForServer(srv, httpClient)
	createS3Bucket(ctx, t, client)
	return client
}

// NewS3ClientWithTransport is like NewS3Client but wraps the HTTP transport
// talking to the fake server with wrap, letting tests inject failures (e.g.
// 503 SlowDown via SlowDownTransport) to exercise retry behavior. The
// S3TestBucket is created with an unwrapped client so setup is unaffected by
// injected failures.
func NewS3ClientWithTransport(
	ctx context.Context,
	t testing.TB,
	wrap func(inner http.RoundTripper) http.RoundTripper,
) *s3.Client {
	t.Helper()

	srv := newFakeS3Server(t)
	createS3Bucket(ctx, t, newS3ClientForServer(srv, nil))

	httpClient := &http.Client{Transport: wrap(http.DefaultTransport)}
	return newS3ClientForServer(srv, httpClient)
}

func newFakeS3Server(t testing.TB) *httptest.Server {
	t.Helper()
	faker := gofakes3.New(s3mem.New())
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)
	return srv
}

func newS3ClientForServer(srv *httptest.Server, httpClient aws.HTTPClient) *s3.Client {
	return s3.New(s3.Options{
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
}

func createS3Bucket(ctx context.Context, t testing.TB, client *s3.Client) {
	t.Helper()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(S3TestBucket),
	}); err != nil {
		t.Fatalf("creating %q bucket: %v", S3TestBucket, err)
	}
}

// NewSlowDownTransport returns a SlowDownTransport that fails the first n
// requests matching match (or all requests when match is nil) with an S3 503
// SlowDown response, then delegates to inner.
func NewSlowDownTransport(
	inner http.RoundTripper,
	n int,
	match func(*http.Request) bool,
) *SlowDownTransport {
	return &SlowDownTransport{inner: inner, remaining: n, match: match}
}

// SlowDownTransport is an http.RoundTripper that simulates S3 per-prefix
// throttling by returning 503 SlowDown for the first N matching requests
// before delegating to the wrapped transport.
type SlowDownTransport struct {
	inner     http.RoundTripper
	match     func(*http.Request) bool
	mu        sync.Mutex
	remaining int
}

// RoundTrip implements http.RoundTripper.
func (f *SlowDownTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	fail := f.remaining > 0 && (f.match == nil || f.match(req))
	if fail {
		f.remaining--
	}
	f.mu.Unlock()

	if fail {
		return slowDownResponse(req), nil
	}
	return f.inner.RoundTrip(req)
}

// Remaining reports how many matching requests are still set to fail.
func (f *SlowDownTransport) Remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remaining
}

func slowDownResponse(req *http.Request) *http.Response {
	const body = `<?xml version="1.0" encoding="UTF-8"?>` +
		`<Error><Code>SlowDown</Code>` +
		`<Message>Please reduce your request rate.</Message></Error>`
	h := make(http.Header)
	h.Set("Content-Type", "application/xml")
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Status:     "503 Slow Down",
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
