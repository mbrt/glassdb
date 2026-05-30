// Package s3 implements the backend interface using Amazon S3.
//
// Each logical key maps to a single S3 object. The user value is stored in the
// object body with an 8-byte random nonce prepended, and the lock/last-writer
// tags are stored as S3 user metadata (x-amz-meta-*). The nonce guarantees a
// fresh ETag on every PutObject, which restores compare-and-swap semantics for
// metadata-only updates (S3 ETags are otherwise the MD5 of the content). See
// docs/s3.md for the full design.
package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/errors"
)

// nonceSize is the number of random bytes prepended to every object body to
// force a unique ETag on each write.
const nonceSize = 8

// maxConflictRetries bounds how many extra times a conditional PutObject is
// retried after S3 reports a 409 ConditionalRequestConflict (see put).
const maxConflictRetries = 5

// defaultMaxRetryAttempts is how many times each S3 operation is attempted by
// default before giving up. It is intentionally higher than the SDK default
// (3) because S3 throttles a hot prefix (notably the transaction-log subtree)
// with 503 SlowDown while it reactively splits the partition, a window that
// can outlast a handful of attempts. Retries remain bounded by the caller's
// context deadline.
const defaultMaxRetryAttempts = 10

// Option configures a Backend.
type Option func(*options)

type options struct {
	maxAttempts int
	// retryer, when set, builds the aws.Retryer used for every S3 call,
	// overriding the adaptive-mode default.
	retryer func() aws.Retryer
}

// WithRetryMaxAttempts sets the maximum number of attempts per S3 operation
// (including the first). It is ignored when WithRetryer is also provided.
func WithRetryMaxAttempts(n int) Option {
	return func(o *options) { o.maxAttempts = n }
}

// WithRetryer overrides the retry strategy applied to every S3 operation. Pass
// a factory returning aws.NopRetryer{} to disable backend-level retries.
func WithRetryer(f func() aws.Retryer) Option {
	return func(o *options) { o.retryer = f }
}

// New creates a Backend backed by the given S3 client and bucket.
//
// By default every operation uses an adaptive retryer that backs off and
// retries throttling/transient errors (503 SlowDown, 5xx) and applies a
// client-side rate limit once throttling is observed. This retryer is applied
// per call, so it takes effect regardless of how the injected client was
// configured. Use WithRetryMaxAttempts or WithRetryer to tune it.
func New(client *s3.Client, bucket string, opts ...Option) Backend {
	o := options{maxAttempts: defaultMaxRetryAttempts}
	for _, opt := range opts {
		opt(&o)
	}

	var retryer aws.Retryer
	if o.retryer != nil {
		retryer = o.retryer()
	} else {
		// Adaptive mode wraps the standard retryer (whose retryable set already
		// covers 503 SlowDown and other 5xx) with a shared, concurrency-safe
		// token bucket that proactively slows requests when throttling is seen.
		retryer = retry.NewAdaptiveMode(func(am *retry.AdaptiveModeOptions) {
			am.StandardOptions = append(am.StandardOptions, func(s *retry.StandardOptions) {
				s.MaxAttempts = o.maxAttempts
			})
		})
	}

	return Backend{client: client, bucket: bucket, retryer: retryer}
}

// Backend implements backend.Backend using Amazon S3.
type Backend struct {
	client *s3.Client
	bucket string
	// retryer is shared across all operations: adaptive mode's rate limiter
	// only works when the same instance observes every request.
	retryer aws.Retryer
}

// applyOpts is passed as a per-operation option to every S3 call so they share
// the Backend's retryer instead of the client's default one.
func (b Backend) applyOpts(o *s3.Options) {
	if b.retryer != nil {
		o.Retryer = b.retryer
	}
}

// Read implements backend.Backend.
func (b Backend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("Read(%q): %w", path, err))
	}
	value, err := readBody(out.Body)
	if err != nil {
		return backend.ReadReply{}, fmt.Errorf("Read(%q): %w", path, err)
	}
	return backend.ReadReply{
		Contents: value,
		Version:  versionFromETag(out.ETag),
		Tags:     tagsFromMeta(out.Metadata),
	}, nil
}

// ReadIfModified implements backend.Backend.
func (b Backend) ReadIfModified(
	ctx context.Context,
	path string,
	expectedWriter backend.WriterID,
) (backend.ReadReply, error) {
	// Compare the writer recorded in the tags via a HEAD before downloading.
	// With nonce-in-content the ETag changes on every write, so it cannot be
	// used to detect "unchanged"; the last-writer tag is the source of truth.
	head, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("ReadIfModified(%q): %w", path, err))
	}
	if head.Metadata[backend.LastWriterTag] == backend.EncodeWriterTag(expectedWriter) {
		return backend.ReadReply{}, backend.ErrPrecondition
	}
	return b.Read(ctx, path)
}

// GetMetadata implements backend.Backend.
func (b Backend) GetMetadata(ctx context.Context, path string) (backend.Metadata, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("GetMetadata(%q): %w", path, err))
	}
	return backend.Metadata{
		Tags:    tagsFromMeta(out.Metadata),
		Version: versionFromETag(out.ETag),
	}, nil
}

// SetTagsIf implements backend.Backend.
//
// S3 has no metadata-only update, so the object must be re-uploaded. The
// existing tags are preserved (notably last-writer) and the new tags are
// overlaid on top, matching the merge semantics of the other backends. A fresh
// nonce ensures the ETag changes so the conditional write provides real CAS.
func (b Backend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("SetTagsIf(%q): %w", path, err))
	}
	value, err := readBody(out.Body)
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("SetTagsIf(%q): %w", path, err)
	}
	merged := make(map[string]string, len(out.Metadata)+len(t))
	maps.Copy(merged, out.Metadata)
	maps.Copy(merged, t)

	meta, err := b.put(ctx, path, value, merged, putConds{ifMatch: &expected.Token})
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("SetTagsIf(%q): %w", path, err)
	}
	return meta, nil
}

// Write implements backend.Backend.
func (b Backend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := b.put(ctx, path, value, t, putConds{})
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("Write(%q): %w", path, err)
	}
	return meta, nil
}

// WriteIf implements backend.Backend.
func (b Backend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := b.put(ctx, path, value, t, putConds{ifMatch: &expected.Token})
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("WriteIf(%q): %w", path, err)
	}
	return meta, nil
}

// WriteIfNotExists implements backend.Backend.
func (b Backend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := b.put(ctx, path, value, t, putConds{ifNoneMatch: true})
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("WriteIfNotExists(%q): %w", path, err)
	}
	return meta, nil
}

// Delete implements backend.Backend.
func (b Backend) Delete(ctx context.Context, path string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return annotate(fmt.Errorf("Delete(%q): %w", path, err))
	}
	return nil
}

// DeleteIf implements backend.Backend.
//
// S3 has no conditional delete, so this is a HEAD-then-DELETE. The TOCTOU
// window between the two calls is documented in docs/s3.md and covered by the
// transaction algorithm's recovery.
func (b Backend) DeleteIf(ctx context.Context, path string, expected backend.Version) error {
	head, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts)
	if err != nil {
		return annotate(fmt.Errorf("DeleteIf(%q): %w", path, err))
	}
	if versionFromETag(head.ETag) != expected {
		return backend.ErrPrecondition
	}
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.bucket,
		Key:    &path,
	}, b.applyOpts); err != nil {
		return annotate(fmt.Errorf("DeleteIf(%q): %w", path, err))
	}
	return nil
}

// List implements backend.Backend.
//
// The returned iterator pages through ListObjectsV2 lazily, fetching the next
// page only when the current one is exhausted, so a large directory is never
// materialised in full.
func (b Backend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	return &listIter{
		ctx:       ctx,
		client:    b.client,
		bucket:    b.bucket,
		prefix:    ensureTrailingSlash(dirPath),
		applyOpts: b.applyOpts,
	}, nil
}

// putConds describes the optional conditional headers for a PutObject.
type putConds struct {
	ifMatch     *string
	ifNoneMatch bool
}

func (b Backend) put(
	ctx context.Context,
	path string,
	value []byte,
	t map[string]string,
	conds putConds,
) (backend.Metadata, error) {
	if conds.ifMatch != nil && *conds.ifMatch == "" {
		// An empty token can never match a stored ETag, and S3 rejects an empty
		// If-Match header outright. Treat it as a failed precondition rather
		// than risk sending a malformed request or an unconditional overwrite.
		// The storage layer never reaches here with a null version
		// (Locker.UpdateLock guards that), so this is purely defensive.
		return backend.Metadata{}, backend.ErrPrecondition
	}
	payload, err := addNonce(value)
	if err != nil {
		return backend.Metadata{}, err
	}
	in := &s3.PutObjectInput{
		Bucket:   &b.bucket,
		Key:      &path,
		Metadata: t,
	}
	if conds.ifMatch != nil {
		in.IfMatch = conds.ifMatch
	}
	if conds.ifNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}

	// Two retry layers compose here. The Backend retryer (applyOpts) handles
	// transient/throttling failures (503 SlowDown, 5xx) inside each PutObject
	// call. This outer loop handles 409 ConditionalRequestConflict, which the
	// SDK does not treat as retryable, by re-issuing the request.
	for attempt := 0; ; attempt++ {
		// PutObject consumes the body, so hand it a fresh reader each attempt.
		in.Body = bytes.NewReader(payload)
		out, err := b.client.PutObject(ctx, in, b.applyOpts)
		if err == nil {
			return backend.Metadata{
				Tags:    tagsFromMeta(t),
				Version: versionFromETag(out.ETag),
			}, nil
		}
		// A 409 ConditionalRequestConflict means a concurrent conditional write
		// to the same key raced with this one. Unlike a 412 PreconditionFailed
		// (the expected version genuinely differs), it is transient: the write
		// may well succeed once the other request settles, so we retry.
		if isConflict(err) && attempt < maxConflictRetries {
			if werr := sleep(ctx, conflictBackoff(attempt)); werr != nil {
				return backend.Metadata{}, werr
			}
			continue
		}
		return backend.Metadata{}, annotate(err)
	}
}

// addNonce prepends nonceSize random bytes to value.
func addNonce(value []byte) ([]byte, error) {
	buf := make([]byte, nonceSize+len(value))
	if _, err := rand.Read(buf[:nonceSize]); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}
	copy(buf[nonceSize:], value)
	return buf, nil
}

// readBody reads the full object body and strips the leading nonce.
func readBody(r io.ReadCloser) ([]byte, error) {
	defer r.Close()
	stored, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading object body: %w", err)
	}
	if len(stored) < nonceSize {
		return nil, fmt.Errorf("object body too short (%d bytes) to contain nonce", len(stored))
	}
	return stored[nonceSize:], nil
}

// versionFromETag builds an opaque backend.Version from an S3 ETag. The ETag
// is kept verbatim (quotes included) and treated as an opaque CAS token.
func versionFromETag(etag *string) backend.Version {
	if etag == nil {
		return backend.Version{}
	}
	return backend.Version{Token: *etag}
}

// tagsFromMeta converts S3 user metadata into backend.Tags, returning nil when
// empty to match the other backends.
func tagsFromMeta(m map[string]string) backend.Tags {
	if len(m) == 0 {
		return nil
	}
	res := make(backend.Tags, len(m))
	maps.Copy(res, m)
	return res
}

func annotate(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return errors.WithCause(err, backend.ErrPrecondition)
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return errors.WithCause(err, backend.ErrNotFound)
		}
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		switch re.HTTPStatusCode() {
		case http.StatusNotFound:
			return errors.WithCause(err, backend.ErrNotFound)
		case http.StatusPreconditionFailed, http.StatusConflict:
			return errors.WithCause(err, backend.ErrPrecondition)
		}
	}
	return err
}

// isConflict reports whether err is a 409 ConditionalRequestConflict, which S3
// returns when concurrent conditional writes to the same key race. It is
// distinct from a 412 PreconditionFailed and should be retried.
func isConflict(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "ConditionalRequestConflict" {
		return true
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode() == http.StatusConflict
	}
	return false
}

// conflictBackoff returns the delay before the given (zero-based) conflict
// retry attempt: an exponential ramp from 25ms, capped at one second.
func conflictBackoff(attempt int) time.Duration {
	d := 25 * time.Millisecond << attempt
	if d > time.Second {
		return time.Second
	}
	return d
}

// sleep waits for d or until ctx is done, returning ctx.Err() in the latter case.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func ensureTrailingSlash(a string) string {
	if len(a) > 0 && a[len(a)-1] == '/' {
		return a
	}
	return a + "/"
}

// listIter lazily pages through ListObjectsV2, holding at most one page in
// memory at a time. S3 returns keys in lexicographic order across pages, so
// merging each page's Contents and CommonPrefixes (which arrive in separate
// arrays) is enough to keep the overall stream sorted.
type listIter struct {
	ctx       context.Context
	client    *s3.Client
	bucket    string
	prefix    string
	applyOpts func(*s3.Options)

	token *string // continuation token for the next page
	page  []string
	pos   int
	done  bool
	err   error
}

func (l *listIter) Next() (string, bool) {
	for l.pos >= len(l.page) {
		if l.err != nil || l.done {
			return "", false
		}
		l.fetch()
	}
	res := l.page[l.pos]
	l.pos++
	return res, true
}

// fetch loads the next page into l.page, recording any error and whether the
// listing is exhausted.
func (l *listIter) fetch() {
	out, err := l.client.ListObjectsV2(l.ctx, &s3.ListObjectsV2Input{
		Bucket:            &l.bucket,
		Prefix:            &l.prefix,
		Delimiter:         aws.String("/"),
		ContinuationToken: l.token,
	}, l.applyOpts)
	if err != nil {
		l.err = annotate(fmt.Errorf("List(%q): %w", l.prefix, err))
		return
	}
	page := make([]string, 0, len(out.Contents)+len(out.CommonPrefixes))
	for _, p := range out.CommonPrefixes {
		if p.Prefix != nil {
			page = append(page, *p.Prefix)
		}
	}
	for _, o := range out.Contents {
		if o.Key != nil {
			page = append(page, *o.Key)
		}
	}
	sort.Strings(page)
	l.page = page
	l.pos = 0
	if out.IsTruncated != nil && *out.IsTruncated {
		l.token = out.NextContinuationToken
	} else {
		l.done = true
		l.token = nil
	}
}

func (l *listIter) Err() error {
	return l.err
}

// Ensure that Backend interface is implemented correctly.
var (
	_ backend.Backend  = (*Backend)(nil)
	_ backend.ListIter = (*listIter)(nil)
)
