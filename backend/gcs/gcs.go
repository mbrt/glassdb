// Package gcs implements the backend interface using Google Cloud Storage.
package gcs

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/errors"
)

// formatToken encodes a generation and metageneration into the opaque
// backend.Version token. The format is "<gen>/<metagen>".
func formatToken(gen, metagen int64) string {
	return strconv.FormatInt(gen, 10) + "/" + strconv.FormatInt(metagen, 10)
}

// parseToken decodes a token produced by formatToken back into generation and
// metageneration. Returns (0, 0, error) for an empty or malformed token.
func parseToken(token string) (gen, metagen int64, err error) {
	if token == "" {
		return 0, 0, fmt.Errorf("empty token")
	}
	before, after, ok := strings.Cut(token, "/")
	if !ok {
		return 0, 0, fmt.Errorf("invalid token %q: missing separator", token)
	}
	gen, err = strconv.ParseInt(before, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid token %q: %w", token, err)
	}
	metagen, err = strconv.ParseInt(after, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid token %q: %w", token, err)
	}
	return gen, metagen, nil
}

// versionFromAttrs constructs a backend.Version from object attributes.
func versionFromAttrs(generation, metageneration int64) backend.Version {
	return backend.Version{Token: formatToken(generation, metageneration)}
}

// TODO: Reimplement by using GCS REST APIs. The Go client is inefficient.
// See https://cloud.google.com/storage/docs/uploading-objects#uploading-an-object
// Even better, we can use GRPC only. See e.g.
// https://github.com/fsouza/fake-gcs-server/blob/4d841124144532e140ac3c3261d4a87593b6e3c1/main.go#L91-L93

var errRateLimited = errors.New("rate limited")

// New creates a Backend backed by the given GCS bucket.
func New(bucket *storage.BucketHandle) Backend {
	return Backend{bucket}
}

// Backend implements backend.Backend using Google Cloud Storage.
type Backend struct {
	bucket *storage.BucketHandle
}

// GetMetadata implements backend.Backend.
func (b Backend) GetMetadata(ctx context.Context, path string) (backend.Metadata, error) {
	attr, err := b.object(path).Attrs(ctx)
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("GetMetadata(%q): %w", path, err))
	}
	return backend.Metadata{
		Tags:    attr.Metadata,
		Version: versionFromAttrs(attr.Generation, attr.Metageneration),
	}, nil
}

// ReadIfModified implements backend.Backend.
func (b Backend) ReadIfModified(ctx context.Context,
	path string,
	expectedWriter backend.WriterID,
) (backend.ReadReply, error) {
	obj := b.object(path)
	// Fetch the current generation and tags via HEAD. We need the metadata
	// (tags) to compare the writer, which the GCS Go client only exposes via
	// the JSON Attrs API.
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("Attrs in ReadIfModified(%q): %w", path, err))
	}
	currentWriter := attrs.Metadata[backend.LastWriterTag]
	if currentWriter == backend.EncodeWriterTag(expectedWriter) {
		return backend.ReadReply{}, backend.ErrPrecondition
	}
	return b.readFromAttrs(ctx, obj, attrs)
}

func (b Backend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	obj := b.object(path)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("Attrs in Read(%q): %w", path, err))
	}
	return b.readFromAttrs(ctx, obj, attrs)
}

// SetTagsIf implements backend.Backend.
func (b Backend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	gen, metagen, err := parseToken(expected.Token)
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("SetTagsIf(%q): %w", path, err)
	}
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     gen,
		MetagenerationMatch: metagen,
	})
	attr, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: t,
	})
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("set tags: %w", err))
	}
	return backend.Metadata{
		Tags:    attr.Metadata,
		Version: versionFromAttrs(attr.Generation, attr.Metageneration),
	}, nil
}

func (b Backend) Write(ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	obj := b.object(path)
	return b.writeTo(ctx, obj, value, t)
}

// WriteIf implements backend.Backend.
func (b Backend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	gen, metagen, err := parseToken(expected.Token)
	if err != nil {
		return backend.Metadata{}, fmt.Errorf("WriteIf(%q): %w", path, err)
	}
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     gen,
		MetagenerationMatch: metagen,
	})
	return b.writeTo(ctx, obj, value, t)
}

// WriteIfNotExists implements backend.Backend.
func (b Backend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	obj := b.object(path).If(storage.Conditions{
		DoesNotExist: true,
	})
	return b.writeTo(ctx, obj, value, t)
}

// Delete implements backend.Backend.
func (b Backend) Delete(ctx context.Context, path string) error {
	if err := b.object(path).Delete(ctx); err != nil {
		return annotate(fmt.Errorf("deleting object: %w", err))
	}
	return nil
}

// DeleteIf implements backend.Backend.
func (b Backend) DeleteIf(ctx context.Context, path string, expected backend.Version) error {
	gen, metagen, err := parseToken(expected.Token)
	if err != nil {
		return fmt.Errorf("DeleteIf(%q): %w", path, err)
	}
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     gen,
		MetagenerationMatch: metagen,
	})
	if err := obj.Delete(ctx); err != nil {
		return annotate(fmt.Errorf("deleting object: %w", err))
	}
	return nil
}

// List implements backend.Backend.
func (b Backend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	iter := b.bucket.Objects(ctx, &storage.Query{
		Delimiter:                "/",
		Prefix:                   ensureTrailingSlash(dirPath),
		Projection:               storage.ProjectionNoACL,
		IncludeTrailingDelimiter: false,
	})
	return &listIter{inner: iter}, nil
}

// readFromAttrs reads the object's content for the generation described by
// attrs. A conditional GenerationMatch read ensures atomicity: if the object
// is rewritten between the Attrs call and the read, the read fails with a
// precondition error and we retry by re-fetching attrs.
func (b Backend) readFromAttrs(
	ctx context.Context,
	obj *storage.ObjectHandle,
	attrs *storage.ObjectAttrs,
) (backend.ReadReply, error) {
	for range 3 {
		condObj := obj.If(storage.Conditions{
			GenerationMatch: attrs.Generation,
		})
		reader, err := condObj.NewReader(ctx)
		if err != nil {
			if errors.Is(annotate(err), backend.ErrPrecondition) {
				// The object was rewritten between Attrs and read. Refresh.
				attrs, err = obj.Attrs(ctx)
				if err != nil {
					return backend.ReadReply{}, annotate(fmt.Errorf("re-fetching Attrs: %w", err))
				}
				continue
			}
			return backend.ReadReply{}, annotate(fmt.Errorf("opening object in read: %w", err))
		}
		buf, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return backend.ReadReply{}, annotate(fmt.Errorf("reading object: %w", err))
		}
		return backend.ReadReply{
			Contents: buf,
			Version:  versionFromAttrs(attrs.Generation, attrs.Metageneration),
			Tags:     attrs.Metadata,
		}, nil
	}
	return backend.ReadReply{}, fmt.Errorf("too many concurrent writes during read")
}

func (b Backend) writeTo(
	ctx context.Context,
	obj *storage.ObjectHandle,
	value []byte,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	writer := obj.NewWriter(ctx)
	// If tags are nil, no updates to metadata.
	// Try with no buffered reads. This is more performant, but doesn't allow
	// for automatic retries.
	writer.ChunkSize = 0
	writer.ObjectAttrs.Metadata = t
	writer.ContentType = "application/octet-stream"
	// The GCS client has a race condition on context cancellation. The object
	// is sent only partially but the request looks 100% the same as a legit
	// one. This is visible when the cancellation happens between metadata and
	// object data being sent. If this happens, the result is an empty object.
	// TODO: Fix this somehow.
	writer.SendCRC32C = true
	writer.CRC32C = crc32.Checksum(value, crc32.MakeTable(crc32.Castagnoli))

	meta, err = b.writeWith(writer, value)
	if err == nil || !errors.Is(err, errRateLimited) {
		return meta, err
	}

	// Here we were rate limited. Let's just do this again, but allow for
	// automatic retries by setting a reasonable chunk size.
	writer = obj.NewWriter(ctx)
	writer.ChunkSize = len(value) + 100
	writer.ObjectAttrs.Metadata = t
	writer.ContentType = "application/octet-stream"
	return b.writeWith(writer, value)
}

func (b Backend) writeWith(
	writer *storage.Writer,
	value []byte,
) (meta backend.Metadata, err error) {
	_, err = writer.Write(value)
	if err != nil {
		return backend.Metadata{}, annotate(err)
	}
	if err := writer.Close(); err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("writing object: %w", err))
	}

	return backend.Metadata{
		Tags:    writer.Attrs().Metadata,
		Version: versionFromAttrs(writer.Attrs().Generation, writer.Attrs().Metageneration),
	}, nil
}

func (b Backend) object(path string) *storage.ObjectHandle {
	// https://cloud.google.com/storage/docs/retry-strategy.
	return b.bucket.Object(path).Retryer(
		storage.WithBackoff(gax.Backoff{
			Initial:    200 * time.Millisecond,
			Max:        3 * time.Second,
			Multiplier: 2,
		}),
		storage.WithPolicy(storage.RetryAlways),
	)
}

func annotate(err error) error {
	if err == nil {
		return nil
	}

	var aerr *googleapi.Error
	if errors.As(err, &aerr) {
		switch aerr.Code {
		case http.StatusNotFound:
			return errors.WithCause(err, backend.ErrNotFound)
		case http.StatusPreconditionFailed:
			return errors.WithCause(err, backend.ErrPrecondition)
		case http.StatusTooManyRequests:
			return errors.WithCause(err, errRateLimited)
		default:
			return err
		}
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return backend.ErrNotFound
	}

	return err
}

func ensureTrailingSlash(a string) string {
	if len(a) > 0 && a[len(a)-1] == '/' {
		return a
	}
	return a + "/"
}

type listIter struct {
	inner *storage.ObjectIterator
	err   error
}

func (l *listIter) Next() (string, bool) {
	attrs, err := l.inner.Next()
	if err != nil {
		// Filter out iterator.Done errors.
		if !errors.Is(err, iterator.Done) {
			l.err = err
		}
		return "", false
	}
	if attrs.Prefix != "" {
		return attrs.Prefix, true
	}
	return attrs.Name, true
}

func (l *listIter) Err() error {
	return l.err
}

// Ensure that Backend interface is implemented correctly.
var (
	_ backend.Backend  = (*Backend)(nil)
	_ backend.ListIter = (*listIter)(nil)
)
