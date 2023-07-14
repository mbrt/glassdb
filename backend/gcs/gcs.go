// Copyright 2023 The glassdb Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/errors"
)

var errRateLimited = errors.New("rate limited")

func New(bucket *storage.BucketHandle) Backend {
	return Backend{bucket}
}

type Backend struct {
	bucket *storage.BucketHandle
}

func (b Backend) GetMetadata(ctx context.Context, path string) (backend.Metadata, error) {
	attr, err := b.object(path).Attrs(ctx)
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("GetMetadata(%q): %w", path, err))
	}
	return backend.Metadata{
		Tags: attr.Metadata,
		Version: backend.Version{
			Contents: attr.Generation,
			Meta:     attr.Metageneration,
		},
	}, nil
}

func (b Backend) ReadIfModified(ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	obj := b.object(path).If(storage.Conditions{
		GenerationNotMatch: version,
	})
	return b.readFrom(ctx, obj)
}

func (b Backend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	obj := b.object(path)
	return b.readFrom(ctx, obj)
}

func (b Backend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     expected.Contents,
		MetagenerationMatch: expected.Meta,
	})
	attr, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: t,
	})
	if err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("set tags: %w", err))
	}
	return backend.Metadata{
		Tags: attr.Metadata,
		Version: backend.Version{
			Contents: attr.Generation,
			Meta:     attr.Metageneration,
		},
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

func (b Backend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     expected.Contents,
		MetagenerationMatch: expected.Meta,
	})
	return b.writeTo(ctx, obj, value, t)
}

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

func (b Backend) Delete(ctx context.Context, path string) error {
	if err := b.object(path).Delete(ctx); err != nil {
		return annotate(fmt.Errorf("deleting object: %w", err))
	}
	return nil
}

func (b Backend) DeleteIf(ctx context.Context, path string, expected backend.Version) error {
	obj := b.object(path).If(storage.Conditions{
		GenerationMatch:     expected.Contents,
		MetagenerationMatch: expected.Meta,
	})
	if err := obj.Delete(ctx); err != nil {
		return annotate(fmt.Errorf("deleting object: %w", err))
	}
	return nil
}

func (b Backend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	iter := b.bucket.Objects(ctx, &storage.Query{
		Delimiter:                "/",
		Prefix:                   ensureTrailingSlash(dirPath),
		Projection:               storage.ProjectionNoACL,
		IncludeTrailingDelimiter: false,
	})
	return &listIter{inner: iter}, nil
}

func (b Backend) readFrom(
	ctx context.Context,
	obj *storage.ObjectHandle,
) (backend.ReadReply, error) {
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("opening object in read: %w", err))
	}
	defer reader.Close()

	buf, err := io.ReadAll(reader)
	if err != nil {
		return backend.ReadReply{}, annotate(fmt.Errorf("reading object: %w", err))
	}

	return backend.ReadReply{
		Contents: buf,
		Version: backend.Version{
			Contents: reader.Attrs.Generation,
			Meta:     reader.Attrs.Metageneration,
		},
	}, nil
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

	meta, err = b.writeWith(ctx, writer, value, t)
	if err == nil || !errors.Is(err, errRateLimited) {
		return meta, err
	}

	// Here we were rate limited. Let's just do this again, but allow for
	// automatic retries by setting a reasonable chunk size.
	writer = obj.NewWriter(ctx)
	writer.ChunkSize = len(value) + 100
	writer.ObjectAttrs.Metadata = t
	writer.ContentType = "application/octet-stream"
	return b.writeWith(ctx, writer, value, t)
}

func (b Backend) writeWith(
	ctx context.Context,
	writer *storage.Writer,
	value []byte,
	t backend.Tags,
) (meta backend.Metadata, err error) {
	_, err = writer.Write(value)
	if err != nil {
		writer.Close()
		return backend.Metadata{}, annotate(err)
	}
	if err := writer.Close(); err != nil {
		return backend.Metadata{}, annotate(fmt.Errorf("writing object: %w", err))
	}

	return backend.Metadata{
		Tags: writer.Attrs().Metadata,
		Version: backend.Version{
			Contents: writer.Attrs().Generation,
			Meta:     writer.Attrs().Metageneration,
		},
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
