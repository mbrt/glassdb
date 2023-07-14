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

package middleware

import (
	"context"
	"fmt"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
)

func NewBackendLogger(inner backend.Backend, id string, tracer glassdb.Tracer) BackendLogger {
	return BackendLogger{
		inner: inner,
		id:    id,
		log:   tracer,
	}
}

type BackendLogger struct {
	inner backend.Backend
	id    string
	log   glassdb.Tracer
}

func (b BackendLogger) ReadIfModified(
	ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	r, err := b.inner.ReadIfModified(ctx, path, version)
	b.log.Tracef("%s: path=%s, event=ReadIfModified, args=v:%d, res=%v, err=%v",
		b.id, path, version, readReplyLog(r), err)
	return r, err
}

func (b BackendLogger) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	r, err := b.inner.Read(ctx, path)
	b.log.Tracef("%s: path=%s, event=Read, res=%v, err=%v", b.id, path, readReplyLog(r), err)
	return r, err
}

func (b BackendLogger) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	r, err := b.inner.GetMetadata(ctx, path)
	b.log.Tracef("%s: path=%s, event=GetMetadata, res=%+v, err=%v", b.id, path, r, err)
	return r, err
}

func (b BackendLogger) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.SetTagsIf(ctx, path, expected, t)
	b.log.Tracef("%s: path=%s, event=SetTagsIf, args=expv:%+v;t=%v, res=%+v, err=%v",
		b.id, path, expected, t, r, err)
	return r, err
}

func (b BackendLogger) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.Write(ctx, path, value, t)
	b.log.Tracef("%s: path=%s, event=Write, args=val[size]:%d;t:%v, res=%+v, err=%v",
		b.id, path, len(value), t, r, err)
	return r, err
}

func (b BackendLogger) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.WriteIf(ctx, path, value, expected, t)
	b.log.Tracef("%s: path=%s, event=WriteIf, args=val[size]:%d;expv=%+v;t=%v, res=%+v, err=%v",
		b.id, path, len(value), expected, t, r, err)
	return r, err
}

func (b BackendLogger) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.WriteIfNotExists(ctx, path, value, t)
	b.log.Tracef("%s: path=%s, event=WriteIfNotExists, args=val[size]:%d;t=%v, res=%+v, err=%v",
		b.id, path, len(value), t, r, err)
	return r, err
}

func (b BackendLogger) Delete(ctx context.Context, path string) error {
	err := b.inner.Delete(ctx, path)
	b.log.Tracef("%s: path=%s, event=Delete, err=%v", b.id, path, err)
	return err
}

func (b BackendLogger) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	err := b.inner.DeleteIf(ctx, path, expected)
	b.log.Tracef("%s: path=%s, event=DeleteIf, args=expv:%+v, err=%v", b.id, path, expected, err)
	return err
}

func (b BackendLogger) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	r, err := b.inner.List(ctx, dirPath)
	b.log.Tracef("%s: path=%s, event=List, err=%v", b.id, dirPath, err)
	return r, err
}

type readReplyLog backend.ReadReply

func (r readReplyLog) String() string {
	return fmt.Sprintf("{cont[size]=%d, v=%+v}", len(r.Contents), r.Version)
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*BackendLogger)(nil)
