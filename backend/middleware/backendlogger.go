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
	"log/slog"

	"github.com/mbrt/glassdb/backend"
)

func NewBackendLogger(inner backend.Backend, id string, log *slog.Logger) BackendLogger {
	return BackendLogger{
		inner: inner,
		log:   log.With("backend-id", id),
	}
}

type BackendLogger struct {
	inner backend.Backend
	log   *slog.Logger
}

func (b BackendLogger) ReadIfModified(
	ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	r, err := b.inner.ReadIfModified(ctx, path, version)
	b.log.LogAttrs(ctx, slog.LevelDebug, "ReadIfModified",
		argsAttr("v:%d", version), pathAttr(path), resAttr(r), errAttr(err))
	return r, err
}

func (b BackendLogger) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	r, err := b.inner.Read(ctx, path)
	b.log.LogAttrs(ctx, slog.LevelDebug, "Read", pathAttr(path), resAttr(r), errAttr(err))
	return r, err
}

func (b BackendLogger) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	r, err := b.inner.GetMetadata(ctx, path)
	b.log.LogAttrs(ctx, slog.LevelDebug, "GetMetadata", pathAttr(path), resFmtAttr("%+v", r), errAttr(err))
	return r, err
}

func (b BackendLogger) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.SetTagsIf(ctx, path, expected, t)
	b.log.LogAttrs(ctx, slog.LevelDebug, "SetTagsIf", pathAttr(path),
		argsAttr("expv:%+v;t:%v", expected, t), resFmtAttr("%+v", r), errAttr(err))
	return r, err
}

func (b BackendLogger) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.Write(ctx, path, value, t)
	b.log.LogAttrs(ctx, slog.LevelDebug, "Write", pathAttr(path),
		argsAttr("val[size]:%d;t:%v", len(value), t), resFmtAttr("%+v", r), errAttr(err))
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
	b.log.LogAttrs(ctx, slog.LevelDebug, "WriteIf", pathAttr(path),
		argsAttr("val[size]:%d;expv=%+v;t:%v", len(value), expected, t),
		resFmtAttr("%+v", r), errAttr(err))
	return r, err
}

func (b BackendLogger) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	r, err := b.inner.WriteIfNotExists(ctx, path, value, t)
	b.log.LogAttrs(ctx, slog.LevelDebug, "WriteIfNotExists", pathAttr(path),
		argsAttr("val[size]:%d;t:%v", len(value), t),
		resFmtAttr("%+v", r), errAttr(err))
	return r, err
}

func (b BackendLogger) Delete(ctx context.Context, path string) error {
	err := b.inner.Delete(ctx, path)
	b.log.LogAttrs(ctx, slog.LevelDebug, "Delete", pathAttr(path), errAttr(err))
	return err
}

func (b BackendLogger) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	err := b.inner.DeleteIf(ctx, path, expected)
	b.log.LogAttrs(ctx, slog.LevelDebug, "DeleteIf", pathAttr(path),
		argsAttr("expv:%+v", expected), errAttr(err))
	return err
}

func (b BackendLogger) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	r, err := b.inner.List(ctx, dirPath)
	b.log.LogAttrs(ctx, slog.LevelDebug, "List", pathAttr(dirPath), errAttr(err))
	return r, err
}

type readReplyLog backend.ReadReply

func (r readReplyLog) LogValue() slog.Value {
	return slog.StringValue(fmt.Sprintf("{cont[size]=%d, v=%+v}", len(r.Contents), r.Version))
}

func resAttr(r backend.ReadReply) slog.Attr {
	return slog.Attr{
		Key:   "res",
		Value: slog.AnyValue(readReplyLog(r)),
	}
}

func resFmtAttr(format string, v ...any) slog.Attr {
	return slog.Attr{
		Key:   "res",
		Value: slog.StringValue(fmt.Sprintf(format, v...)),
	}
}

func errAttr(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.Attr{
		Key:   "err",
		Value: slog.StringValue(err.Error()),
	}
}

func pathAttr(p string) slog.Attr {
	return slog.Attr{
		Key:   "path",
		Value: slog.StringValue(p),
	}
}

func argsAttr(format string, v ...any) slog.Attr {
	return slog.Attr{
		Key:   "args",
		Value: slog.StringValue(fmt.Sprintf(format, v...)),
	}
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*BackendLogger)(nil)
