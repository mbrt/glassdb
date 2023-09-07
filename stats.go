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

package glassdb

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/mbrt/glassdb/backend"
)

type Stats struct {
	// Transactions statistics.
	TxN       int           // number of completed transactions.
	TxTime    time.Duration // time spent within transactions.
	TxReads   int           // number of reads.
	TxWrites  int           // number of writes.
	TxRetries int           // number of retried transactions.

	// Backend statistics.
	MetaReads  int // number of read metadata.
	MetaWrites int // number of write metadata.
	ObjReads   int // number of read objects.
	ObjWrites  int // number of written objects.
	ObjLists   int // number of list calls.
}

// Sub calculates and returns the difference between two sets of transaction
// stats. This is useful when obtaining stats at two different points in time
// and you need the performance counters occurred within that time span.
func (s Stats) Sub(other Stats) Stats {
	return Stats{
		TxN:       s.TxN - other.TxN,
		TxTime:    s.TxTime - other.TxTime,
		TxReads:   s.TxReads - other.TxReads,
		TxWrites:  s.TxWrites - other.TxWrites,
		TxRetries: s.TxRetries - other.TxRetries,

		MetaReads:  s.MetaReads - other.MetaReads,
		MetaWrites: s.MetaWrites - other.MetaWrites,
		ObjReads:   s.ObjReads - other.ObjReads,
		ObjWrites:  s.ObjWrites - other.ObjWrites,
		ObjLists:   s.ObjLists - other.ObjLists,
	}
}

func (s *Stats) add(other *Stats) {
	s.TxN += other.TxN
	s.TxTime += other.TxTime
	s.TxReads += other.TxReads
	s.TxWrites += other.TxWrites
	s.TxRetries += other.TxRetries

	s.MetaReads += other.MetaReads
	s.MetaWrites += other.MetaWrites
	s.ObjReads += other.ObjReads
	s.ObjWrites += other.ObjWrites
	s.ObjLists += other.ObjLists
}

type statsBackend struct {
	inner backend.Backend

	metaReads  int32
	metaWrites int32
	objReads   int32
	objWrites  int32
	objLists   int32
}

// StatsAndReset returns backend stats and resets them.
func (b *statsBackend) StatsAndReset() Stats {
	return Stats{
		MetaReads:  int(atomic.SwapInt32(&b.metaReads, 0)),
		MetaWrites: int(atomic.SwapInt32(&b.metaWrites, 0)),
		ObjReads:   int(atomic.SwapInt32(&b.objReads, 0)),
		ObjWrites:  int(atomic.SwapInt32(&b.objWrites, 0)),
		ObjLists:   int(atomic.SwapInt32(&b.objLists, 0)),
	}
}

func (b *statsBackend) ReadIfModified(
	ctx context.Context, path string, version int64,
) (backend.ReadReply, error) {
	atomic.AddInt32(&b.objReads, 1)
	return b.inner.ReadIfModified(ctx, path, version)
}

func (b *statsBackend) Read(
	ctx context.Context, path string,
) (backend.ReadReply, error) {
	atomic.AddInt32(&b.objReads, 1)
	return b.inner.Read(ctx, path)
}

func (b *statsBackend) GetMetadata(
	ctx context.Context, path string,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.metaReads, 1)
	return b.inner.GetMetadata(ctx, path)
}

func (b *statsBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.metaWrites, 1)
	return b.inner.SetTagsIf(ctx, path, expected, t)
}

func (b *statsBackend) Write(
	ctx context.Context, path string, value []byte, t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWrites, 1)
	return b.inner.Write(ctx, path, value, t)
}

func (b *statsBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWrites, 1)
	return b.inner.WriteIf(ctx, path, value, expected, t)
}

func (b *statsBackend) WriteIfNotExists(
	ctx context.Context, path string, value []byte, t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWrites, 1)
	return b.inner.WriteIfNotExists(ctx, path, value, t)
}

func (b *statsBackend) Delete(ctx context.Context, path string) error {
	atomic.AddInt32(&b.objWrites, 1)
	return b.inner.Delete(ctx, path)
}

func (b *statsBackend) DeleteIf(
	ctx context.Context, path string, expected backend.Version,
) error {
	atomic.AddInt32(&b.objWrites, 1)
	return b.inner.DeleteIf(ctx, path, expected)
}

func (b *statsBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	atomic.AddInt32(&b.objLists, 1)
	return b.inner.List(ctx, dirPath)
}

// Ensure backendStatser implements Backend.
var _ backend.Backend = (*statsBackend)(nil)
