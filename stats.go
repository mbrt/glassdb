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
	TxN       int           // total number of completed transactions.
	TxTime    time.Duration // total time spent within transactions.
	TxReads   int           // total number of reads.
	TxWrites  int           // total number of writes.
	TxRetries int           // total number of retried transactions.

	// Backend statistics.
	MetaReadN  int // number of read metadata.
	MetaWriteN int // number of write metadata.
	ObjReadN   int // number of read objects.
	ObjWriteN  int // number of written objects.
	ObjListN   int // number of list calls.
}

// Sub calculates and returns the difference between two sets of transaction
// stats. This is useful when obtaining stats at two different points and time
// and you need the performance counters that occurred within that time span.
func (s Stats) Sub(other Stats) Stats {
	return Stats{
		TxN:       s.TxN - other.TxN,
		TxTime:    s.TxTime - other.TxTime,
		TxReads:   s.TxReads - other.TxReads,
		TxWrites:  s.TxWrites - other.TxWrites,
		TxRetries: s.TxRetries - other.TxRetries,

		MetaReadN:  s.MetaReadN - other.MetaReadN,
		MetaWriteN: s.MetaWriteN - other.MetaWriteN,
		ObjReadN:   s.ObjReadN - other.ObjReadN,
		ObjWriteN:  s.ObjWriteN - other.ObjWriteN,
		ObjListN:   s.ObjListN - other.ObjListN,
	}
}

func (s *Stats) add(other *Stats) {
	s.TxN += other.TxN
	s.TxTime += other.TxTime
	s.TxReads += other.TxReads
	s.TxWrites += other.TxWrites
	s.TxRetries += other.TxRetries

	s.MetaReadN += other.MetaReadN
	s.MetaWriteN += other.MetaWriteN
	s.ObjReadN += other.ObjReadN
	s.ObjWriteN += other.ObjWriteN
	s.ObjListN += other.ObjListN
}

type statsBackend struct {
	inner backend.Backend

	metaReadN  int32
	metaWriteN int32
	objReadN   int32
	objWriteN  int32
	objListN   int32
}

// StatsAndReset returns backend stats and resets them.
func (b *statsBackend) StatsAndReset() Stats {
	return Stats{
		MetaReadN:  int(atomic.SwapInt32(&b.metaReadN, 0)),
		MetaWriteN: int(atomic.SwapInt32(&b.metaWriteN, 0)),
		ObjReadN:   int(atomic.SwapInt32(&b.objReadN, 0)),
		ObjWriteN:  int(atomic.SwapInt32(&b.objWriteN, 0)),
		ObjListN:   int(atomic.SwapInt32(&b.objListN, 0)),
	}
}

func (b *statsBackend) ReadIfModified(
	ctx context.Context, path string, version int64,
) (backend.ReadReply, error) {
	atomic.AddInt32(&b.objReadN, 1)
	return b.inner.ReadIfModified(ctx, path, version)
}

func (b *statsBackend) Read(
	ctx context.Context, path string,
) (backend.ReadReply, error) {
	atomic.AddInt32(&b.objReadN, 1)
	return b.inner.Read(ctx, path)
}

func (b *statsBackend) GetMetadata(
	ctx context.Context, path string,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.metaReadN, 1)
	return b.inner.GetMetadata(ctx, path)
}

func (b *statsBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.metaWriteN, 1)
	return b.inner.SetTagsIf(ctx, path, expected, t)
}

func (b *statsBackend) Write(
	ctx context.Context, path string, value []byte, t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWriteN, 1)
	return b.inner.Write(ctx, path, value, t)
}

func (b *statsBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWriteN, 1)
	return b.inner.WriteIf(ctx, path, value, expected, t)
}

func (b *statsBackend) WriteIfNotExists(
	ctx context.Context, path string, value []byte, t backend.Tags,
) (backend.Metadata, error) {
	atomic.AddInt32(&b.objWriteN, 1)
	return b.inner.WriteIfNotExists(ctx, path, value, t)
}

func (b *statsBackend) Delete(ctx context.Context, path string) error {
	atomic.AddInt32(&b.objWriteN, 1)
	return b.inner.Delete(ctx, path)
}

func (b *statsBackend) DeleteIf(
	ctx context.Context, path string, expected backend.Version,
) error {
	atomic.AddInt32(&b.objWriteN, 1)
	return b.inner.DeleteIf(ctx, path, expected)
}

func (b *statsBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	iter, err := b.inner.List(ctx, dirPath)
	if err != nil {
		return iter, err
	}
	return statsListIter{
		inner:   iter,
		counter: &b.objListN,
	}, nil
}

type statsListIter struct {
	inner   backend.ListIter
	counter *int32
}

func (l statsListIter) Next() (path string, ok bool) {
	atomic.AddInt32(l.counter, 1)
	return l.inner.Next()
}

func (l statsListIter) Err() error {
	return l.inner.Err()
}

// Ensure backendStatser implements Backend.
var (
	_ backend.Backend  = (*statsBackend)(nil)
	_ backend.ListIter = (*statsListIter)(nil)
)
