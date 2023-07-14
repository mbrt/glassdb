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
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
)

var errBackoff = errors.New("rate limited")

type DelayOptions struct {
	MetaRead  time.Duration
	MetaWrite time.Duration
	ObjRead   time.Duration
	ObjWrite  time.Duration
	List      time.Duration
	// How many writes per second to the same object before being
	// rate limited?
	SameObjWritePs int
	// TODO: Use actual std deviation of type duration for each op.
	// StdDevPerc is the ratio between standard deviation and mean
	// for each measure: stddev(d) / mean(d).
	StdDevPerc float64
}

func NewDelayBackend(
	inner backend.Backend,
	clock clockwork.Clock,
	opts DelayOptions,
) *DelayBackend {
	return &DelayBackend{
		inner:     inner,
		clock:     clock,
		metaRead:  lognormalDelay(opts.MetaRead, opts.StdDevPerc),
		metaWrite: lognormalDelay(opts.MetaWrite, opts.StdDevPerc),
		objRead:   lognormalDelay(opts.ObjRead, opts.StdDevPerc),
		objWrite:  lognormalDelay(opts.ObjWrite, opts.StdDevPerc),
		list:      lognormalDelay(opts.List, opts.StdDevPerc),
		rlimit: rateLimiter{
			tokensPerSec: int(opts.SameObjWritePs),
			clock:        clock,
			buckets:      map[string]bucketState{},
		},
		retryDelay: opts.ObjWrite * 2,
	}
}

type DelayBackend struct {
	inner      backend.Backend
	clock      clockwork.Clock
	metaRead   lognormal
	metaWrite  lognormal
	objRead    lognormal
	objWrite   lognormal
	list       lognormal
	rlimit     rateLimiter
	retryDelay time.Duration
}

func (b *DelayBackend) ReadIfModified(
	ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	r, err := b.inner.ReadIfModified(ctx, path, version)
	if errors.Is(err, backend.ErrPrecondition) {
		b.delay(b.metaRead)
		return r, err
	}
	b.delay(b.objRead)
	return r, err
}

func (b *DelayBackend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	r, err := b.inner.Read(ctx, path)
	b.delay(b.objRead)
	return r, err
}

func (b *DelayBackend) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	r, err := b.inner.GetMetadata(ctx, path)
	b.delay(b.metaRead)
	return r, err
}

func (b *DelayBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.backoff(ctx, path)
	r, err := b.inner.SetTagsIf(ctx, path, expected, t)
	b.delay(b.metaWrite)
	return r, err
}

func (b *DelayBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.backoff(ctx, path)
	r, err := b.inner.Write(ctx, path, value, t)
	b.delay(b.objWrite)
	return r, err
}

func (b *DelayBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.backoff(ctx, path)
	r, err := b.inner.WriteIf(ctx, path, value, expected, t)
	if errors.Is(err, backend.ErrPrecondition) {
		b.delay(b.metaRead)
		return r, err
	}
	b.delay(b.objWrite)
	return r, err
}

func (b *DelayBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.backoff(ctx, path)
	r, err := b.inner.WriteIfNotExists(ctx, path, value, t)
	if errors.Is(err, backend.ErrPrecondition) {
		b.delay(b.metaRead)
		return r, err
	}
	b.delay(b.objWrite)
	return r, err
}

func (b *DelayBackend) Delete(ctx context.Context, path string) error {
	b.backoff(ctx, path)
	err := b.inner.Delete(ctx, path)
	b.delay(b.objWrite)
	return err
}

func (b *DelayBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	b.backoff(ctx, path)
	err := b.inner.DeleteIf(ctx, path, expected)
	if errors.Is(err, backend.ErrPrecondition) {
		b.delay(b.metaRead)
		return err
	}
	b.delay(b.objWrite)
	return err
}

func (b *DelayBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	r, err := b.inner.List(ctx, dirPath)
	b.delay(b.list)
	return r, err
}

func (b *DelayBackend) backoff(ctx context.Context, path string) error {
	r := concurr.RetryOptions(b.retryDelay, b.retryDelay*10, b.clock)
	return r.Retry(ctx, func() error {
		if !b.rlimit.TryAcquireToken(path) {
			return errBackoff
		}
		return nil
	})
}

func (b *DelayBackend) delay(ln lognormal) {
	ms := ln.Rand()
	d := time.Duration(ms * float64(time.Millisecond))
	b.clock.Sleep(d)
}

func lognormalDelay(mean time.Duration, stdPerc float64) lognormal {
	m64 := float64(mean) / float64(time.Millisecond)
	s64 := m64 * stdPerc
	return newLognormalWith(m64, s64)
}

func newLognormalWith(mean, stddev float64) lognormal {
	// https://stats.stackexchange.com/a/95506
	sByM := stddev / mean
	v := math.Log(sByM*sByM + 1)
	return lognormal{
		Mu:    math.Log(mean) - 0.5*v,
		Sigma: math.Sqrt(v),
	}
}

type lognormal struct {
	Mu    float64
	Sigma float64
}

func (l lognormal) Rand() float64 {
	return math.Exp(rand.NormFloat64()*l.Sigma + l.Mu)
}

type rateLimiter struct {
	tokensPerSec int
	clock        clockwork.Clock
	buckets      map[string]bucketState
	m            sync.Mutex
}

func (r *rateLimiter) TryAcquireToken(key string) bool {
	if r.tokensPerSec == 0 {
		return false
	}

	r.m.Lock()
	defer r.m.Unlock()

	now := r.clock.Now()
	entry, ok := r.buckets[key]
	if !ok {
		r.buckets[key] = bucketState{
			LastCheck: now,
			Tokens:    r.tokensPerSec - 1,
		}
		return true
	}

	elapsed := now.Sub(entry.LastCheck)
	if elapsed >= time.Second {
		newTokens := entry.Tokens + int(elapsed.Seconds()*float64(r.tokensPerSec))
		if newTokens > r.tokensPerSec {
			newTokens = r.tokensPerSec
		}
		if newTokens <= 0 {
			return false
		}
		r.buckets[key] = bucketState{
			LastCheck: now,
			Tokens:    newTokens - 1,
		}
		return true
	}

	entry.Tokens--
	r.buckets[key] = entry
	return true
}

type bucketState struct {
	LastCheck time.Time
	Tokens    int
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*DelayBackend)(nil)
