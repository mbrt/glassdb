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

var GCSDelays = DelayOptions{
	MetaRead:       Latency{22 * time.Millisecond, 7 * time.Millisecond},
	MetaWrite:      Latency{31 * time.Millisecond, 8 * time.Millisecond},
	ObjRead:        Latency{57 * time.Millisecond, 7 * time.Millisecond},
	ObjWrite:       Latency{70 * time.Millisecond, 15 * time.Millisecond},
	List:           Latency{10 * time.Millisecond, 3 * time.Millisecond},
	SameObjWritePs: 1,
}

type DelayOptions struct {
	MetaRead  Latency
	MetaWrite Latency
	ObjRead   Latency
	ObjWrite  Latency
	List      Latency
	// How many writes per second to the same object before being
	// rate limited?
	SameObjWritePs int
}

type Latency struct {
	Mean   time.Duration
	StdDev time.Duration
}

func NewDelayBackend(
	inner backend.Backend,
	clock clockwork.Clock,
	opts DelayOptions,
) *DelayBackend {
	return &DelayBackend{
		inner:     inner,
		clock:     clock,
		metaRead:  lognormalDelay(opts.MetaRead),
		metaWrite: lognormalDelay(opts.MetaWrite),
		objRead:   lognormalDelay(opts.ObjRead),
		objWrite:  lognormalDelay(opts.ObjWrite),
		list:      lognormalDelay(opts.List),
		rlimit: rateLimiter{
			tokensPerSec: opts.SameObjWritePs,
			clock:        clock,
			buckets:      map[string]bucketState{},
		},
		retryDelay: opts.ObjWrite.Mean * 2,
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
	b.delay(b.objRead)
	return b.inner.ReadIfModified(ctx, path, version)
}

func (b *DelayBackend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	b.delay(b.objRead)
	r, err := b.inner.Read(ctx, path)
	return r, err
}

func (b *DelayBackend) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	b.delay(b.metaRead)
	r, err := b.inner.GetMetadata(ctx, path)
	return r, err
}

func (b *DelayBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.metaWrite)
	return b.inner.SetTagsIf(ctx, path, expected, t)
}

func (b *DelayBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	r, err := b.inner.Write(ctx, path, value, t)
	return r, err
}

func (b *DelayBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	return b.inner.WriteIf(ctx, path, value, expected, t)
}

func (b *DelayBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	return b.inner.WriteIfNotExists(ctx, path, value, t)
}

func (b *DelayBackend) Delete(ctx context.Context, path string) error {
	if err := b.backoff(ctx, path); err != nil {
		return err
	}
	b.delay(b.objWrite)
	err := b.inner.Delete(ctx, path)
	return err
}

func (b *DelayBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	if err := b.backoff(ctx, path); err != nil {
		return err
	}
	b.delay(b.objWrite)
	return b.inner.DeleteIf(ctx, path, expected)
}

func (b *DelayBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	b.delay(b.list)
	r, err := b.inner.List(ctx, dirPath)
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

func lognormalDelay(l Latency) lognormal {
	m64 := float64(l.Mean) / float64(time.Millisecond)
	s64 := float64(l.StdDev) / float64(time.Millisecond)
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
