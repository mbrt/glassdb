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

package testkit

import (
	"context"
	"sync"

	"github.com/sourcegraph/conc"

	"github.com/mbrt/glassdb/backend"
)

func NewStallBackend(inner backend.Backend) *StallBackend {
	res := &StallBackend{
		Backend: inner,
	}
	res.cond = sync.NewCond(&res.m)
	return res
}

// StallBackend is a backend where writes can be paused and resumed.
//
// When a write is stalled, it returns immediately with an error. It will complete
// asynchronously only when released.
type StallBackend struct {
	backend.Backend
	stall   bool
	release bool
	m       sync.Mutex
	cond    *sync.Cond
	wg      conc.WaitGroup
}

func (b *StallBackend) WaitForStalled() {
	b.wg.Wait()
}

func (b *StallBackend) StallWrites() {
	b.m.Lock()
	b.stall = true
	b.release = false
	b.m.Unlock()
	b.cond.Broadcast()
}

func (b *StallBackend) StopStalling() {
	b.m.Lock()
	b.stall = false
	b.m.Unlock()
	b.cond.Broadcast()
}

func (b *StallBackend) ReleaseStalled() {
	b.m.Lock()
	b.release = true
	b.m.Unlock()
	b.cond.Broadcast()
}

func (b *StallBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.SetTagsIf(ctx, path, expected, t)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_, _ = b.Backend.SetTagsIf(ctx, path, expected, t)
	})
	return backend.Metadata{}, context.Canceled
}

func (b *StallBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.Write(ctx, path, value, t)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_, _ = b.Backend.Write(ctx, path, value, t)
	})
	return backend.Metadata{}, context.Canceled
}

func (b *StallBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.WriteIf(ctx, path, value, expected, t)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_, _ = b.Backend.WriteIf(ctx, path, value, expected, t)
	})
	return backend.Metadata{}, context.Canceled
}

func (b *StallBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.WriteIfNotExists(ctx, path, value, t)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_, _ = b.Backend.WriteIfNotExists(ctx, path, value, t)
	})
	return backend.Metadata{}, context.Canceled
}

func (b *StallBackend) Delete(ctx context.Context, path string) error {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.Delete(ctx, path)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_ = b.Backend.Delete(ctx, path)
	})
	return context.Canceled
}

func (b *StallBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return b.Backend.DeleteIf(ctx, path, expected)
	}

	b.wg.Go(func() {
		for !b.release {
			b.cond.Wait()
		}
		b.m.Unlock()
		_ = b.Backend.DeleteIf(ctx, path, expected)
	})
	return context.Canceled
}
