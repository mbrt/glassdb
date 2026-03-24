package testkit

import (
	"context"
	"sync"

	"github.com/sourcegraph/conc"

	"github.com/mbrt/glassdb/backend"
)

// NewStallBackend wraps a backend so that writes can be paused, stalled, and released.
func NewStallBackend(inner backend.Backend) *StallBackend {
	return &StallBackend{
		Backend: inner,
		relCh:   make(chan struct{}),
	}
}

// StallBackend is a backend where writes can be paused and resumed.
//
// When a write is stalled, it returns immediately with an error. It will complete
// asynchronously only when released.
type StallBackend struct {
	backend.Backend
	stall bool
	relCh chan struct{}
	m     sync.Mutex
	wg    conc.WaitGroup
}

// WaitForStalled blocks until all stalled write operations have been queued.
func (b *StallBackend) WaitForStalled() {
	b.wg.Wait()
}

// StallWrites causes all subsequent write operations to be held until released.
func (b *StallBackend) StallWrites() {
	b.m.Lock()
	b.stall = true
	b.relCh = make(chan struct{})
	b.m.Unlock()
}

// StopStalling allows new write operations to proceed normally without stalling.
func (b *StallBackend) StopStalling() {
	b.m.Lock()
	b.stall = false
	b.m.Unlock()
}

// ReleaseStalled allows all previously stalled write operations to complete.
func (b *StallBackend) ReleaseStalled() {
	b.m.Lock()
	ch := b.relCh
	b.m.Unlock()
	close(ch)
}

func (b *StallBackend) stallOp() (release <-chan struct{}, stalled bool) {
	b.m.Lock()
	if !b.stall {
		b.m.Unlock()
		return nil, false
	}
	ch := b.relCh
	b.m.Unlock()
	return ch, true
}

// SetTagsIf sets tags on a path conditionally, stalling if stalling is enabled.
func (b *StallBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.SetTagsIf(ctx, path, expected, t)
	}
	b.wg.Go(func() {
		<-rel
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
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.Write(ctx, path, value, t)
	}
	b.wg.Go(func() {
		<-rel
		_, _ = b.Backend.Write(ctx, path, value, t)
	})
	return backend.Metadata{}, context.Canceled
}

// WriteIf writes a value conditionally, stalling if stalling is enabled.
func (b *StallBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.WriteIf(ctx, path, value, expected, t)
	}
	b.wg.Go(func() {
		<-rel
		_, _ = b.Backend.WriteIf(ctx, path, value, expected, t)
	})
	return backend.Metadata{}, context.Canceled
}

// WriteIfNotExists writes a value only if the path does not exist, stalling if stalling is enabled.
func (b *StallBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.WriteIfNotExists(ctx, path, value, t)
	}
	b.wg.Go(func() {
		<-rel
		_, _ = b.Backend.WriteIfNotExists(ctx, path, value, t)
	})
	return backend.Metadata{}, context.Canceled
}

// Delete removes an object at the given path, stalling if stalling is enabled.
func (b *StallBackend) Delete(ctx context.Context, path string) error {
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.Delete(ctx, path)
	}
	b.wg.Go(func() {
		<-rel
		_ = b.Backend.Delete(ctx, path)
	})
	return context.Canceled
}

// DeleteIf removes an object conditionally, stalling if stalling is enabled.
func (b *StallBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	rel, stalled := b.stallOp()
	if !stalled {
		return b.Backend.DeleteIf(ctx, path, expected)
	}
	b.wg.Go(func() {
		<-rel
		_ = b.Backend.DeleteIf(ctx, path, expected)
	})
	return context.Canceled
}
