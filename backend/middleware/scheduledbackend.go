package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/mbrt/glassdb/backend"
)

// NewScheduler creates a Scheduler that consumes bytes from data to
// produce deterministic delays. Each call to Wait consumes one byte and
// sleeps for byte_value * tick. When data is exhausted, Wait returns
// immediately (zero delay).
func NewScheduler(data []byte, tick time.Duration) *Scheduler {
	return &Scheduler{data: data, tick: tick}
}

// Scheduler produces deterministic delays from a byte sequence.
// It is safe for concurrent use.
type Scheduler struct {
	mu   sync.Mutex
	data []byte
	pos  int
	tick time.Duration
}

func (s *Scheduler) next() byte {
	s.mu.Lock()
	var d byte
	if s.pos < len(s.data) {
		d = s.data[s.pos]
		s.pos++
	}
	s.mu.Unlock()
	return d
}

// NewScheduledBackend wraps inner so that every operation is preceded by
// a scheduler-determined delay. Use inside a synctest bubble for
// deterministic, instant-in-wall-clock operation ordering.
func NewScheduledBackend(inner backend.Backend, sched *Scheduler) *ScheduledBackend {
	return &ScheduledBackend{inner: inner, sched: sched}
}

// ScheduledBackend is a backend.Backend decorator that injects
// deterministic delays before every operation.
type ScheduledBackend struct {
	inner backend.Backend
	sched *Scheduler
}

func (b *ScheduledBackend) wait(ctx context.Context) {
	d := b.sched.next()
	if d == 0 {
		return
	}
	t := time.NewTimer(time.Duration(d) * b.sched.tick)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// ReadIfModified implements backend.Backend.
func (b *ScheduledBackend) ReadIfModified(
	ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	b.wait(ctx)
	return b.inner.ReadIfModified(ctx, path, version)
}

// Read implements backend.Backend.
func (b *ScheduledBackend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	b.wait(ctx)
	return b.inner.Read(ctx, path)
}

// GetMetadata implements backend.Backend.
func (b *ScheduledBackend) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	b.wait(ctx)
	return b.inner.GetMetadata(ctx, path)
}

// SetTagsIf implements backend.Backend.
func (b *ScheduledBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.wait(ctx)
	return b.inner.SetTagsIf(ctx, path, expected, t)
}

// Write implements backend.Backend.
func (b *ScheduledBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.wait(ctx)
	return b.inner.Write(ctx, path, value, t)
}

// WriteIf implements backend.Backend.
func (b *ScheduledBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	b.wait(ctx)
	return b.inner.WriteIf(ctx, path, value, expected, t)
}

// WriteIfNotExists implements backend.Backend.
func (b *ScheduledBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	b.wait(ctx)
	return b.inner.WriteIfNotExists(ctx, path, value, t)
}

// Delete implements backend.Backend.
func (b *ScheduledBackend) Delete(ctx context.Context, path string) error {
	b.wait(ctx)
	return b.inner.Delete(ctx, path)
}

// DeleteIf implements backend.Backend.
func (b *ScheduledBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	b.wait(ctx)
	return b.inner.DeleteIf(ctx, path, expected)
}

// List implements backend.Backend.
func (b *ScheduledBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	b.wait(ctx)
	return b.inner.List(ctx, dirPath)
}

var _ backend.Backend = (*ScheduledBackend)(nil)
