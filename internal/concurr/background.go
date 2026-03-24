// Package concurr provides concurrency utilities including goroutine
// management, fan-out execution, and retry with backoff.
package concurr

import (
	"context"
	"sync"

	"github.com/sourcegraph/conc"
)

// NewBackground creates a new Background goroutine manager.
func NewBackground() *Background {
	return &Background{
		done: make(chan struct{}),
	}
}

// Background manages a set of background goroutines that are cancelled together on Close.
type Background struct {
	wg     conc.WaitGroup
	done   chan struct{}
	closed bool
	m      sync.Mutex
}

// Close signals all background goroutines to stop and waits for them to finish.
func (b *Background) Close() {
	b.m.Lock()
	if b.closed {
		b.m.Unlock()
		return
	}

	close(b.done)
	b.closed = true
	b.m.Unlock()
	b.wg.Wait()
}

// Go spawns fn as a background goroutine and returns true, or returns false if already closed.
func (b *Background) Go(ctx context.Context, fn func(context.Context)) bool {
	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return false
	}
	b.wg.Go(func() {
		fn(ContextWithNewCancel(ctx, b.done))
	})
	return true
}
