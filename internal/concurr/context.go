package concurr

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

// ContextWithTimeout returns a context that is cancelled after the given timeout using the provided clock.
func ContextWithTimeout(
	parent context.Context,
	clock clockwork.Clock,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	clock.AfterFunc(timeout, func() {
		cancel()
	})
	return ctx, cancel
}

// ContextWithNewCancel replaces the parent cancellation (if any) with the
// given one, while preserving the embedded values.
func ContextWithNewCancel(parent context.Context, done <-chan struct{}) context.Context {
	return detachedCtx{
		parent: parent,
		done:   done,
	}
}

type detachedCtx struct {
	parent context.Context
	done   <-chan struct{}
}

func (v detachedCtx) Deadline() (time.Time, bool)       { return time.Time{}, false }
func (v detachedCtx) Done() <-chan struct{}             { return v.done }
func (v detachedCtx) Value(key interface{}) interface{} { return v.parent.Value(key) }

func (v detachedCtx) Err() error {
	select {
	case <-v.done:
		return context.Canceled
	default:
		return nil
	}
}
