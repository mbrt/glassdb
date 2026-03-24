//go:build !tracing

package trace

import "context"

// NewTask returns a no-op tracing task when the tracing build tag is disabled.
func NewTask(ctx context.Context, _ string) (context.Context, Task) {
	return ctx, Task{}
}

// Task is a no-op implementation of a tracing task.
type Task struct{}

// End is a no-op that satisfies the tracing task interface.
func (Task) End() {}

// WithRegion calls fn directly without tracing when the tracing build tag is disabled.
func WithRegion(_ context.Context, _ string, fn func()) {
	fn()
}

// StartRegion returns a no-op Region when the tracing build tag is disabled.
func StartRegion(context.Context, string) Region {
	return Region{}
}

// Region is a no-op implementation of a tracing region.
type Region struct{}

// End is a no-op that satisfies the tracing region interface.
func (Region) End() {}
