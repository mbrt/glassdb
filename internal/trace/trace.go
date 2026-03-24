//go:build tracing

package trace

import (
	"context"
	"runtime/trace"
)

func NewTask(ctx context.Context, n string) (context.Context, *trace.Task) {
	return trace.NewTask(ctx, n)
}

func WithRegion(ctx context.Context, n string, fn func()) {
	trace.WithRegion(ctx, n, fn)
}

func StartRegion(ctx context.Context, n string) *trace.Region {
	return trace.StartRegion(ctx, n)
}
