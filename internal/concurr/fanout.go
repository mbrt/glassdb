package concurr

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// NewFanout creates a Fanout with the given maximum concurrency limit.
func NewFanout(maxConcurrent int) Fanout {
	return Fanout{
		limit: maxConcurrent,
	}
}

// Fanout executes functions concurrently up to a configured concurrency limit.
type Fanout struct {
	limit int
}

// Spawn runs f concurrently for each index in [0, num), returning a Result to wait on.
func (o Fanout) Spawn(ctx context.Context, num int, f func(context.Context, int) error) Result {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(o.limit)

	for i := 0; i < num; i++ {
		i := i // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return f(ctx, i)
		})
	}

	return g
}

// Result represents the outcome of a fan-out operation that can be waited on.
type Result interface {
	Wait() error
}
