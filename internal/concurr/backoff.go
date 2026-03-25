package concurr

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// TODO: Make these configurable. Compute them based on backend latency.
const (
	initialInterval = 200 * time.Millisecond
	maxInterval     = 5 * time.Second
)

// Permanent wraps an error to signal that retries should stop immediately.
func Permanent(err error) error {
	return backoff.Permanent(err)
}

// IsPermanent reports whether the error was marked as permanent.
func IsPermanent(err error) bool {
	var perr *backoff.PermanentError
	return errors.As(err, &perr)
}

// Retrier provides configurable exponential backoff retry logic.
type Retrier struct {
	backoff.BackOff
}

// RetryOptions creates a Retrier with the given initial and maximum backoff intervals.
func RetryOptions(initial, maxInterval time.Duration) Retrier {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initial
	b.MaxInterval = maxInterval
	b.MaxElapsedTime = 0
	return Retrier{b}
}

// Retry calls fn repeatedly with exponential backoff until it succeeds or ctx is cancelled.
func (r Retrier) Retry(ctx context.Context, fn func() error) error {
	return backoff.Retry(fn, backoff.WithContext(r.BackOff, ctx))
}

// RetryWithBackoff retries f with default exponential backoff settings until it succeeds or ctx is cancelled.
func RetryWithBackoff(ctx context.Context, f func() error) error {
	r := RetryOptions(initialInterval, maxInterval)
	return backoff.Retry(f, backoff.WithContext(r.BackOff, ctx))
}
