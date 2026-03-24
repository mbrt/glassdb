package concurr

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jonboulle/clockwork"
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
	clock clockwork.Clock
}

// RetryOptions creates a Retrier with the given initial and maximum backoff intervals.
func RetryOptions(initial, maxInterval time.Duration, c clockwork.Clock) Retrier {
	b := backoff.NewExponentialBackOff()
	b.Clock = c
	b.InitialInterval = initial
	b.MaxInterval = maxInterval
	b.MaxElapsedTime = 0
	return Retrier{b, c}
}

// Retry calls fn repeatedly with exponential backoff until it succeeds or ctx is cancelled.
func (r Retrier) Retry(ctx context.Context, fn func() error) error {
	return backoff.RetryNotifyWithTimer(fn,
		backoff.WithContext(r.BackOff, ctx), nil, &timer{r.clock, nil})
}

// RetryWithBackoff retries f with default exponential backoff settings until it succeeds or ctx is cancelled.
func RetryWithBackoff(ctx context.Context, c clockwork.Clock, f func() error) error {
	r := RetryOptions(initialInterval, maxInterval, c)
	return backoff.RetryNotifyWithTimer(f,
		backoff.WithContext(r.BackOff, ctx), nil, &timer{c, nil})
}

type timer struct {
	clockwork.Clock
	timer clockwork.Timer
}

func (t *timer) C() <-chan time.Time {
	return t.timer.Chan()
}

// Start starts the timer to fire after the given duration
func (t *timer) Start(duration time.Duration) {
	if t.timer == nil {
		t.timer = t.Clock.NewTimer(duration)
	} else {
		t.timer.Reset(duration)
	}
}

// Stop is called when the timer is not used anymore and resources may be freed.
func (t timer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}
