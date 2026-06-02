package concurr

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// Default retry timing, used when a caller does not tune it explicitly.
const (
	defaultInitialInterval = 200 * time.Millisecond
	defaultMaxInterval     = 5 * time.Second
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

// RetryConfig configures the exponential backoff used by a Retrier.
type RetryConfig struct {
	// InitialInterval is the delay before the first retry; it grows
	// exponentially up to MaxInterval.
	InitialInterval time.Duration
	// MaxInterval caps the per-retry delay.
	MaxInterval time.Duration
	// Jitter randomizes intervals to spread retries and avoid thundering-herd
	// contention. Keep it enabled in production; disable it only where
	// deterministic retry timing is required (e.g. tests), since it draws from
	// the process-global math/rand that testing/synctest does not virtualize.
	Jitter bool
}

// DefaultRetryConfig returns the production retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialInterval: defaultInitialInterval,
		MaxInterval:     defaultMaxInterval,
		Jitter:          true,
	}
}

// Retrier retries an operation with exponential backoff.
type Retrier struct {
	cfg RetryConfig
}

// NewRetrier creates a Retrier from the given configuration.
func NewRetrier(cfg RetryConfig) Retrier {
	return Retrier{cfg: cfg}
}

// DefaultRetrier returns a Retrier using DefaultRetryConfig.
func DefaultRetrier() Retrier {
	return NewRetrier(DefaultRetryConfig())
}

// Retry calls fn repeatedly with exponential backoff until it succeeds, returns
// a permanent error, or ctx is cancelled.
func (r Retrier) Retry(ctx context.Context, fn func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = r.cfg.InitialInterval
	b.MaxInterval = r.cfg.MaxInterval
	b.MaxElapsedTime = 0
	if !r.cfg.Jitter {
		b.RandomizationFactor = 0
	}
	return backoff.Retry(fn, backoff.WithContext(b, ctx))
}
