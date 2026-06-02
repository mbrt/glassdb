package concurr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// Default retry timing, used when a caller does not tune it explicitly.
const (
	defaultInitialInterval = 200 * time.Millisecond
	defaultMaxInterval     = 5 * time.Second
)

// Exponential growth and jitter parameters. These mirror the defaults of the
// previously-vendored cenkalti/backoff library.
const (
	backoffMultiplier    = 1.5
	backoffRandomization = 0.5
)

// Permanent wraps an error to signal that retries should stop immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether the error was marked as permanent.
func IsPermanent(err error) bool {
	var perr *permanentError
	return errors.As(err, &perr)
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// RetryConfig configures the exponential backoff used by a Retrier.
type RetryConfig struct {
	// InitialInterval is the delay before the first retry; it grows
	// exponentially up to MaxInterval.
	InitialInterval time.Duration
	// MaxInterval caps the per-retry delay.
	MaxInterval time.Duration
	// Rand is the source of jitter randomness. Jitter randomizes each interval
	// to spread retries and avoid thundering-herd contention. When Rand is nil
	// no jitter is applied and intervals are purely exponential, which keeps
	// retry timing deterministic. Rand must be safe for concurrent use.
	Rand io.Reader
}

// DefaultRetryConfig returns the default retry timing. Jitter is disabled
// (Rand is nil); callers that want jitter must supply a randomness source.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialInterval: defaultInitialInterval,
		MaxInterval:     defaultMaxInterval,
	}
}

// Retrier retries an operation with exponential backoff.
type Retrier struct {
	cfg RetryConfig
}

// NewRetrier creates a Retrier from the given configuration, filling unset
// (non-positive) intervals with defaults.
func NewRetrier(cfg RetryConfig) Retrier {
	if cfg.InitialInterval <= 0 {
		cfg.InitialInterval = defaultInitialInterval
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = defaultMaxInterval
	}
	return Retrier{cfg: cfg}
}

// DefaultRetrier returns a Retrier using DefaultRetryConfig.
func DefaultRetrier() Retrier {
	return NewRetrier(DefaultRetryConfig())
}

// Retry calls fn repeatedly with exponential backoff until it succeeds, returns
// a permanent error (see Permanent), or ctx is cancelled. It does not give up
// on its own: it retries indefinitely until one of those conditions is met.
func (r Retrier) Retry(ctx context.Context, fn func() error) error {
	interval := r.cfg.InitialInterval
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if perr, ok := errors.AsType[*permanentError](err); ok {
			return perr.err
		}

		wait := r.jittered(interval)
		interval = nextInterval(interval, r.cfg.MaxInterval)

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// jittered randomizes interval within [interval*(1-f), interval*(1+f)] when a
// jitter source is configured, or returns it unchanged otherwise.
func (r Retrier) jittered(interval time.Duration) time.Duration {
	if r.cfg.Rand == nil {
		return interval
	}
	delta := backoffRandomization * float64(interval)
	lo := float64(interval) - delta
	hi := float64(interval) + delta
	return time.Duration(lo + readFloat(r.cfg.Rand)*(hi-lo+1))
}

// nextInterval grows current by the multiplier, capped at maxInterval.
func nextInterval(current, maxInterval time.Duration) time.Duration {
	if float64(current) >= float64(maxInterval)/backoffMultiplier {
		return maxInterval
	}
	return time.Duration(float64(current) * backoffMultiplier)
}

// readFloat draws a float64 in [0,1) from r, mirroring math/rand's Float64.
func readFloat(r io.Reader) float64 {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		panic(fmt.Sprintf("cannot read from jitter random source: %v", err))
	}
	return float64(binary.BigEndian.Uint64(b[:])>>11) / (1 << 53)
}
