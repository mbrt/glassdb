package middleware

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
)

var errBackoff = errors.New("rate limited")

// GCSDelays contains typical latency values observed with Google Cloud Storage.
var GCSDelays = DelayOptions{
	MetaRead:       Latency{22 * time.Millisecond, 7 * time.Millisecond},
	MetaWrite:      Latency{31 * time.Millisecond, 8 * time.Millisecond},
	ObjRead:        Latency{57 * time.Millisecond, 7 * time.Millisecond},
	ObjWrite:       Latency{70 * time.Millisecond, 15 * time.Millisecond},
	List:           Latency{10 * time.Millisecond, 3 * time.Millisecond},
	SameObjWritePs: 1,
}

// S3Delays contains typical latency values for Amazon S3 Standard accessed
// in-region, derived from AWS guidance and public benchmarks (p50 GET ~30 ms,
// p50 PUT ~70 ms, with a long right tail captured by the lognormal model).
//
// MetaWrite is higher than the other backends because S3 has no metadata-only
// update: SetTagsIf re-uploads the object (a GET followed by a PUT), so its
// latency is roughly the sum of the two. See docs/s3.md.
//
// Unlike GCS, S3 has no per-object write limit; throughput scales per prefix.
// SameObjWritePs is therefore set high so the per-object limiter never binds,
// and the per-prefix request-rate limit is modeled separately (see below).
//
// PrefixReadPs/PrefixWritePs/PrefixDepth model S3's documented per-prefix
// request-rate limit: a single partitioned prefix sustains at least 5,500
// GET/HEAD and 3,500 PUT/COPY/POST/DELETE requests per second, returning
// 503 SlowDown above that. We use those documented numbers directly rather
// than a value fitted to one benchmark run, so the limit is explainable and so
// it responds correctly to algorithm changes: issuing fewer requests per
// transaction, or spreading load across more prefixes, raises throughput just
// as it would on real S3. The purpose of this backend is to design and test
// such changes, so faithful *relative* behavior matters more than reproducing
// any single run's absolute numbers.
//
// PrefixDepth selects the partition granularity. GlassDB lays objects out as
// "<db>/_t/<txid>" (transaction log) and "<db>/_c/<coll>/_k/<key>" (data), so
// depth 2 throttles the transaction-log subtree ("<db>/_t") and the data
// subtree ("<db>/_c") as two independent prefixes — the granularity at which a
// commit-path change that shifts work between them becomes visible. S3
// auto-partitions hot prefixes, so real throughput can exceed a single
// prefix's limit; raise PrefixDepth to model more partitions. See docs/s3.md
// and docs/adr/004-fake-s3-backend-fidelity.md.
//
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
var S3Delays = DelayOptions{
	MetaRead:       Latency{21 * time.Millisecond, 9 * time.Millisecond},
	MetaWrite:      Latency{75 * time.Millisecond, 19 * time.Millisecond},
	ObjRead:        Latency{22 * time.Millisecond, 9 * time.Millisecond},
	ObjWrite:       Latency{55 * time.Millisecond, 18 * time.Millisecond},
	List:           Latency{22 * time.Millisecond, 8 * time.Millisecond},
	SameObjWritePs: 3500,
	PrefixReadPs:   5500,
	PrefixWritePs:  3500,
	PrefixDepth:    2,
}

// DelayOptions configures simulated latency for each type of backend operation.
type DelayOptions struct {
	MetaRead  Latency
	MetaWrite Latency
	ObjRead   Latency
	ObjWrite  Latency
	List      Latency
	// How many writes per second to the same object before being
	// rate limited?
	SameObjWritePs int
	// PrefixReadPs and PrefixWritePs cap the GET/HEAD and PUT/POST/DELETE
	// request rates against a shared key prefix, modeling S3's documented
	// per-prefix request-rate limit (5,500 GET/HEAD and 3,500 PUT/POST/DELETE
	// per second per partitioned prefix). A request that would exceed the rate
	// is delayed (not failed) until the bucket refills, so the cap bounds
	// throughput without inflating transaction-retry counts. Zero disables the
	// corresponding limit.
	PrefixReadPs  int
	PrefixWritePs int
	// PrefixDepth selects how many leading '/'-separated path segments form a
	// throttled prefix, i.e. the partition granularity (e.g. depth 1 groups
	// every object under the database root into a single hot partition; depth 2
	// throttles each immediate subtree independently). Ignored when both
	// PrefixReadPs and PrefixWritePs are zero.
	PrefixDepth int
	// Scale multiplies all delay durations. Defaults to 1.0 if zero.
	// Use values < 1 to compress delays (e.g. 0.001 for 1000x speedup).
	Scale float64
}

// Latency describes the mean and standard deviation of an operation's duration.
type Latency struct {
	Mean   time.Duration
	StdDev time.Duration
}

// NewDelayBackend creates a DelayBackend that adds simulated latency to backend operations.
func NewDelayBackend(
	inner backend.Backend,
	opts DelayOptions,
) *DelayBackend {
	scale := opts.Scale
	if scale == 0 {
		scale = 1.0
	}
	return &DelayBackend{
		inner:     inner,
		scale:     scale,
		metaRead:  lognormalDelay(opts.MetaRead),
		metaWrite: lognormalDelay(opts.MetaWrite),
		objRead:   lognormalDelay(opts.ObjRead),
		objWrite:  lognormalDelay(opts.ObjWrite),
		list:      lognormalDelay(opts.List),
		rlimit: rateLimiter{
			tokensPerSec: opts.SameObjWritePs,
			scale:        scale,
			buckets:      map[string]bucketState{},
		},
		prefixReads:  newPrefixLimiter(opts.PrefixReadPs, opts.PrefixDepth, scale),
		prefixWrites: newPrefixLimiter(opts.PrefixWritePs, opts.PrefixDepth, scale),
		retryDelay:   time.Duration(float64(opts.ObjWrite.Mean*2) * scale),
	}
}

// DelayBackend is a backend.Backend decorator that simulates network latency,
// per-object write rate limiting, and per-prefix request-rate ceilings.
type DelayBackend struct {
	inner        backend.Backend
	scale        float64
	metaRead     lognormal
	metaWrite    lognormal
	objRead      lognormal
	objWrite     lognormal
	list         lognormal
	rlimit       rateLimiter
	prefixReads  *prefixLimiter
	prefixWrites *prefixLimiter
	retryDelay   time.Duration
}

// ReadIfModified implements backend.Backend.
func (b *DelayBackend) ReadIfModified(
	ctx context.Context,
	path string,
	expectedWriter backend.WriterID,
) (backend.ReadReply, error) {
	if err := b.prefixReads.wait(ctx, path); err != nil {
		return backend.ReadReply{}, err
	}
	b.delay(b.objRead)
	return b.inner.ReadIfModified(ctx, path, expectedWriter)
}

func (b *DelayBackend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	if err := b.prefixReads.wait(ctx, path); err != nil {
		return backend.ReadReply{}, err
	}
	b.delay(b.objRead)
	r, err := b.inner.Read(ctx, path)
	return r, err
}

// GetMetadata implements backend.Backend.
func (b *DelayBackend) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	if err := b.prefixReads.wait(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.metaRead)
	r, err := b.inner.GetMetadata(ctx, path)
	return r, err
}

// SetTagsIf implements backend.Backend.
func (b *DelayBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.metaWrite)
	return b.inner.SetTagsIf(ctx, path, expected, t)
}

func (b *DelayBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	r, err := b.inner.Write(ctx, path, value, t)
	return r, err
}

// WriteIf implements backend.Backend.
func (b *DelayBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	return b.inner.WriteIf(ctx, path, value, expected, t)
}

// WriteIfNotExists implements backend.Backend.
func (b *DelayBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	if err := b.backoff(ctx, path); err != nil {
		return backend.Metadata{}, err
	}
	b.delay(b.objWrite)
	return b.inner.WriteIfNotExists(ctx, path, value, t)
}

// Delete implements backend.Backend.
func (b *DelayBackend) Delete(ctx context.Context, path string) error {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return err
	}
	if err := b.backoff(ctx, path); err != nil {
		return err
	}
	b.delay(b.objWrite)
	err := b.inner.Delete(ctx, path)
	return err
}

// DeleteIf implements backend.Backend.
func (b *DelayBackend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	if err := b.prefixWrites.wait(ctx, path); err != nil {
		return err
	}
	if err := b.backoff(ctx, path); err != nil {
		return err
	}
	b.delay(b.objWrite)
	return b.inner.DeleteIf(ctx, path, expected)
}

// List implements backend.Backend.
func (b *DelayBackend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	if err := b.prefixReads.wait(ctx, dirPath); err != nil {
		return nil, err
	}
	b.delay(b.list)
	r, err := b.inner.List(ctx, dirPath)
	return r, err
}

func (b *DelayBackend) backoff(ctx context.Context, path string) error {
	r := concurr.NewRetrier(concurr.RetryConfig{
		InitialInterval: b.retryDelay,
		MaxInterval:     b.retryDelay * 10,
		Jitter:          true,
	})
	return r.Retry(ctx, func() error {
		if !b.rlimit.TryAcquireToken(path) {
			return errBackoff
		}
		return nil
	})
}

func (b *DelayBackend) delay(ln lognormal) {
	ms := ln.Rand()
	d := time.Duration(ms * float64(time.Millisecond) * b.scale)
	time.Sleep(d)
}

func lognormalDelay(l Latency) lognormal {
	m64 := float64(l.Mean) / float64(time.Millisecond)
	s64 := float64(l.StdDev) / float64(time.Millisecond)
	return newLognormalWith(m64, s64)
}

func newLognormalWith(mean, stddev float64) lognormal {
	// https://stats.stackexchange.com/a/95506
	sByM := stddev / mean
	v := math.Log(sByM*sByM + 1)
	return lognormal{
		Mu:    math.Log(mean) - 0.5*v,
		Sigma: math.Sqrt(v),
	}
}

type lognormal struct {
	Mu    float64
	Sigma float64
}

func (l lognormal) Rand() float64 {
	return math.Exp(rand.NormFloat64()*l.Sigma + l.Mu)
}

type rateLimiter struct {
	tokensPerSec int
	scale        float64
	buckets      map[string]bucketState
	m            sync.Mutex
}

func (r *rateLimiter) TryAcquireToken(key string) bool {
	if r.tokensPerSec == 0 {
		return false
	}

	r.m.Lock()
	defer r.m.Unlock()

	window := time.Duration(float64(time.Second) * r.scale)

	now := time.Now()
	entry, ok := r.buckets[key]
	if !ok {
		r.buckets[key] = bucketState{
			LastCheck: now,
			Tokens:    r.tokensPerSec - 1,
		}
		return true
	}

	elapsed := now.Sub(entry.LastCheck)
	if elapsed >= window {
		newTokens := min(entry.Tokens+int(float64(elapsed)/float64(window)*float64(r.tokensPerSec)), r.tokensPerSec)
		if newTokens <= 0 {
			return false
		}
		r.buckets[key] = bucketState{
			LastCheck: now,
			Tokens:    newTokens - 1,
		}
		return true
	}

	entry.Tokens--
	r.buckets[key] = entry
	return true
}

type bucketState struct {
	LastCheck time.Time
	Tokens    int
}

// newPrefixLimiter builds a per-prefix request-rate limiter, or returns nil
// (no throttling) when ratePerSec is non-positive. depth selects how many
// leading path segments form the throttled prefix.
func newPrefixLimiter(ratePerSec, depth int, scale float64) *prefixLimiter {
	if ratePerSec <= 0 {
		return nil
	}
	if depth < 1 {
		depth = 1
	}
	// Delays are sleep-scaled by scale (see delay), so a sub-unit scale
	// compresses wall-clock time; the request rate must grow by the same
	// factor to keep the simulated rate constant.
	return &prefixLimiter{
		rate:    float64(ratePerSec) / scale,
		burst:   float64(ratePerSec),
		depth:   depth,
		buckets: map[string]*tokenBucket{},
	}
}

// prefixLimiter caps the request rate against a shared key prefix using a
// continuous token bucket per prefix. Unlike rateLimiter (tuned for infrequent
// per-object writes), it behaves correctly under thousands of concurrent
// acquisitions per second: callers that exceed the rate are told how long to
// wait, and that debt accumulates so the long-run rate converges to the cap.
type prefixLimiter struct {
	rate    float64 // tokens added per wall-clock second
	burst   float64 // bucket capacity, in tokens
	depth   int
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens   float64
	lastFill time.Time
}

// wait blocks until a request token for path's prefix is available, or until
// ctx is done. A nil limiter never blocks.
func (l *prefixLimiter) wait(ctx context.Context, path string) error {
	if l == nil {
		return nil
	}
	d := l.reserve(prefixKey(path, l.depth), time.Now())
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// reserve takes a token for key and returns how long the caller must wait
// before the request may proceed (zero if a token was immediately available).
func (l *prefixLimiter) reserve(key string, now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: l.burst, lastFill: now}
		l.buckets[key] = b
	}
	if elapsed := now.Sub(b.lastFill).Seconds(); elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.lastFill = now
	}
	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	// Negative tokens represent queued demand: wait for them to refill.
	return time.Duration(-b.tokens / l.rate * float64(time.Second))
}

// prefixKey returns the first depth '/'-separated segments of path, which
// defines the granularity at which the request-rate ceiling is applied.
func prefixKey(path string, depth int) string {
	idx := 0
	for range depth {
		next := indexByteFrom(path, '/', idx)
		if next < 0 {
			return path
		}
		idx = next + 1
	}
	return path[:idx-1]
}

func indexByteFrom(s string, c byte, from int) int {
	for i := from; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*DelayBackend)(nil)
