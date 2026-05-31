package middleware

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := rateLimiter{
			tokensPerSec: 1,
			scale:        1.0,
			buckets:      make(map[string]bucketState),
		}

		begin := time.Now()
		assert.True(t, rt.TryAcquireToken("k"))
		time.Sleep(100 * time.Millisecond)
		assert.True(t, rt.TryAcquireToken("k"))
		time.Sleep(100 * time.Millisecond)
		assert.True(t, rt.TryAcquireToken("k"))
		time.Sleep(700 * time.Millisecond)
		assert.True(t, rt.TryAcquireToken("k"))
		time.Sleep(150 * time.Millisecond)
		// Here 1050ms passed.
		// We were able to sneak in 3 extra requests, so we should be rejected for
		// around 4 seconds.
		for time.Since(begin) < 4*time.Second {
			assert.False(t, rt.TryAcquireToken("k"), "elapsed: %v", time.Since(begin))
			time.Sleep(250 * time.Millisecond)
		}
		// We can sneak in more requests.
		for i := range 5 {
			assert.True(t, rt.TryAcquireToken("k"), "i: %d", i)
		}
		time.Sleep(time.Second)
		begin = time.Now()
		// And now we should be blocked again for 5 seconds.
		for time.Since(begin) < 4*time.Second {
			assert.False(t, rt.TryAcquireToken("k"), "elapsed: %v", time.Since(begin))
			time.Sleep(250 * time.Millisecond)
		}
		assert.True(t, rt.TryAcquireToken("k"))
	})
}

func TestPrefixLimiterDisabled(t *testing.T) {
	// A non-positive rate yields a nil limiter that never blocks.
	assert.Nil(t, newPrefixLimiter(0, 1, 1.0))
	var l *prefixLimiter
	assert.NoError(t, l.wait(t.Context(), "any/path"))
}

func TestPrefixLimiterReserve(t *testing.T) {
	l := newPrefixLimiter(100, 1, 1.0)
	require.NotNil(t, l)
	now := time.Now()

	// The first `burst` (== rate) tokens are served immediately.
	for i := range 100 {
		assert.Zero(t, l.reserve("bench", now), "token %d", i)
	}
	// Beyond the burst, demand queues: each extra request waits one more
	// token-refill interval (1/rate == 10ms at 100/s).
	assert.Equal(t, 10*time.Millisecond, l.reserve("bench", now))
	assert.Equal(t, 20*time.Millisecond, l.reserve("bench", now))

	// A different prefix has an independent bucket.
	assert.Zero(t, l.reserve("other", now))

	// After a second of wall-clock time the bucket refills at the configured
	// rate, so requests are served immediately again.
	assert.Zero(t, l.reserve("bench", now.Add(time.Second)))
}

func TestPrefixLimiterScale(t *testing.T) {
	// Compressing time by 1000x must raise the wall-clock rate by 1000x so the
	// simulated rate is unchanged: the post-burst wait shrinks from 10ms to
	// 10us.
	l := newPrefixLimiter(100, 1, 1.0/1000)
	require.NotNil(t, l)
	now := time.Now()
	for range 100 {
		l.reserve("bench", now)
	}
	assert.Equal(t, 10*time.Microsecond, l.reserve("bench", now))
}

func TestPrefixKey(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		depth int
		want  string
	}{
		{"depth1", "bench/_c/abc/_k/def", 1, "bench"},
		{"depth2", "bench/_c/abc/_k/def", 2, "bench/_c"},
		{"depth3", "bench/_t/xyz", 3, "bench/_t/xyz"},
		{"shorter than depth", "bench", 2, "bench"},
		{"exact segments", "a/b", 2, "a/b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, prefixKey(tc.path, tc.depth))
		})
	}
}
