package middleware

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	clock := clockwork.NewFakeClock()
	rt := rateLimiter{
		tokensPerSec: 1,
		clock:        clock,
		buckets:      make(map[string]bucketState),
	}

	begin := clock.Now()
	assert.True(t, rt.TryAcquireToken("k"))
	clock.Advance(100 * time.Millisecond)
	assert.True(t, rt.TryAcquireToken("k"))
	clock.Advance(100 * time.Millisecond)
	assert.True(t, rt.TryAcquireToken("k"))
	clock.Advance(700 * time.Millisecond)
	assert.True(t, rt.TryAcquireToken("k"))
	clock.Advance(150 * time.Millisecond)
	// Here 1050ms passed.
	// We were able to sneak in 3 extra requests, so we should be rejected for
	// around 4 seconds.
	for clock.Now().Sub(begin) < 4*time.Second {
		assert.False(t, rt.TryAcquireToken("k"), "elapsed: %v", clock.Now().Sub(begin))
		clock.Advance(250 * time.Millisecond)
	}
	// We can sneak in more requests.
	for i := 0; i < 5; i++ {
		assert.True(t, rt.TryAcquireToken("k"), "i: %d", i)
	}
	clock.Advance(time.Second)
	begin = clock.Now()
	// And now we should be blocked again for 5 seconds.
	for clock.Now().Sub(begin) < 4*time.Second {
		assert.False(t, rt.TryAcquireToken("k"), "elapsed: %v", clock.Now().Sub(begin))
		clock.Advance(250 * time.Millisecond)
	}
	assert.True(t, rt.TryAcquireToken("k"))
}
