package middleware

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
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
