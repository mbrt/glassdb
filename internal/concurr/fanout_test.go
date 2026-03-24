package concurr

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestFanout(t *testing.T) {
	ctx := context.Background()
	f := NewFanout(3)
	v := make([]bool, 3)
	res := f.Spawn(ctx, 3, func(_ context.Context, i int) error {
		v[i] = true
		return nil
	})
	assert.NoError(t, res.Wait())
	for _, b := range v {
		assert.True(t, b)
	}
}

func TestParallel(t *testing.T) {
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	f := NewFanout(3)

	r1 := f.Spawn(ctx, 2, func(context.Context, int) error {
		clock.Sleep(100 * time.Millisecond)
		return nil
	})
	r2 := f.Spawn(ctx, 1, func(context.Context, int) error {
		clock.Sleep(100 * time.Millisecond)
		return nil
	})
	// Block until the goroutines start waiting.
	clock.BlockUntil(3)
	// Give enough time to run in parallel but not in series.
	clock.Advance(110 * time.Millisecond)

	assert.NoError(t, r1.Wait())
	assert.NoError(t, r2.Wait())
}

func TestLimit(t *testing.T) {
	ctx := context.Background()
	// We need a real clock, because it's hard to avoid race conditions
	// in the test with the fake clock.
	clock := clockwork.NewRealClock()
	f := NewFanout(2)

	start := clock.Now()
	v := make([]time.Duration, 3)
	r := f.Spawn(ctx, 3, func(_ context.Context, i int) error {
		clock.Sleep(50 * time.Millisecond)
		v[i] = clock.Since(start)
		return nil
	})

	assert.NoError(t, r.Wait())
	assert.Less(t, v[0], 90*time.Millisecond)
	assert.Less(t, v[1], 90*time.Millisecond)
	assert.Greater(t, v[2], 90*time.Millisecond)
}
