package concurr

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFanout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

func TestParallel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		f := NewFanout(3)

		r1 := f.Spawn(ctx, 2, func(context.Context, int) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		r2 := f.Spawn(ctx, 1, func(context.Context, int) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})

		assert.NoError(t, r1.Wait())
		assert.NoError(t, r2.Wait())
	})
}

func TestLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		f := NewFanout(2)

		start := time.Now()
		v := make([]time.Duration, 3)
		r := f.Spawn(ctx, 3, func(_ context.Context, i int) error {
			time.Sleep(50 * time.Millisecond)
			v[i] = time.Since(start)
			return nil
		})

		assert.NoError(t, r.Wait())
		// First two run in parallel (finish at 50ms), third waits (finishes at 100ms).
		assert.Less(t, v[0], 90*time.Millisecond)
		assert.Less(t, v[1], 90*time.Millisecond)
		assert.Greater(t, v[2], 90*time.Millisecond)
	})
}
