package testkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAcceleratedClock(t *testing.T) {
	c := NewAcceleratedClock(100)
	startA := c.Now()
	startR := time.Now()
	c.Sleep(time.Second)
	assertAround(t, 10*time.Millisecond, time.Since(startR))
	assertAround(t, time.Second, c.Since(startA))
}

func TestAcceleratedTimer(t *testing.T) {
	c := NewAcceleratedClock(100)
	startA := c.Now()
	startR := time.Now()
	timer := c.NewTimer(time.Second)
	defer timer.Stop()

	ch := timer.Chan()

	for i := 0; i < 10; i++ {
		<-ch
		assertAround(t, 10*time.Millisecond, time.Since(startR))
		assertAround(t, time.Second, c.Since(startA))

		timer.Reset(time.Second)
		startA = c.Now()
		startR = time.Now()
	}
}

func assertAround(t *testing.T, expected, got time.Duration) {
	t.Helper()
	assert.Less(t, got, expected*4)
	assert.Greater(t, got, expected/4)
}
