// Copyright 2023 The glassdb Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testkit

import (
	"testing"
	"time"

	"github.com/sourcegraph/conc"
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

func newTestSimClock(t *testing.T) *SimulatedClock {
	c := NewSimulatedClock(10*time.Millisecond, 10*time.Microsecond)
	t.Cleanup(c.Close)
	return c
}

func TestSimulatedSleep(t *testing.T) {
	c := newTestSimClock(t)

	start := c.Now()
	c.Sleep(10 * time.Millisecond)
	assert.Equal(t, c.Since(start), 10*time.Millisecond)

	// Always sleep to the next round tick.
	start = c.Now()
	c.Sleep(12 * time.Millisecond)
	assert.Equal(t, 20*time.Millisecond, c.Since(start))

	start = c.Now()

	// Make sure the execution is ordered correctly.
	nowCh := make(chan time.Time, 2)

	var wg conc.WaitGroup
	wg.Go(func() {
		c.Sleep(100 * time.Millisecond)
		nowCh <- c.Now()
	})
	c.Sleep(time.Minute)
	nowCh <- c.Now()

	wg.Wait()
	assert.Equal(t, 100*time.Millisecond, (<-nowCh).Sub(start))
	assert.Equal(t, time.Minute, (<-nowCh).Sub(start))
}

func TestSimulatedTicker(t *testing.T) {
	c := newTestSimClock(t)
	start := c.Now()

	ticker := c.NewTicker(time.Second)
	defer ticker.Stop()

	stopCh := c.After(10*time.Second + 50*time.Millisecond)
	timeoutCh := c.After(12 * time.Second)
	var results []time.Time

loop:
	for {
		select {
		case <-stopCh:
			ticker.Stop()
		case <-timeoutCh:
			break loop
		case now := <-ticker.Chan():
			results = append(results, now)
		}
	}

	assert.Len(t, results, 10)
	for i := 0; i < len(results); i++ {
		assert.Equal(t, time.Duration(i+1)*time.Second, results[i].Sub(start))
	}
}

func assertEmptyCh(t *testing.T, ch <-chan time.Time) {
	select {
	case <-ch:
		t.Error("expected timer to fire after 1s")
	default:
	}
}

func TestSimulatedTimer(t *testing.T) {
	c := newTestSimClock(t)
	start := c.Now()

	timer := c.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case now := <-timer.Chan():
		assert.Equal(t, time.Second, c.Since(start))
		assert.Equal(t, time.Second, now.Sub(start))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlocked timer")
	}

	// New run.
	timer.Reset(time.Second)

	c.Sleep(500 * time.Millisecond)
	assertEmptyCh(t, timer.Chan())
	// Stop before the timer should fire.
	timer.Stop()
	c.Sleep(time.Second)
	assertEmptyCh(t, timer.Chan())
}
