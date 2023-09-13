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

func TestSimulatedSleep(t *testing.T) {
	c := NewSimulatedClock(10*time.Millisecond, 250*time.Microsecond)
	defer c.Close()

	start := c.Now()
	c.Sleep(10 * time.Millisecond)
	assert.Equal(t, c.Since(start), 10*time.Millisecond)

	// Always sleep to the next round tick.
	start = c.Now()
	c.Sleep(12 * time.Millisecond)
	assert.Equal(t, 20*time.Millisecond, c.Since(start))

	start = c.Now()

	// Make sure the execution is
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
