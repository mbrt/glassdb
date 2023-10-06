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
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

func NewSelfAdvanceClock(t *testing.T) clockwork.Clock {
	c := clockwork.NewFakeClock()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			c.BlockUntil(1)
			c.Advance(100 * time.Millisecond)
			// Wait some real time before advancing again.
			// This allows sleepers to start running.
			time.Sleep(100 * time.Microsecond)
		}
	}()

	return c
}

func NewAcceleratedClock(multiplier int) clockwork.Clock {
	c := clockwork.NewRealClock()
	return aclock{
		inner:      c,
		multiplier: multiplier,
		epoch:      c.Now(),
	}
}

type aclock struct {
	inner      clockwork.Clock
	multiplier int
	epoch      time.Time
}

func (c aclock) After(d time.Duration) <-chan time.Time {
	return c.inner.After(c.compress(d))
}

func (c aclock) Sleep(d time.Duration) {
	d = c.compress(d)
	// The only guarantee for a sleep is that it will take "at lest" the
	// duration you give it. This means sometimes sleeping way longer than the
	// duration and this results in much slower tests. I observed that often
	// times a microsecond sleep results in a millisecond waiting.
	//
	// The way to fix this is to trade in some CPU and do "busy waiting" instead
	// for short durations.
	if d < 500*time.Microsecond {
		busyWait(d)
	} else {
		c.inner.Sleep(d)
	}
}

func (c aclock) Now() time.Time {
	since := c.inner.Since(c.epoch)
	return c.epoch.Add(c.expand(since))
}

func (c aclock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c aclock) NewTicker(d time.Duration) clockwork.Ticker {
	return c.inner.NewTicker(c.compress(d))
}

func (c aclock) NewTimer(d time.Duration) clockwork.Timer {
	return atimer{
		Timer:      c.inner.NewTimer(c.compress(d)),
		multiplier: c.multiplier,
	}
}

func (c aclock) AfterFunc(d time.Duration, fn func()) clockwork.Timer {
	d = c.compress(d)
	t := c.inner.AfterFunc(d, fn)
	return atimer{
		Timer:      t,
		multiplier: c.multiplier,
	}
}

func (c aclock) compress(d time.Duration) time.Duration {
	return d / time.Duration(c.multiplier)
}

func (c aclock) expand(d time.Duration) time.Duration {
	return d * time.Duration(c.multiplier)
}

type atimer struct {
	clockwork.Timer
	multiplier int
}

func (t atimer) Reset(d time.Duration) bool {
	return t.Timer.Reset(d / time.Duration(t.multiplier))
}

func busyWait(d time.Duration) {
	// Wait by repeatedly yielding to the scheduler.
	start := time.Now()
	runtime.Gosched()
	for time.Since(start) < d {
		runtime.Gosched()
	}
}

// Make sure the Clock interface is implemented correctly.
var _ clockwork.Clock = (*aclock)(nil)
