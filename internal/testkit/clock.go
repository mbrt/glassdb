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
		Clock:      c,
		multiplier: multiplier,
		epoch:      c.Now(),
	}
}

type aclock struct {
	clockwork.Clock
	multiplier int
	epoch      time.Time
}

func (c aclock) After(d time.Duration) <-chan time.Time {
	return c.Clock.After(c.compress(d))
}

func (c aclock) Sleep(d time.Duration) {
	c.Clock.Sleep(c.compress(d))
}

func (c aclock) Now() time.Time {
	since := c.Clock.Since(c.epoch)
	return c.epoch.Add(c.expand(since))
}

func (c aclock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c aclock) NewTicker(d time.Duration) clockwork.Ticker {
	return c.Clock.NewTicker(c.compress(d))
}

func (c aclock) NewTimer(d time.Duration) clockwork.Timer {
	return atimer{
		Timer:      c.Clock.NewTimer(c.compress(d)),
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
