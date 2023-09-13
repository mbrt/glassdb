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
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
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

func NewSimulatedClock() *SimulatedClock {
	// Just to avoid having a random epoch nor unix zero.
	simNow, _ := time.Parse(time.RFC3339, "2020-02-01T03:02:01Z00:00")

	c := &SimulatedClock{
		waitSlot: time.Millisecond,
		now:      simNow,
		stop:     make(chan token),
	}
	go c.runLoop()

	return c
}

type SimulatedClock struct {
	// We will wait for this duration for new waiters. If none arrive int this
	// interval, we will advance 'now' until the earliest sleeper.
	waitSlot time.Duration
	now      time.Time
	q        timePQ
	stop     chan token
	m        sync.Mutex
}

func (c *SimulatedClock) Close() {
	close(c.stop)

	c.m.Lock()
	defer c.m.Unlock()

	// Unblock all waiters.
	for _, item := range c.q {
		item.waiter <- c.now
	}
}

func (c *SimulatedClock) After(d time.Duration) <-chan time.Time {
	now := c.Now()
	return c.waitUntil(now.Add(d))
}

func (c *SimulatedClock) Sleep(d time.Duration) {
	now := c.Now()
	<-c.waitUntil(now.Add(d))
}

func (c *SimulatedClock) Now() time.Time {
	c.m.Lock()
	defer c.m.Unlock()
	return c.now
}

func (c *SimulatedClock) Since(t time.Time) time.Duration {
	now := c.Now()
	return now.Sub(t)
}

func (c *SimulatedClock) NewTicker(d time.Duration) clockwork.Ticker {
	return newSimulatedTicker(c, d)
}

func (c *SimulatedClock) NewTimer(d time.Duration) clockwork.Timer {
	return newSimulatedTimer(c, d)
}

func (c *SimulatedClock) waitUntil(t time.Time) <-chan time.Time {
	ch := make(chan time.Time, 1)
	item := waitItem{
		t:      t,
		waiter: ch,
	}
	heap.Push(&c.q, item)
	return ch
}

func (c *SimulatedClock) runLoop() {
	for {
		if !c.waitForWaiters() {
			return
		}

		// Advance the clock until the earliest sleeper and notify it.
		c.m.Lock()
		top := heap.Pop(&c.q).(waitItem)
		invalidTime := false
		if top.t.Before(c.now) {
			invalidTime = true
		}
		c.now = top.t
		c.m.Unlock()

		top.waiter <- top.t

		if invalidTime {
			panic("Simulated clock decremented")
		}
	}
}

func (c *SimulatedClock) waitForWaiters() bool {
	for {
		select {
		case <-c.stop:
			return false
		default:
		}

		c.m.Lock()
		waitSize1 := c.q.Len()
		c.m.Unlock()

		time.Sleep(c.waitSlot)

		c.m.Lock()
		waitSize2 := c.q.Len()
		c.m.Unlock()

		if waitSize1 == waitSize2 && waitSize2 > 0 {
			return true
		}
	}
}

type waitItem struct {
	t      time.Time
	waiter chan<- time.Time
}

type token struct{}

type timePQ []waitItem

func (q timePQ) Len() int { return len(q) }

func (q timePQ) Less(i, j int) bool {
	return q[i].t.Before(q[j].t)
}

func (q timePQ) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *timePQ) Push(x any) {
	item := x.(waitItem)
	*q = append(*q, item)
}

func (q *timePQ) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = waitItem{} // Avoid memory leak.
	*q = old[0 : n-1]
	return item
}

func newSimulatedTicker(c *SimulatedClock, d time.Duration) *simulatedTicker {
	st := &simulatedTicker{
		clock:    c,
		interval: d,
		c:        make(chan time.Time, 1),
		changeCh: make(chan token),
	}

	go func() {
		for {
			next, ok := st.nextTick()
			if !ok {
				return
			}

			select {
			case <-c.stop:
				return
			case <-st.changeCh:
				continue
			case now := <-c.waitUntil(next):
				st.c <- now
			}
		}
	}()

	return st
}

type simulatedTicker struct {
	clock    *SimulatedClock
	interval time.Duration
	c        chan time.Time
	changeCh chan token
	m        sync.Mutex
}

func (t *simulatedTicker) Chan() <-chan time.Time {
	return t.c
}

func (t *simulatedTicker) Reset(d time.Duration) {
	t.m.Lock()
	t.interval = d
	t.changeCh <- token{}
	t.m.Unlock()
}

func (t *simulatedTicker) Stop() {
	t.Reset(0)
}

func (t *simulatedTicker) nextTick() (time.Time, bool) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.interval == 0 {
		return time.Time{}, false
	}
	return t.clock.Now().Add(t.interval), true
}

func newSimulatedTimer(c *SimulatedClock, d time.Duration) *simulatedTimer {
	t := &simulatedTimer{
		clock: c,
		ch:    make(chan time.Time, 1),
	}
	t.run(d)
	return t
}

type simulatedTimer struct {
	clock *SimulatedClock
	ch    chan time.Time
	stop  chan token
	fired atomic.Bool
}

func (t *simulatedTimer) Chan() <-chan time.Time {
	return t.ch
}

func (t *simulatedTimer) Reset(d time.Duration) bool {
	wasRunning := t.fired.Swap(false)
	t.run(d)
	return !wasRunning
}

func (t *simulatedTimer) Stop() bool {
	close(t.stop)
	return t.fired.Load()
}

func (t *simulatedTimer) run(d time.Duration) {
	t.stop = make(chan token)

	go func() {
		select {
		case <-t.clock.stop:
		case <-t.stop:
		case now := <-t.clock.waitUntil(t.clock.Now().Add(d)):
			t.fired.Store(true)
			t.ch <- now
		}
	}()
}
