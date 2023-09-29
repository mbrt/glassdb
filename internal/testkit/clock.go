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
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

var timeChPool = sync.Pool{
	New: func() any {
		return make(chan time.Time, 1)
	},
}

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
	c.inner.Sleep(c.compress(d))
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

func NewSimulatedClock(resolution, rtResolution time.Duration) *SimulatedClock {
	// Deterministic epoch different than zero.
	epoch, _ := time.Parse(time.RFC3339, "2020-02-01T03:02:01Z")

	c := &SimulatedClock{
		rtResolution: rtResolution,
		resolution:   resolution,
		epoch:        epoch,
		stop:         make(chan token),
	}
	go c.runLoop()

	return c
}

type SimulatedClock struct {
	// We will wait for this duration for new waiters. If none arrive int this
	// interval, we will advance 'now' until the earliest sleeper.
	rtResolution time.Duration
	resolution   time.Duration
	epoch        time.Time
	nowTicks     int64
	q            timePQ
	stop         chan token
	m            sync.Mutex
}

func (c *SimulatedClock) Close() {
	close(c.stop)

	c.m.Lock()
	defer c.m.Unlock()

	// Unblock all waiters.
	now := c.nowTicks
	for _, item := range c.q {
		item.waiter <- c.ticksToTime(now)
	}
}

func (c *SimulatedClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.afterChan(d, ch)
	return ch
}

func (c *SimulatedClock) Sleep(d time.Duration) {
	// The channel never leaves this function. Save some GC rounds by reusing
	// them in a pool.
	ch := timeChPool.Get().(chan time.Time)
	defer timeChPool.Put(ch)
	c.afterChan(d, ch)
	<-ch
}

func (c *SimulatedClock) Now() time.Time {
	c.m.Lock()
	ticks := c.nowTicks
	c.m.Unlock()
	return c.ticksToTime(ticks)
}

func (c *SimulatedClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *SimulatedClock) NewTicker(d time.Duration) clockwork.Ticker {
	return newSimulatedTicker(c, d)
}

func (c *SimulatedClock) NewTimer(d time.Duration) clockwork.Timer {
	return newSimulatedTimer(c, d, func() {})
}

func (c *SimulatedClock) AfterFunc(d time.Duration, fn func()) clockwork.Timer {
	return newSimulatedTimer(c, d, fn)
}

func (c *SimulatedClock) ticksToTime(ticks int64) time.Time {
	return c.epoch.Add(c.resolution * time.Duration(ticks))
}

func (c *SimulatedClock) afterChan(d time.Duration, ch chan time.Time) {
	// Never wait less than 1 tick.
	dTicks := max(durationTicks(d, c.resolution), 1)
	log.Printf("duration: %v, ticks: %d", d, dTicks)

	c.m.Lock()
	defer c.m.Unlock()

	item := waitItem{
		t:      c.nowTicks + dTicks,
		waiter: ch,
	}
	heap.Push(&c.q, item)
}

func (c *SimulatedClock) runLoop() {
	for {
		if !c.waitForNext() {
			return
		}

		// Advance the clock until the earliest sleeper and notify it.
		c.m.Lock()

		for len(c.q) > 0 {
			top := c.q[0]
			if top.t > c.nowTicks {
				break
			}
			heap.Pop(&c.q)
			top.waiter <- c.ticksToTime(c.nowTicks)
		}
		c.m.Unlock()
	}
}

func (c *SimulatedClock) waitForNext() bool {
	for {
		c.waitRT()

		select {
		case <-c.stop:
			return false
		default:
		}

		c.m.Lock()
		if len(c.q) == 0 {
			c.m.Unlock()
			continue
		}

		// Advance the clock to the top element.
		top := c.q[0]
		if top.t < c.nowTicks {
			panic("Clock moved too fast")
		}
		c.nowTicks = top.t
		c.m.Unlock()

		return true
	}
}

func (c *SimulatedClock) waitRT() {
	if c.rtResolution >= 500*time.Microsecond {
		time.Sleep(c.rtResolution)
		return
	}

	// If we only relied on time.Sleep(), short sleeps would often result in 1ms
	// sleep. Instead of that, we busy wait by handing it over to the go
	// scheduler until the time has passed.
	start := time.Now()
	runtime.Gosched()
	for time.Since(start) < c.rtResolution {
		runtime.Gosched()
	}
}

type waitItem struct {
	t      int64
	waiter chan<- time.Time
}

type token struct{}

type timePQ []waitItem

func (q timePQ) Len() int { return len(q) }

func (q timePQ) Less(i, j int) bool {
	return q[i].t < q[j].t
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
		c:        make(chan time.Time, 1),
		changeCh: make(chan time.Duration, 1),
	}
	st.changeCh <- d
	go st.run()

	return st
}

type simulatedTicker struct {
	clock    *SimulatedClock
	c        chan time.Time
	changeCh chan time.Duration
	stopped  bool
	m        sync.Mutex
}

func (t *simulatedTicker) Chan() <-chan time.Time {
	return t.c
}

func (t *simulatedTicker) Reset(d time.Duration) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.stopped {
		t.stopped = false
		go t.run()
	}
	t.changeCh <- d
}

func (t *simulatedTicker) Stop() {
	t.m.Lock()
	t.stopped = true
	t.changeCh <- 0
	t.m.Unlock()
}

func (t *simulatedTicker) run() {
	duration := <-t.changeCh

	for {
		if duration == 0 {
			return
		}

		select {
		case <-t.clock.stop:
			return
		case d := <-t.changeCh:
			duration = d
			continue
		case now := <-t.clock.After(duration):
			t.c <- now
		}
	}
}

func newSimulatedTimer(c *SimulatedClock, d time.Duration, fn func()) *simulatedTimer {
	t := &simulatedTimer{
		clock: c,
		ch:    make(chan time.Time, 1),
		fn:    fn,
	}
	t.run(d, 0)
	return t
}

type simulatedTimer struct {
	clock   *SimulatedClock
	ch      chan time.Time
	fn      func()
	stopped bool
	version int
	m       sync.Mutex
}

func (t *simulatedTimer) Chan() <-chan time.Time {
	return t.ch
}

// Reset changes the timer to expire after duration d. It returns true if the
// timer had been active, false if the timer had expired or been stopped.
func (t *simulatedTimer) Reset(d time.Duration) bool {
	t.m.Lock()
	t.version++
	wasRunning := t.stopped
	t.stopped = false
	t.m.Unlock()

	t.run(d, t.version)
	return wasRunning
}

// Stop prevents the Timer from firing. It returns true if the call stops the
// timer, false if the timer has already expired or been stopped. Stop does not
// close the channel, to prevent a read from the channel succeeding incorrectly.
func (t *simulatedTimer) Stop() bool {
	t.m.Lock()
	stopped := !t.stopped
	t.stopped = true
	t.m.Unlock()
	return stopped
}

func (t *simulatedTimer) run(d time.Duration, version int) {
	go func() {
		select {
		case <-t.clock.stop:
		case now := <-t.clock.After(d):
			if !t.shouldFire(version) {
				return
			}
			t.ch <- now
			t.fn()
		}
	}()
}

func (t *simulatedTimer) shouldFire(version int) bool {
	t.m.Lock()
	defer t.m.Unlock()
	if t.version != version {
		return false
	}
	if t.stopped {
		return false
	}
	t.stopped = true
	return true
}

func durationTicks(d, res time.Duration) int64 {
	// If duration is not exact multiple of resolution, round it up.
	if d%res == 0 {
		return int64(d / res)
	}
	return int64(d/res + 1)
}

// Make sure the Clock interface is implemented correctly.
var (
	_ clockwork.Clock = (*aclock)(nil)
	_ clockwork.Clock = (*SimulatedClock)(nil)
)
