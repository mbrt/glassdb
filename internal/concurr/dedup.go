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

package concurr

import (
	"context"
	"sync"
)

func NewDedup(w Worker) *Dedup {
	return &Dedup{
		work: w,
		contr: controller{
			calls: make(map[string]*call),
		},
	}
}

type Dedup struct {
	work  Worker
	contr controller
}

func (d *Dedup) Do(ctx context.Context, key string, r Request) error {
	return d.contr.Do(ctx, key, r, d.work)
}

type Request interface {
	Merge(other Request) (Request, bool)
	CanReorder() bool
}

type Worker interface {
	Work(ctx context.Context, key string, cntr DedupContr) error
}

type DedupContr interface {
	Request(key string) Request
	OnNextDo(key string) <-chan struct{}
}

type controller struct {
	calls map[string]*call
	m     sync.Mutex
}

func (d *controller) Do(ctx context.Context, key string, r Request, w Worker) error {
	var err error

	d.m.Lock()
	c, inprog := d.calls[key]
	if !inprog {
		c = &call{
			Curr: requestBundle{
				Main: requestCtx{
					Ctx:     ctx,
					Request: r,
				},
			},
		}
		d.calls[key] = c
		d.m.Unlock()

		err = w.Work(ctx, key, d)
	} else {
		notifyCh := make(chan callResult, 1)
		rctx := requestCtx{
			Ctx:      ctx,
			Request:  r,
			NotifyCh: notifyCh,
		}
		if r.CanReorder() {
			c.Pending = append(c.Pending, rctx)
		} else {
			c.Queue = append(c.Queue, rctx)
		}
		// Notify workers waiting on OnNextDo.
		// Avoid deadlocks when the channel is full. Only one notification is
		// necessary.
		// TODO: Add unit test for this case.
		if c.NextCh != nil {
			select {
			case c.NextCh <- struct{}{}:
			default:
			}
		}
		d.m.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case n := <-notifyCh:
			if !n.IsNext {
				return n.Err
			}
			err = w.Work(ctx, key, d)
			// Fall through the request handling logic.
		}
	}

	d.m.Lock()
	// The main request completed.
	c.Curr.Main = requestCtx{}

	if err == nil || ctx.Err() == nil {
		// If the context expired, do not notify the whole bundle.
		// Some other request in there could pick up the request.
		d.notifyBundle(c.Curr, err)
		c.Curr = requestBundle{}
	}
	if c.NextCh != nil {
		// After the main call completed, we know there's no one waiting for
		// the next Do call. Avoid unnecessary queuing by getting rid of the
		// channel now.
		c.NextCh = nil
	}
	d.wakeUpNext(key, c)
	d.m.Unlock()

	return err
}

func (d *controller) Request(key string) Request {
	d.m.Lock()
	defer d.m.Unlock()
	c := d.calls[key]

	c.Curr.Other = filterExpiredReqs(c.Curr.Other)
	c.Pending = filterExpiredReqs(c.Pending)
	c.Queue = filterExpiredReqs(c.Queue)

	// Start by reconstructing the current bundle.
	newReq := c.Curr.Main.Request
	for _, r := range c.Curr.Other {
		// Requests in the bundle are mergeable by definition.
		var ok bool
		newReq, ok = newReq.Merge(r.Request)
		if !ok {
			panic("dedup: unexpected non-mergeable request in the bundle")
		}
	}

	// Merge pending requests. Any mergeable will join the bundle.
	i := 0
	for _, r := range c.Pending {
		mr, ok := newReq.Merge(r.Request)
		if !ok {
			c.Pending[i] = r
			i++
			continue
		}
		newReq = mr
		c.Curr.Other = append(c.Curr.Other, r)
	}
	c.Pending = c.Pending[:i]

	// Merge from the queue as well.
	// However, since the queue is ordered, stop the first moment we can't
	// merge.
	i = 0
	for _, r := range c.Queue {
		mr, ok := newReq.Merge(r.Request)
		if !ok {
			break
		}
		newReq = mr
		c.Curr.Other = append(c.Curr.Other, r)
		i++
	}
	c.Queue = c.Queue[i:]

	return newReq
}

func (d *controller) OnNextDo(key string) <-chan struct{} {
	d.m.Lock()
	defer d.m.Unlock()

	c := d.calls[key]
	if c.NextCh == nil {
		c.NextCh = make(chan struct{}, 3)
	}
	return c.NextCh
}

func (d *controller) notifyBundle(bundle requestBundle, err error) {
	for _, r := range bundle.Other {
		r.NotifyCh <- callResult{Err: err}
	}
}

func (d *controller) wakeUpNext(key string, c *call) {
	c.Curr.Other = filterExpiredReqs(c.Curr.Other)
	c.Pending = filterExpiredReqs(c.Pending)
	c.Queue = filterExpiredReqs(c.Queue)

	switch {
	case len(c.Curr.Other) > 0:
		c.Curr.Main = c.Curr.Other[0]
		c.Curr.Other = c.Curr.Other[1:]
	case len(c.Queue) > 0:
		c.Curr.Main = c.Queue[0]
		c.Queue = c.Queue[1:]
	case len(c.Pending) > 0:
		c.Curr.Main = c.Pending[0]
		c.Pending = c.Pending[1:]
	default:
		// Nothing to wake up. Cleanup.
		delete(d.calls, key)
		return
	}

	// Wake up the new main request.
	c.Curr.Main.NotifyCh <- callResult{IsNext: true}
}

type call struct {
	Curr    requestBundle
	Pending []requestCtx
	Queue   []requestCtx
	NextCh  chan struct{}
}

type requestCtx struct {
	Ctx      context.Context
	Request  Request
	NotifyCh chan callResult
}

type callResult struct {
	Err    error
	IsNext bool
}

type requestBundle struct {
	Main  requestCtx
	Other []requestCtx
}

func filterExpiredReqs(rs []requestCtx) []requestCtx {
	i := 0
	for _, r := range rs {
		if r.Ctx.Err() == nil {
			rs[i] = r
			i++
		}
	}
	return rs[:i]
}
