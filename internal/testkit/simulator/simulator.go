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

package simulator

import (
	"context"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/stringset"
	"github.com/mbrt/glassdb/internal/testkit"
	"github.com/mbrt/glassdb/internal/trans"
)

var validNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type TestGoroutine func(context.Context, *glassdb.DB) error

func New(t *testing.T, random []byte) *Sim {
	clock := newClock(t, random)
	backend := newBackend(clock)
	return &Sim{
		clock:   clock,
		backend: backend,
		t:       t,
		funcs:   make(map[string]TestGoroutine),
	}
}

type Sim struct {
	clock   *clock
	backend *simBackend
	t       *testing.T
	funcs   map[string]TestGoroutine
	eg      errgroup.Group
}

func (s *Sim) Wait() error {
	return s.eg.Wait()
}

func (s *Sim) Verify(keys []glassdb.FQKey) error {
	// TODO
	// Run the same operations again in commit order and verify that the result
	// is exactly the same as the observed one.
	return nil
}

func (s *Sim) DBInstance() *glassdb.DB {
	opts := glassdb.DefaultOptions()
	opts.Clock = s.clock
	db, _ := glassdb.OpenWith(context.Background(), "sim", s.backend, opts)
	s.t.Cleanup(func() {
		db.Close(context.Background())
	})
	return db
}

func (s *Sim) Run(ctx context.Context, name string, db *glassdb.DB, fn TestGoroutine) {
	if !validNameRe.MatchString(name) {
		s.t.Fatalf("invalid name %q", name)
	}
	s.funcs[name] = fn
	// Make tx ids deterministic by passing it through the context.
	ctx = trans.CtxWithTxID(ctx, data.TxID(name))
	s.eg.Go(func() error {
		// Give the scheduler time to reorder this.
		s.clock.Sleep(time.Millisecond)
		return fn(ctx, db)
	})
}

func newBackend(c *clock) *simBackend {
	// Actual latency doesn't matter, as we are using a random sleep.
	latency := middleware.Latency{Mean: time.Millisecond}
	b := middleware.NewDelayBackend(memory.New(), c, middleware.DelayOptions{
		MetaRead:       latency,
		MetaWrite:      latency,
		ObjRead:        latency,
		ObjWrite:       latency,
		List:           latency,
		SameObjWritePs: 100000, // Disable throttling.
	})
	return &simBackend{
		Backend:     b,
		committedTx: stringset.New(),
	}
}

type simBackend struct {
	backend.Backend
	txOrder     []string
	committedTx stringset.Set
	m           sync.Mutex
}

func (b *simBackend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	m, err := b.Backend.SetTagsIf(ctx, path, expected, t)
	if err != nil {
		return m, err
	}
	b.checkCommittedTx(path, t)
	return m, err
}

func (b *simBackend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	m, err := b.Backend.Write(ctx, path, value, t)
	if err != nil {
		return m, err
	}
	b.checkCommittedTx(path, t)
	return m, err
}

func (b *simBackend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	m, err := b.Backend.WriteIf(ctx, path, value, expected, t)
	if err != nil {
		return m, err
	}
	b.checkCommittedTx(path, t)
	return m, err
}

func (b *simBackend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	m, err := b.Backend.WriteIfNotExists(ctx, path, value, t)
	if err != nil {
		return m, err
	}
	b.checkCommittedTx(path, t)
	return m, err
}

func (b *simBackend) CommitOrder() []data.TxID {
	b.m.Lock()
	defer b.m.Unlock()

	var res []data.TxID
	for _, str := range b.txOrder {
		res = append(res, data.TxID(str))
	}
	return res
}

func (b *simBackend) checkCommittedTx(path string, t backend.Tags) {
	pi, err := paths.Parse(path)
	if err != nil {
		return
	}
	ti, err := storage.TagsLockInfo(t)
	if err != nil {
		return
	}

	b.m.Lock()
	defer b.m.Unlock()

	if b.committedTx.Has(string(ti.LastWriter)) {
		// We've already seen this transaction committed.
		return
	}

	if pi.Type == paths.TransactionType {
		// TODO: Use some utility function in TLogger instead.
		if st, ok := t["commit-status"]; !ok || st != "committed" {
			return
		}
		tx, err := paths.ToTransaction(pi.Suffix)
		if err != nil {
			return
		}
		b.committedTx.Add(string(tx))
		b.txOrder = append(b.txOrder, string(tx))
		return
	}
	if pi.Type != paths.KeyType {
		return
	}
	b.committedTx.Add(string(ti.LastWriter))
	b.txOrder = append(b.txOrder, string(ti.LastWriter))
}

func newClock(t *testing.T, random []byte) *clock {
	sc := testkit.NewSimulatedClock(time.Millisecond, 50*time.Microsecond)
	t.Cleanup(sc.Close)
	return &clock{
		inner: sc,
		rnd:   randomStream{source: random},
	}
}

// clock is a simulated deterministic random clock.
type clock struct {
	inner clockwork.Clock
	rnd   randomStream
}

func (c *clock) After(time.Duration) <-chan time.Time {
	return c.inner.After(c.rndDuration())
}

func (c *clock) Sleep(time.Duration) {
	c.inner.Sleep(c.rndDuration())
}

func (c *clock) Now() time.Time {
	return c.inner.Now()
}

func (c *clock) Since(t time.Time) time.Duration {
	return c.inner.Since(t)
}

func (c *clock) NewTicker(time.Duration) clockwork.Ticker {
	return c.inner.NewTicker(c.rndDuration())
}

func (c *clock) NewTimer(time.Duration) clockwork.Timer {
	return c.inner.NewTimer(c.rndDuration())
}

func (c *clock) AfterFunc(_ time.Duration, fn func()) clockwork.Timer {
	return c.inner.AfterFunc(c.rndDuration(), fn)
}

func (c *clock) rndDuration() time.Duration {
	return time.Duration(c.rnd.RandByte()) * time.Millisecond
}

type randomStream struct {
	source []byte
	curr   int64
}

func (r *randomStream) RandByte() byte {
	next := atomic.AddInt64(&r.curr, 1)
	return r.source[int(next)%len(r.source)]
}
