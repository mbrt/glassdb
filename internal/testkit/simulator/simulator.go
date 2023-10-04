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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		clock:      clock,
		backend:    backend,
		t:          t,
		funcs:      make(map[string]TestGoroutine),
		readValues: make(map[string][]readValue),
	}
}

type Sim struct {
	clock      *testkit.SimulatedClock
	backend    *simBackend
	t          *testing.T
	funcs      map[string]TestGoroutine
	initFuncs  []TestGoroutine
	readValues map[string][]readValue
	onVerify   bool
	eg         errgroup.Group
}

func (s *Sim) Wait() error {
	return s.eg.Wait()
}

func (s *Sim) Verify(ctx context.Context, keys []CollectionKey) error {
	s.onVerify = true

	goldenDB := newTestDB(s.t, memory.New())
	// Reuse the same backend of the tests, but use the inner backend.
	// This way we avoid inserting new transactions in the sim backend.
	testDB := newTestDB(s.t, s.backend.Backend)

	// Run the same operations again in commit order and verify that the result
	// is exactly the same as the observed one.
	for i, fn := range s.initFuncs {
		if err := fn(ctx, goldenDB); err != nil {
			return fmt.Errorf("running #%d init func: %v", i, err)
		}
	}

	order := s.backend.CommitOrder()
	for i, tx := range order {
		tf, ok := s.funcs[string(tx)]
		if !ok {
			return fmt.Errorf("cannot find tx %q", string(tx))
		}
		err := tf(ctx, goldenDB)
		if err != nil {
			return fmt.Errorf("executing func #%d (%s): %v", i, string(tx), err)
		}
		// Check the read values if any were given.
		rvs, ok := s.readValues[string(tx)]
		if !ok {
			continue
		}
		for _, rv := range rvs {
			got, err := goldenDB.Collection(rv.Key.Collection).ReadStrong(ctx, rv.Key.Key)
			if err != nil {
				return fmt.Errorf("read for func #%d (%s) failed: %v", i, string(tx), err)
			}
			if !bytes.Equal(got, rv.Value) {
				return fmt.Errorf("different read for func #%d (%s), got: %q, want: %q",
					i, string(tx), string(got), string(rv.Value))
			}
		}
	}

	for _, k := range keys {
		refC := goldenDB.Collection(k.Collection)
		refB, refErr := refC.ReadStrong(ctx, k.Key)
		testC := testDB.Collection(k.Collection)
		testB, testErr := testC.ReadStrong(ctx, k.Key)
		if (refErr == nil) != (testErr == nil) {
			return fmt.Errorf("different errors for key %q: test (%v), ref (%v)",
				string(k.Key), testErr, refErr)
		}
		if !bytes.Equal(refB, testB) {
			return fmt.Errorf("different read for key %q: test (%s), ref (%s)",
				string(k.Key), string(testB), string(refB))
		}
	}

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

// Init runs before the start of the tests and is not fuzzed.
func (s *Sim) Init(ctx context.Context, fn TestGoroutine) error {
	// Use the inner backend to avoid keeping track of these transactions.
	s.initFuncs = append(s.initFuncs, fn)
	db := newTestDB(s.t, s.backend.Backend)
	return fn(ctx, db)
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

// NotifyReadValue adds the read to the list of reads to check during `Verify`.
//
// Only call this after the transaction successfully committed; never before or
// when a transaction failed.
func (s *Sim) NotifyReadValue(ctx context.Context, k CollectionKey, val []byte) {
	if s.onVerify {
		// We don't use this during verify. The check is done afterwards.
		return
	}

	id := trans.TxIDFromCtx(ctx)
	if id == nil {
		s.t.Fatal("NotifyReadValue from unknown context")
	}
	// If this is a readonly transaction, make sure we notify that it's already
	// completed, as the backend doesn't know about it.
	s.backend.NotifyCommitted(id)
	rvs := s.readValues[string(id)]
	s.readValues[string(id)] = append(rvs, readValue{Key: k, Value: val})
}

type CollectionKey struct {
	Collection []byte
	Key        []byte
}

type readValue struct {
	Key   CollectionKey
	Value []byte
}

func newTestDB(t *testing.T, b backend.Backend) *glassdb.DB {
	opts := glassdb.DefaultOptions()
	opts.Clock = testkit.NewAcceleratedClock(1000)
	db, err := glassdb.OpenWith(context.Background(), "sim", b, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func newBackend(c *testkit.SimulatedClock) *simBackend {
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

func (b *simBackend) NotifyCommitted(txid data.TxID) {
	b.m.Lock()
	defer b.m.Unlock()

	stid := string(txid)
	if !b.committedTx.Has(stid) {
		b.committedTx.Add(stid)
		b.txOrder = append(b.txOrder, stid)
	}
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
		tx, err := paths.ToTransaction(string(pi.Type) + "/" + pi.Suffix)
		if err != nil {
			return
		}
		b.committedTx.Add(string(tx))
		b.txOrder = append(b.txOrder, string(tx))
		return
	}
	if pi.Type != paths.KeyType || len(ti.LastWriter) == 0 {
		return
	}
	b.committedTx.Add(string(ti.LastWriter))
	b.txOrder = append(b.txOrder, string(ti.LastWriter))
}

func newClock(t *testing.T, random []byte) *testkit.SimulatedClock {
	sc := testkit.NewSimulatedClockWithQueue(time.Millisecond, 50*time.Microsecond, &randomOrderQueue{
		ticks: 1,
		rnd:   randomStream{source: random},
	})
	t.Cleanup(sc.Close)
	return sc
}

type randomOrderQueue struct {
	q     []testkit.WaitItem
	ticks int64
	rnd   randomStream
}

func (q *randomOrderQueue) Push(i testkit.WaitItem) {
	i.Ticks = 0 // Reset ticks. We updated them in Top() and Pop().
	q.q = append(q.q, i)
	if len(q.q) > 1 {
		// Swap the new element with a random one in the queue.
		victim := int(q.rnd.RandByte()) % len(q.q)
		q.q[victim], q.q[len(q.q)-1] = q.q[len(q.q)-1], q.q[victim]
	}
}

func (q *randomOrderQueue) Top() (testkit.WaitItem, bool) {
	if len(q.q) == 0 {
		return testkit.WaitItem{}, false
	}
	return testkit.WaitItem{
		Ticks: q.ticks,
		Ch:    q.q[0].Ch,
		Fn:    q.q[0].Fn,
	}, true
}

func (q *randomOrderQueue) Pop() (testkit.WaitItem, bool) {
	top, ok := q.Top()
	if !ok {
		return top, ok
	}
	q.ticks++
	q.q[0] = testkit.WaitItem{} // Avoid memory leak.
	q.q = q.q[1:]
	return top, ok
}

func (q *randomOrderQueue) ToSlice() []testkit.WaitItem {
	return q.q
}

type randomStream struct {
	source []byte
	curr   int64
}

func (r *randomStream) RandByte() byte {
	if len(r.source) == 0 {
		return 0
	}
	next := atomic.AddInt64(&r.curr, 1)
	return r.source[int(next)%len(r.source)]
}
