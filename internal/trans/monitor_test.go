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

package trans

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/testkit"
)

type monTestCtx struct {
	Clock   clockwork.Clock
	Local   storage.Local
	Global  storage.Global
	Backend backend.Backend
	TLogger storage.TLogger
}

func initMonTest(t *testing.T) (*Monitor, monTestCtx) {
	t.Helper()
	clock := testkit.NewAcceleratedClock(1000)
	b := memory.New()
	return newTestMonitor(t, clock, b)
}

func newTestMonitor(
	t *testing.T,
	c clockwork.Clock,
	b backend.Backend,
) (*Monitor, monTestCtx) {
	t.Helper()
	l := storage.NewLocal(cache.New(1024), c)
	g := storage.NewGlobal(b, l, c)
	tl := storage.NewTLogger(c, g, l, "test")
	bg := concurr.NewBackground()
	t.Cleanup(bg.Close)
	mon := NewMonitor(c, l, tl, bg)

	return mon, monTestCtx{
		Clock:   c,
		Local:   l,
		Backend: b,
		Global:  g,
		TLogger: tl,
	}
}

func TestStatus(t *testing.T) {
	ctx := context.Background()
	mon1, tctx1 := initMonTest(t)
	mon2, _ := newTestMonitor(t, tctx1.Clock, tctx1.Backend)

	key := paths.FromKey("example", []byte("key1"))
	tx := data.TxID("tx1")
	mon1.BeginTx(ctx, tx)

	// Local pending.
	status, err := mon1.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusPending, status)
	// Remote pending.
	status, err = mon2.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusPending, status)

	// Abort. Local status.
	err = mon1.AbortTx(ctx, tx)
	assert.NoError(t, err)
	status, err = mon1.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusAborted, status)
	// Remote status.
	status, err = mon2.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusAborted, status)

	// New transaction.
	tx = data.TxID("tx2")
	mon1.BeginTx(ctx, tx)
	// Commit.
	err = mon1.CommitTx(ctx, storage.TxLog{
		ID:     tx,
		Status: storage.TxCommitStatusOK,
		Locks:  []storage.PathLock{{Path: key, Type: storage.LockTypeWrite}},
	})
	assert.NoError(t, err)
	// Local status.
	status, err = mon1.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusOK, status)
	// Remote status.
	status, err = mon2.TxStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusOK, status)
}

func TestCommittedValue(t *testing.T) {
	ctx := context.Background()
	mon1, tctx1 := initMonTest(t)
	mon2, _ := newTestMonitor(t, tctx1.Clock, tctx1.Backend)
	key := paths.FromKey("example", []byte("key"))

	// Initialize key.
	_, err := tctx1.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	tx := data.TxID("tx1")
	mon1.BeginTx(ctx, tx)

	// Local pending.
	cs, err := mon1.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	assert.Equal(t, KeyCommitStatus{Status: storage.TxCommitStatusPending}, cs)
	// Remote pending.
	cs, err = mon2.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	assert.Equal(t, KeyCommitStatus{Status: storage.TxCommitStatusPending}, cs)

	// Abort. Local value.
	err = mon1.AbortTx(ctx, tx)
	assert.NoError(t, err)
	cs, err = mon1.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	assert.Equal(t, KeyCommitStatus{Status: storage.TxCommitStatusAborted}, cs)
	// Remote value.
	cs, err = mon2.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	assert.Equal(t, KeyCommitStatus{Status: storage.TxCommitStatusAborted}, cs)

	// New transaction.
	tx = data.TxID("tx2")
	mon1.BeginTx(ctx, tx)
	// Commit.
	err = mon1.CommitTx(ctx, storage.TxLog{
		ID:     tx,
		Status: storage.TxCommitStatusOK,
		Writes: []storage.TxWrite{
			{Path: key, Value: []byte("val1")},
		},
		Locks: []storage.PathLock{
			{Path: key, Type: storage.LockTypeWrite},
		},
	})
	assert.NoError(t, err)
	// Local value.
	cs, err = mon1.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	expected := KeyCommitStatus{
		Status: storage.TxCommitStatusOK,
		Value:  storage.TValue{Value: []byte("val1")},
	}
	assert.Equal(t, expected, cs)
	// Remote value.
	cs, err = mon2.CommittedValue(ctx, key, tx)
	assert.NoError(t, err)
	assert.Equal(t, expected, cs)

	// Uncommitted value. Local.
	key2 := paths.FromKey("example", []byte("key2"))
	cs, err = mon1.CommittedValue(ctx, key2, tx)
	assert.NoError(t, err)
	expected = KeyCommitStatus{
		Status: storage.TxCommitStatusOK,
		Value:  storage.TValue{NotWritten: true},
	}
	assert.Equal(t, expected, cs)
	// Remote.
	cs, err = mon2.CommittedValue(ctx, key2, tx)
	assert.NoError(t, err)
	assert.Equal(t, expected, cs)
}

func TestMonWaitForLocalTx(t *testing.T) {
	ctx := context.Background()
	mon1, _ := initMonTest(t)

	tx := data.TxID("tx1")
	mon1.BeginTx(ctx, tx)

	// Wait without a result.
	ch1 := mon1.WaitForTx(ctx, tx)
	// The second wait should depend on the first one.
	ch2 := mon1.WaitForTx(ctx, tx)
	assertNoWaitTxResult(t, ch1)
	assertNoWaitTxResult(t, ch2)

	// Abort and fetch the result.
	err := mon1.AbortTx(ctx, tx)
	assert.NoError(t, err)
	r := <-ch1
	assert.Equal(t, WaitTxResult{Status: storage.TxCommitStatusAborted}, r)
	r = <-ch2
	assert.Equal(t, WaitTxResult{Status: storage.TxCommitStatusAborted}, r)
}

func TestMonWaitForRemoteTx(t *testing.T) {
	ctx := context.Background()
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	mon1, tctx1 := initMonTest(t)
	mon2, _ := newTestMonitor(t, tctx1.Clock, tctx1.Backend)

	tx := data.TxID("tx1")
	mon1.BeginTx(ctx, tx)

	// Wait without a result.
	ch1 := mon2.WaitForTx(ctx1, tx)
	// The second wait should depend on the first one.
	ch2 := mon2.WaitForTx(ctx, tx)
	// Run a third wait as well.
	ch3 := mon2.WaitForTx(ctx, tx)

	assertNoWaitTxResult(t, ch1)
	assertNoWaitTxResult(t, ch2)
	assertNoWaitTxResult(t, ch3)

	// Cancel the first waiter.
	cancel1()

	// Abort and fetch the result.
	err := mon1.AbortTx(ctx, tx)
	assert.NoError(t, err)

	r := <-ch2
	assert.Equal(t, WaitTxResult{Status: storage.TxCommitStatusAborted}, r)
	r = <-ch3
	assert.Equal(t, WaitTxResult{Status: storage.TxCommitStatusAborted}, r)
}

func TestLongPendingTx(t *testing.T) {
	if testkit.RaceEnabled {
		t.Skip("race detector causes too much clock delay")
	}

	ctx := context.Background()
	mon, tctx := initMonTest(t)

	tx := data.TxID("tx1")
	mon.BeginTx(ctx, tx)
	t.Cleanup(func() {
		_ = mon.AbortTx(ctx, tx)
	})
	mon.StartRefreshTx(ctx, tx)

	start := tctx.Clock.Now()
	// Wait until we know the transaction should be considered pending.
	tctx.Clock.Sleep(pendingTxTimeout)
	now := tctx.Clock.Now()

	for elapsed := now.Sub(start); elapsed < pendingTxTimeout*15; elapsed = now.Sub(start) {
		tctx.Clock.Sleep(time.Second)
		now = tctx.Clock.Now()
		tl, err := tctx.TLogger.Get(ctx, tx)
		assert.NoError(t, err)

		assert.Equal(t, tl.Status, storage.TxCommitStatusPending)
		assert.Less(t, now.Sub(tl.Timestamp), pendingTxTimeout*2) // Give some headroom on slow machines :/
		t.Logf("Timestamp: %v, updated %v ago", tl.Timestamp, now.Sub(tl.Timestamp))
	}
}

func TestRefreshCtxShouldNotCancel(t *testing.T) {
	ctx := context.Background()
	mon, tctx := initMonTest(t)

	tx := data.TxID("tx1")
	mon.BeginTx(ctx, tx)
	t.Cleanup(func() {
		_ = mon.AbortTx(ctx, tx)
	})

	// Refresh with a context that is canceled.
	// The refresh should continue anyway.
	ctxr, cancel := context.WithCancel(ctx)
	mon.StartRefreshTx(ctxr, tx)
	cancel()

	// Wait until we know the transaction should be considered pending.
	tctx.Clock.Sleep(pendingTxTimeout)

	// Verify that this is actually pending.
	tl, err := tctx.TLogger.Get(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, tl.Status, storage.TxCommitStatusPending)
}

func assertNoWaitTxResult(t *testing.T, ch <-chan WaitTxResult) {
	select {
	case r := <-ch:
		t.Errorf("Unexpected WaitForTx result: %v", r)
	default:
	}
}
