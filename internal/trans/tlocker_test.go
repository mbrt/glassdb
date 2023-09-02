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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sourcegraph/conc"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/testkit"
)

type tlTestCtx struct {
	Clock   clockwork.Clock
	Local   storage.Local
	Global  storage.Global
	Backend backend.Backend
	TLogger storage.TLogger
	Monitor *Monitor
}

func initTLTest(t *testing.T) (*Locker, tlTestCtx) {
	t.Helper()
	clock := testkit.NewAcceleratedClock(1000)
	b := memory.New()
	return newTestLocker(t, clock, b)
}

func newTestLocker(
	t *testing.T,
	c clockwork.Clock,
	b backend.Backend,
) (*Locker, tlTestCtx) {
	t.Helper()
	l := storage.NewLocal(cache.New(1024), c)
	g := storage.NewGlobal(b, l, c)
	tl := storage.NewTLogger(c, g, l, "test")
	bg := concurr.NewBackground()
	t.Cleanup(bg.Close)
	mon := NewMonitor(c, l, tl, bg)
	locker := NewLocker(l, g, tl, c, mon)

	return locker, tlTestCtx{
		Clock:   c,
		Local:   l,
		Backend: b,
		Global:  g,
		TLogger: tl,
		Monitor: mon,
	}
}

func TestLockCreate(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	// Lock unlock without commit.
	for i := 0; i < 3; i++ {
		err := locker.LockCreate(ctx, key, tx)
		assert.NoError(t, err)
		assertLockInfo(t, tctx.Global, key, storage.LockInfo{
			Type:     storage.LockTypeCreate,
			LockedBy: []data.TxID{tx},
		})

		err = locker.Unlock(ctx, key, tx)
		assert.NoError(t, err)
		_, err = tctx.Global.GetMetadata(ctx, key)
		assert.ErrorIs(t, err, backend.ErrNotFound)
	}

	// Lock, commit the transaction and unlock.
	err := locker.LockCreate(ctx, key, tx)
	assert.NoError(t, err)
	value := []byte("val")
	err = tctx.Monitor.CommitTx(ctx, storage.TxLog{
		ID: tx,
		Writes: []storage.TxWrite{
			{
				Path:  key,
				Value: value,
			},
		},
	})
	assert.NoError(t, err)

	err = locker.Unlock(ctx, key, tx)
	assert.NoError(t, err)
	// Make sure the unlock wrote the correct value.
	gr, err := tctx.Global.Read(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, string(value), string(gr.Value))
}

func TestLockCreateFail(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// LockCreate should fail when the item is already present.
	err = locker.LockCreate(ctx, key, tx)
	assert.ErrorIs(t, err, backend.ErrPrecondition)
}

func TestUnlockAfterCreateTimeout(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	// Locking will not succeed with an expired context.
	err := locker.LockCreate(canceledCtx(ctx), key, tx)
	assert.ErrorIs(t, err, context.Canceled)

	_, err = tctx.Global.GetMetadata(ctx, key)
	assert.ErrorIs(t, err, backend.ErrNotFound)

	// Unlocking should be no problem.
	err = locker.Unlock(ctx, key, tx)
	assert.NoError(t, err)
}

func TestLockReadWrite(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	err = locker.LockRead(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx},
	})

	err = locker.Unlock(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type: storage.LockTypeNone,
	})

	err = locker.LockWrite(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx},
	})

	err = locker.Unlock(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type: storage.LockTypeNone,
	})
}

func TestLockMultipleR(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx1 := data.NewTId()
	tx2 := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx1)
	tctx.Monitor.BeginTx(ctx, tx2)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// Two transactions lock in read.
	err = locker.LockRead(ctx, key, tx1)
	assert.NoError(t, err)
	err = locker.LockRead(ctx, key, tx2)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx1, tx2},
	})

	// Lock again with the same tx.
	err = locker.LockRead(ctx, key, tx1)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx1, tx2},
	})

	// Unlock the first.
	err = locker.Unlock(ctx, key, tx1)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx2},
	})

	// Unlock the second.
	err = locker.Unlock(ctx, key, tx2)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type: storage.LockTypeNone,
	})
}

func TestLockReadAfterDelete(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	txw := data.TxID("txw")
	tctx.Monitor.BeginTx(ctx, txw)

	// Lock, commit the transaction (deleting).
	err = locker.LockWrite(ctx, key, txw)
	assert.NoError(t, err)
	err = tctx.Monitor.CommitTx(ctx, storage.TxLog{
		ID: txw,
		Writes: []storage.TxWrite{
			{Path: key, Deleted: true},
		},
	})
	assert.NoError(t, err)

	txr := data.TxID("txr")
	tctx.Monitor.BeginTx(ctx, txw)
	// LockRead should fail.
	err = locker.LockRead(ctx, key, txr)
	assert.ErrorIs(t, err, backend.ErrNotFound)
}

func TestWaitForTx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	txr := data.TxID("txr")
	txw := data.TxID("txw")
	tctx.Monitor.BeginTx(ctx, txr)
	tctx.Monitor.BeginTx(ctx, txw)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// Lock in read first.
	err = locker.LockRead(ctx, key, txr)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{txr},
	})

	// Schedule the read unlock to just after the write starts waiting.
	wg := conc.WaitGroup{}
	wg.Go(func() {
		time.Sleep(time.Millisecond)
		err := locker.Unlock(ctx, key, txr)
		assert.NoError(t, err)
	})

	// Lock in write.
	err = locker.LockWrite(ctx, key, txw)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{txw},
	})

	// Schedule abort of the write Tx for after a new read starts waiting.
	wg.Go(func() {
		time.Sleep(time.Millisecond)
		err := tctx.Monitor.AbortTx(ctx, txw)
		assert.NoError(t, err)
	})

	// Lock in read.
	// TODO: Fix bug. Without this sleep, this can happen before the async
	//       unlock for txr above finishes updating the local cache, causing
	//       this to not run at all.
	time.Sleep(time.Millisecond)
	err = locker.LockRead(ctx, key, txr)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{txr},
	})
	wg.Wait()
}

func TestQueueUp(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	txw := data.TxID("txw")
	tctx.Monitor.BeginTx(ctx, txw)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// Lock in write.
	err = locker.LockWrite(ctx, key, txw)
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{txw},
	})

	// Try to lock in read in parallel.
	gr := &errgroup.Group{}
	var txrs []data.TxID
	for i := 0; i < 3; i++ {
		tx := data.TxID(fmt.Sprintf("txr%d", i))
		txrs = append(txrs, tx)

		gr.Go(func() error {
			tctx.Monitor.BeginTx(ctx, tx)
			return locker.LockRead(ctx, key, tx)
		})
	}

	// Wait for a bit and schedule a write lock as well.
	txw0 := data.TxID("txw0")
	gw := &errgroup.Group{}
	gw.Go(func() error {
		time.Sleep(time.Millisecond)
		tctx.Monitor.BeginTx(ctx, txw0)
		return locker.LockWrite(ctx, key, txw0)
	})

	// Unlock the original write after we know that the following read locks are waiting.
	time.Sleep(time.Millisecond)
	err = locker.Unlock(ctx, key, txw)
	assert.NoError(t, err)

	// Wait for the lock reads to complete.
	err = gr.Wait()
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: txrs,
	})

	// Now unlock all of them.
	for _, tx := range txrs {
		err := tctx.Monitor.AbortTx(ctx, tx)
		assert.NoError(t, err)
	}
	// Wait for the write lock.
	err = gw.Wait()
	assert.NoError(t, err)

	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{txw0},
	})
}

func TestLockUpgrade(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	err = locker.LockRead(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx},
	})

	err = locker.LockWrite(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx},
	})
}

func TestLockUpgradeWait(t *testing.T) {
	ctx := context.Background()
	locker, tctx := initTLTest(t)
	key := paths.FromKey("example", []byte("key"))
	tx := data.NewTId()
	tctx.Monitor.BeginTx(ctx, tx)

	txr := data.NewTId()
	tctx.Monitor.BeginTx(ctx, txr)

	// Initialize key.
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	err = locker.LockRead(ctx, key, tx)
	assert.NoError(t, err)
	err = locker.LockRead(ctx, key, txr)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx, txr},
	})

	// Wait before lock write starts locking.
	wg := conc.WaitGroup{}
	wg.Go(func() {
		time.Sleep(5 * time.Millisecond)
		err := locker.Unlock(ctx, key, txr)
		assert.NoError(t, err)
	})

	err = locker.LockWrite(ctx, key, tx)
	assert.NoError(t, err)
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx},
	})
	wg.Wait()
}

func TestLockReadRemote(t *testing.T) {
	ctx := context.Background()
	locker1, tctx1 := initTLTest(t)
	locker2, tctx2 := newTestLocker(t, tctx1.Clock, tctx1.Backend)

	// Initialize key.
	key := paths.FromKey("example", []byte("key"))
	_, err := tctx1.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// Fetch metadata from local1.
	// This makes sure when LockRead runs, it will see stale metadata.
	_, err = tctx1.Global.GetMetadata(ctx, key)
	assert.NoError(t, err)

	// Lock the key from locker2.
	tx2 := data.NewTId()
	tctx2.Monitor.BeginTx(ctx, tx2)
	err = locker2.LockRead(ctx, key, tx2)
	assert.NoError(t, err)
	assertLockInfo(t, tctx2.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx2},
	})

	// Lock it from locker1 as well.
	tx1 := data.NewTId()
	tctx1.Monitor.BeginTx(ctx, tx1)
	err = locker1.LockRead(ctx, key, tx1)
	assert.NoError(t, err)
	assertLockInfo(t, tctx1.Global, key, storage.LockInfo{
		Type:     storage.LockTypeRead,
		LockedBy: []data.TxID{tx2, tx1},
	})

	// Make sure there was a retry here (metadata was stale).
	stats := locker1.StatsAndReset()
	assert.Equal(t, 1, stats.Retries)
}

func TestWaitRemote(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	locker1, tctx1 := initTLTest(t)
	locker2, tctx2 := newTestLocker(t, tctx1.Clock, tctx1.Backend)

	// Initialize key.
	key := paths.FromKey("example", []byte("key"))
	_, err := tctx1.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	// Lock in write from locker2.
	tx2 := data.NewTId()
	tctx2.Monitor.BeginTx(ctx, tx2)
	err = locker2.LockWrite(ctx, key, tx2)
	assert.NoError(t, err)
	assertLockInfo(t, tctx2.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx2},
	})

	// Wait before locker1 starts locking.
	wg := conc.WaitGroup{}
	wg.Go(func() {
		time.Sleep(5 * time.Millisecond)
		err := locker2.Unlock(ctx, key, tx2)
		assert.NoError(t, err)
	})

	// Lock in write from locker1.
	tx1 := data.NewTId()
	tctx1.Monitor.BeginTx(ctx, tx1)
	err = locker1.LockWrite(ctx, key, tx1)
	assert.NoError(t, err)
	assertLockInfo(t, tctx1.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx1},
	})

	// Wait before locker2 starts locking again.
	wg.Go(func() {
		time.Sleep(5 * time.Millisecond)
		// This time commit the transaction.
		err := tctx2.Monitor.CommitTx(ctx, storage.TxLog{
			ID: tx2,
			Writes: []storage.TxWrite{
				{Path: key, Value: []byte("foo")},
			},
		})
		if ctx.Err() == nil {
			assert.NoError(t, err)
		}
	})

	// Lock in write from locker2.
	err = locker1.LockWrite(ctx, key, tx1)
	assert.NoError(t, err)
	assertLockInfo(t, tctx1.Global, key, storage.LockInfo{
		Type:     storage.LockTypeWrite,
		LockedBy: []data.TxID{tx1},
	})

	wg.Wait()
}

func TestConcurrentLockUnlock(t *testing.T) {
	ctx := context.Background()
	clock := testkit.NewAcceleratedClock(1000)
	innerB := memory.New()
	b := testkit.NewStallBackend(innerB)
	defer b.WaitForStalled()
	locker, tctx := newTestLocker(t, clock, b)

	// Initialize key.
	key := paths.FromKey("example", []byte("key"))
	_, err := tctx.Global.Write(ctx, key, []byte("x"), backend.Tags{})
	assert.NoError(t, err)

	tx := data.TxID("tx")
	tctx.Monitor.BeginTx(ctx, tx)

	// Lock in write but don't let it finish.
	b.StallWrites()
	err = locker.LockWrite(ctx, key, tx)
	assert.Error(t, err)

	// Stop stalling writes, but do not release the previously stalled.
	b.StopStalling()

	// Unlock the same.
	err = locker.Unlock(ctx, key, tx)
	assert.NoError(t, err)

	// Allow the first lock to complete.
	b.ReleaseStalled()
	// Wait for all writes to complete.
	b.WaitForStalled()

	// The lock should not succeed even if the backend reordered the calls.
	assertLockInfo(t, tctx.Global, key, storage.LockInfo{
		Type: storage.LockTypeNone,
	})
}

func canceledCtx(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	cancel()
	return ctx
}

func assertLockInfo(t *testing.T, g storage.Global, key string, expected storage.LockInfo) {
	t.Helper()
	meta, err := g.GetMetadata(context.Background(), key)
	assert.NoError(t, err)
	info, err := storage.TagsLockInfo(meta.Tags)
	assert.NoError(t, err)
	sortTxIDs(info.LockedBy)
	sortTxIDs(expected.LockedBy)
	assert.Equal(t, expected, info)
}

func sortTxIDs(s []data.TxID) {
	sort.Slice(s, func(i, j int) bool {
		return s[i].String() < s[j].String()
	})
}
