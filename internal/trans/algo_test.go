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
	"log/slog"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/testkit"
)

const (
	testCollName    = "testp"
	clockMultiplier = 1000
)

var collInfoContents = []byte("__foo__")

type nilHandler struct{}

func (nilHandler) Enabled(context.Context, slog.Level) bool {
	return false
}
func (nilHandler) Handle(context.Context, slog.Record) error  { return nil }
func (h nilHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h nilHandler) WithGroup(name string) slog.Handler       { return h }

type testContext struct {
	backend backend.Backend
	clock   clockwork.Clock
	global  storage.Global
	local   storage.Local
	tlogger storage.TLogger
	tmon    *Monitor
	locker  *Locker
}

func testAlgoContext(t *testing.T) (Algo, testContext) {
	t.Helper()
	return newAlgoFromBackend(t, memory.New())
}

func newAlgoFromBackend(t *testing.T, b backend.Backend) (Algo, testContext) {
	t.Helper()

	// It's important to close the context at the end to stop
	// any background tasks.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	clock := testkit.NewAcceleratedClock(clockMultiplier)
	cache := cache.New(1024)
	local := storage.NewLocal(cache, clock)
	global := storage.NewGlobal(b, local, clock)
	tlogger := storage.NewTLogger(clock, global, local, testCollName)
	background := concurr.NewBackground()
	t.Cleanup(background.Close)
	tmon := NewMonitor(clock, local, tlogger, background)
	locker := NewLocker(local, global, tlogger, clock, tmon)
	// Disable algo background tasks, as they make tests flaky.
	// They should be tested separately.
	background = (*concurr.Background)(nil)

	// Create test collection.
	_, err := global.Write(ctx, paths.CollectionInfo(testCollName), collInfoContents, nil)
	require.NoError(t, err)

	tm := NewAlgo(clock, global, local, locker, tmon, background, slog.New(nilHandler{}))
	return tm, testContext{
		backend: b,
		clock:   clock,
		global:  global,
		local:   local,
		tlogger: tlogger,
		tmon:    tmon,
		locker:  locker,
	}
}

func TestWriteNew(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	h := tm.Begin(ctx, Data{
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  val,
			},
		},
	})
	err := tm.Commit(ctx, h)
	assert.NoError(t, err)
	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// Check the end results.
	_, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)
	// The transaction is committed.
	status, err := tctx.tlogger.CommitStatus(ctx, h.id)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusOK, status.Status)
	// Check the log.
	txlog, err := tctx.tlogger.Get(ctx, h.id)
	assert.NoError(t, err)
	assert.NotEqual(t, time.Time{}, txlog.Timestamp)
	txlog.Timestamp = time.Time{} // Clear timestamp, which we don't know.
	assert.Equal(t, storage.TxLog{
		ID:     h.id,
		Status: storage.TxCommitStatusOK,
		Writes: []storage.TxWrite{
			{
				Path:  keyp,
				Value: val,
			},
		},
		Locks: []storage.PathLock{
			{
				Path: paths.CollectionInfo("testp"),
				Type: storage.LockTypeWrite,
			},
			{
				Path: keyp,
				Type: storage.LockTypeCreate,
			},
		},
	}, txlog)
}

func TestReadNotFoundWrite(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	var txID data.TxID

	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{
			{
				Path:  keyp,
				Found: false,
			},
		},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  val,
			},
		},
	})
	err := tm.Commit(ctx, h)
	assert.NoError(t, err)
	txID = h.id
	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// Check the end results.
	_, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)
	// The transaction is committed.
	status, err := tctx.tlogger.CommitStatus(ctx, txID)
	assert.NoError(t, err)
	assert.Equal(t, storage.TxCommitStatusOK, status.Status)
}

func TestSingleReadWrite(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	// Initialize the variable first.
	h := commitWrites(t, tm, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("init"),
		},
	})
	flushWrites(t, tm, h)

	// Read the value.
	gr, err := tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h = tm.Begin(ctx, Data{
		Reads: []ReadAccess{
			{
				Path:    keyp,
				Version: ReadVersion{Version: gr.Version},
				Found:   true,
			},
		},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  val,
			},
		},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// Check the end result.
	gr, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)
	assert.Equal(t, gr.Value, val)
}

func TestReadWriteWhileLockCreate(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	// Initialize the variable from another algo.
	tm2, _ := newAlgoFromBackend(t, tctx.backend)
	commitWrites(t, tm2, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("init"),
		},
	})
	// Note that we are not flushing here. Cleanups are disabled, so we'll end
	// up with a committed item but still marked in LockCreate.

	// Read the value.
	gr, err := tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{
			{
				Path:    keyp,
				Version: ReadVersion{Version: gr.Version},
				Found:   true,
			},
		},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  val,
			},
		},
	})
	err = tm.Commit(ctx, h)
	// The read was inconsistent.
	assert.ErrorIs(t, err, ErrRetry)

	// Read again.
	gr, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)

	// Commit again.
	tm.Reset(h, Data{
		Reads: []ReadAccess{
			{
				Path:    keyp,
				Version: ReadVersion{Version: gr.Version},
				Found:   true,
			},
		},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  val,
			},
		},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)

	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// Check the end result locally.
	lr, ok := tctx.local.Read(keyp, storage.MaxStaleness)
	assert.True(t, ok)
	assert.Equal(t, val, lr.Value)
	assert.Equal(t, h.id, lr.Version.Writer)

	// Unlock and check global.
	err = tm.unlockAll(ctx, h)
	assert.NoError(t, err)
	gr, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)
	assert.Equal(t, val, gr.Value)
}

func TestReadonly(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	// Initialize the variable first.
	h := commitWrites(t, tm, []WriteAccess{
		{
			Path: keyp,
			Val:  val,
		},
	})
	flushWrites(t, tm, h)

	// Read the value.
	gr, err := tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h = tm.Begin(ctx, Data{
		Reads: []ReadAccess{
			{
				Path:    keyp,
				Version: ReadVersion{Version: gr.Version},
				Found:   true,
			},
		},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// Check the end result.
	gr, err = tctx.global.Read(ctx, keyp)
	assert.NoError(t, err)
	assert.Equal(t, gr.Value, val)
}

func TestReadonlyInLockCreate(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	// Commit the value from another algo.
	tm2, _ := newAlgoFromBackend(t, tctx.backend)
	commitWrites(t, tm2, []WriteAccess{
		{
			Path: keyp,
			Val:  val,
		},
	})

	// Read the value.
	r, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{r},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
}

func TestReadonlyAfterDelete(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))

	// Commit the value and delete from another algo.
	tm2, _ := newAlgoFromBackend(t, tctx.backend)
	commitWrites(t, tm2, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("v"),
		},
	})
	commitWrites(t, tm2, []WriteAccess{
		{
			Path:   keyp,
			Delete: true,
		},
	})

	// Read the value.
	r, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{r},
	})
	err = tm.Commit(ctx, h)
	// The read was inconsistent.
	assert.ErrorIs(t, err, ErrRetry)

	// Read again.
	r, err = doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Commit again.
	tm.Reset(h, Data{
		Reads: []ReadAccess{r},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
}

func TestReadonlyLocalAfterDelete(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))

	// Commit the value and delete from the same algo.
	commitWrites(t, tm, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("v"),
		},
	})
	commitWrites(t, tm, []WriteAccess{
		{
			Path:   keyp,
			Delete: true,
		},
	})

	// Read the value.
	r, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{r},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
}

func TestReadonlyLocalAfterDeleteFlushed(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))

	// Commit the value and delete from the same algo.
	commitWrites(t, tm, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("v"),
		},
	})
	h := commitWrites(t, tm, []WriteAccess{
		{
			Path:   keyp,
			Delete: true,
		},
	})
	flushWrites(t, tm, h)

	// Read the value.
	r, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Then commit.
	h = tm.Begin(ctx, Data{
		Reads: []ReadAccess{r},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
}

func TestReadonlyFromUncommitted(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keyp := paths.FromKey("testp", []byte("k"))
	val := []byte("v")

	// Initialize the variable first.
	wh := commitWrites(t, tm, []WriteAccess{
		{Path: keyp, Val: []byte("xxx")},
	})
	flushWrites(t, tm, wh)

	// Read the value.
	ra1, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Modify it.
	wh = commitWrites(t, tm, []WriteAccess{
		{Path: keyp, Val: val},
	})
	flushWrites(t, tm, wh)

	// One transition tries to update the value starting from an old read.
	// It should fail and leave the item locked in write.
	h1 := tm.Begin(ctx, Data{
		Reads: []ReadAccess{ra1},
		Writes: []WriteAccess{
			{Path: keyp, Val: []byte("tmpw")},
		},
	})
	err = tm.Commit(ctx, h1)
	assert.ErrorIs(t, err, ErrRetry)

	// The item is locked in write and the transaction hasn't committed yet.
	meta, err := tctx.global.GetMetadata(ctx, keyp)
	assert.NoError(t, err)
	info, err := storage.TagsLockInfo(meta.Tags)
	assert.NoError(t, err)
	assert.Equal(t, storage.LockTypeWrite, info.Type)
	assert.Equal(t, info.LockedBy[0], h1.id)

	// The readonly transition arrives now with an old read.
	// And asks for a retry.
	h2 := tm.Begin(ctx, Data{
		Reads: []ReadAccess{ra1},
	})
	err = tm.Commit(ctx, h2)
	assert.ErrorIs(t, err, ErrRetry)

	// We should know the new value now.
	ra2, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)
	assert.True(t, ra2.Version.IsLocal())
	assert.Equal(t, ra2.Version.LastWriter, wh.id)
}

func TestSerialValidate(t *testing.T) {
	ctx := context.Background()
	tm1, tctx := testAlgoContext(t)
	tm2, tctx2 := newAlgoFromBackend(t, tctx.backend)
	tm3, _ := newAlgoFromBackend(t, tctx.backend)
	keys := []string{
		paths.FromKey("testp", []byte("k0")),
		paths.FromKey("testp", []byte("k1")),
		paths.FromKey("testp", []byte("k2")),
		paths.FromKey("testp", []byte("k3")),
	}

	// Create all keys except k1 (from another algo).
	commitWrites(t, tm3, []WriteAccess{
		{Path: keys[0], Val: []byte("0")},
		{Path: keys[2], Val: []byte("0")},
		{Path: keys[3], Val: []byte("0")},
	})

	// Read all keys.
	r01, err := doReads(ctx, tctx, keys[:2])
	assert.NoError(t, err)
	r23, err := doReads(ctx, tctx2, keys[2:])
	assert.NoError(t, err)

	// Change k0 and k3 from a tm3.
	// This makes sure transactions need to be retried.
	commitWrites(t, tm3, []WriteAccess{
		{Path: keys[0], Val: []byte("3")},
		{Path: keys[3], Val: []byte("3")},
	})

	// Commit k0, k1 from tm1.
	h1 := tm1.Begin(ctx, Data{
		Reads: r01,
		Writes: []WriteAccess{
			{Path: keys[0], Val: []byte("1")},
			{Path: keys[1], Val: []byte("1")},
		},
	})
	err = tm1.Commit(ctx, h1)
	assert.ErrorIs(t, err, ErrRetry)

	// Commit k2, k3 from tm2.
	h2 := tm2.Begin(ctx, Data{
		Reads: r23,
		Writes: []WriteAccess{
			{Path: keys[2], Val: []byte("2")},
			{Path: keys[3], Val: []byte("2")},
		},
	})
	err = tm2.Commit(ctx, h2)
	assert.ErrorIs(t, err, ErrRetry)

	// Now the transactions are holding locks on k0 and k2.
	meta, err := tctx.global.GetMetadata(ctx, keys[0])
	assert.NoError(t, err)
	info, err := storage.TagsLockInfo(meta.Tags)
	assert.NoError(t, err)
	assert.Equal(t, storage.LockTypeWrite, info.Type)

	meta, err = tctx.global.GetMetadata(ctx, keys[2])
	assert.NoError(t, err)
	info, err = storage.TagsLockInfo(meta.Tags)
	assert.NoError(t, err)
	assert.Equal(t, storage.LockTypeWrite, info.Type)

	// To create the deadlock, we now make t1 read k0, k1, k3
	// and t2 read k0, k2, k3.
	r013, err := doReads(ctx, tctx, []string{keys[0], keys[1], keys[3]})
	assert.NoError(t, err)
	r023, err := doReads(ctx, tctx2, []string{keys[0], keys[2], keys[3]})
	assert.NoError(t, err)

	// Commit t2 in parallel.
	g := errgroup.Group{}
	g.Go(func() error {
		// Start the commit only after the timeout for serial locking expired.
		// This makes sure t1 will win over t2 in the commit race, making the
		// test deterministic.
		tctx2.clock.Sleep(10 * time.Second)

		tm2.Reset(h2, Data{
			Reads: r023,
			Writes: []WriteAccess{
				{Path: keys[0], Val: []byte("2")},
				{Path: keys[2], Val: []byte("2")},
				{Path: keys[3], Val: []byte("2")},
			},
		})
		return tm2.Commit(ctx, h2)
	})

	// In parallel, t1 tries to commit as well.
	tm1.Reset(h1, Data{
		Reads: r013,
		Writes: []WriteAccess{
			{Path: keys[0], Val: []byte("1")},
			{Path: keys[1], Val: []byte("1")},
			{Path: keys[3], Val: []byte("1")},
		},
	})
	err = tm1.Commit(ctx, h1)
	assert.NoError(t, err)

	// t2 should need to retry.
	err = g.Wait()
	assert.ErrorIs(t, err, ErrRetry)
	// Let's abort it now.
	err = tm2.End(ctx, h2)
	assert.NoError(t, err)

	// Check end result.
	flushWrites(t, tm1, h1)
	gr, err := tctx.global.Read(ctx, keys[0])
	assert.NoError(t, err)
	assert.Equal(t, "1", string(gr.Value))

	gr, err = tctx.global.Read(ctx, keys[1])
	assert.NoError(t, err)
	assert.Equal(t, "1", string(gr.Value))

	gr, err = tctx.global.Read(ctx, keys[2])
	assert.NoError(t, err)
	assert.Equal(t, "0", string(gr.Value))

	gr, err = tctx.global.Read(ctx, keys[3])
	assert.NoError(t, err)
	assert.Equal(t, "1", string(gr.Value))
}

func TestCleanAbort(t *testing.T) {
	// Test that aborting after retry doesn't leave things locked.
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	tm2, _ := newAlgoFromBackend(t, tctx.backend)

	for numWrites := 1; numWrites <= 3; numWrites++ {
		t.Run(fmt.Sprintf("%d writes", numWrites), func(t *testing.T) {
			keys := make([]string, numWrites)
			for i := 0; i < numWrites; i++ {
				keys[i] = paths.FromKey("testp", []byte(fmt.Sprintf("k%d", i)))
			}

			// Create keys from another algo.
			writes := make([]WriteAccess, len(keys))
			for i, k := range keys {
				writes[i] = WriteAccess{Path: k, Val: []byte("0")}
			}
			h := commitWrites(t, tm2, writes)
			flushWrites(t, tm2, h)

			// Read the values.
			reads, err := doReads(ctx, tctx, keys)
			assert.NoError(t, err)

			// Change one value from another algo.
			commitWrites(t, tm2, []WriteAccess{
				{Path: keys[numWrites-1], Val: []byte("x")},
			})

			// Then commit updated value.
			for i, k := range keys {
				writes[i] = WriteAccess{Path: k, Val: []byte("1")}
			}
			h = tm.Begin(ctx, Data{
				Reads:  reads,
				Writes: writes,
			})
			err = tm.Commit(ctx, h)
			// The read was inconsistent.
			assert.ErrorIs(t, err, ErrRetry)

			// Abort.
			err = tm.End(ctx, h)
			assert.NoError(t, err)

			// The keys should be lockable now.
			txtest := data.NewTId()
			tctx.tmon.BeginTx(ctx, txtest)
			for _, key := range keys {
				err := tctx.locker.LockWrite(ctx, key, txtest)
				assert.NoError(t, err)
				err = tctx.locker.Unlock(ctx, key, txtest)
				assert.NoError(t, err)
			}
			err = tctx.tmon.AbortTx(ctx, txtest)
			assert.NoError(t, err)
		})
	}
}

func TestChangeWritesCleanAbort(t *testing.T) {
	// Test that aborting after retry doesn't leave things locked.
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	keys := []string{
		paths.FromKey("testp", []byte("k1")),
		paths.FromKey("testp", []byte("k2")),
		paths.FromKey("testp", []byte("k3")),
	}

	// Create keys from another algo.
	tm2, _ := newAlgoFromBackend(t, tctx.backend)
	h := commitWrites(t, tm2, []WriteAccess{
		{Path: keys[0], Val: []byte("0")},
		{Path: keys[1], Val: []byte("0")},
		{Path: keys[2], Val: []byte("0")},
	})
	flushWrites(t, tm2, h)

	// Read 2 of the values.
	reads, err := doReads(ctx, tctx, keys[:2])
	assert.NoError(t, err)

	// Change one from another algo.
	commitWrites(t, tm2, []WriteAccess{
		{Path: keys[0], Val: []byte("x")},
	})

	// Then commit updated value.
	h = tm.Begin(ctx, Data{
		Reads: reads,
		Writes: []WriteAccess{
			{Path: keys[0], Val: []byte("1")},
			{Path: keys[1], Val: []byte("1")},
		},
	})
	err = tm.Commit(ctx, h)
	// The read was inconsistent.
	assert.ErrorIs(t, err, ErrRetry)

	// Read again, but different keys.
	reads, err = doReads(ctx, tctx, keys[1:])
	assert.NoError(t, err)

	// Change it from another algo.
	commitWrites(t, tm2, []WriteAccess{
		{Path: keys[2], Val: []byte("y")},
	})

	// Commit again.
	tm.Reset(h, Data{
		Reads: reads,
		Writes: []WriteAccess{
			{Path: keys[0], Val: []byte("1")},
		},
	})
	err = tm.Commit(ctx, h)
	// The read was inconsistent.
	assert.ErrorIs(t, err, ErrRetry)

	// Abort.
	err = tm.End(ctx, h)
	assert.NoError(t, err)

	// The keys should be lockable now.
	txtest := data.NewTId()
	tctx.tmon.BeginTx(ctx, txtest)
	for _, key := range keys {
		err := tctx.locker.LockWrite(ctx, key, txtest)
		assert.NoError(t, err)
		err = tctx.locker.Unlock(ctx, key, txtest)
		assert.NoError(t, err)
	}
	err = tctx.tmon.AbortTx(ctx, txtest)
	assert.NoError(t, err)
}

func TestSingleRWRetry(t *testing.T) {
	ctx := context.Background()
	tm, tctx := testAlgoContext(t)
	tm2, _ := newAlgoFromBackend(t, tctx.backend)
	keyp := paths.FromKey("testp", []byte("k"))

	// Initialize the value from the same algo.
	// This way we cache the value.
	commitWrites(t, tm2, []WriteAccess{
		{
			Path: keyp,
			Val:  []byte("v1"),
		},
	})

	// Read the value.
	ra, err := doRead(ctx, tctx, keyp)
	assert.NoError(t, err)
	assert.True(t, ra.Version.IsLocal())

	// Modify the value from another algo.
	// It's important that this is a single RW transaction.
	// This will make the previous read value stale.
	commitAccess(t, tm2, Data{
		Reads: []ReadAccess{ra},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  []byte("v"),
			},
		},
	})

	// Then commit.
	h := tm.Begin(ctx, Data{
		Reads: []ReadAccess{ra},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  []byte("v2"),
			},
		},
	})
	err = tm.Commit(ctx, h)
	// The read was inconsistent.
	assert.ErrorIs(t, err, ErrRetry)

	// Read again.
	ra, err = doRead(ctx, tctx, keyp)
	assert.NoError(t, err)

	// Commit again.
	tm.Reset(h, Data{
		Reads: []ReadAccess{ra},
		Writes: []WriteAccess{
			{
				Path: keyp,
				Val:  []byte("v2"),
			},
		},
	})
	err = tm.Commit(ctx, h)
	assert.NoError(t, err)
}

func TestExpiredTx(t *testing.T) {
	ctx := context.Background()
	tm2, tctx2 := testAlgoContext(t)
	backend := testkit.NewStallBackend(tctx2.backend)
	defer backend.WaitForStalled()
	tm, tctx := newAlgoFromBackend(t, backend)

	key1 := paths.FromKey("testp", []byte("k1"))
	key2 := paths.FromKey("testp", []byte("k2"))

	// Initialize the values.
	commitWrites(t, tm, []WriteAccess{
		{Path: key1, Val: []byte("v1")},
		{Path: key2, Val: []byte("v1")},
	})

	// Read them.
	ra, err := doReads(ctx, tctx, []string{key1, key2})
	assert.NoError(t, err)

	// Modify the values from another algo.
	// This will make the previous read values stale.
	commitAccess(t, tm2, Data{
		Reads: ra,
		Writes: []WriteAccess{
			{Path: key1, Val: []byte("v2")},
			{Path: key2, Val: []byte("v2")},
		},
	})

	// Try to commit the values (one read only and the other modified).
	h1 := tm.Begin(ctx, Data{
		Reads: ra,
		Writes: []WriteAccess{
			{Path: key1, Val: []byte("v3")},
		},
	})
	err = tm.Commit(ctx, h1)
	// This will fail with a retry and leave both value as locked.
	assert.ErrorIs(t, err, ErrRetry)
	info1 := lockInfo(ctx, t, tctx, key1)
	info2 := lockInfo(ctx, t, tctx, key2)
	assert.Equal(t, storage.LockTypeWrite, info1.Type)
	assert.Equal(t, storage.LockTypeRead, info2.Type)

	// Now stop tm1 access to the storage. This way it won't be able to keep
	// the transaction alive.
	backend.StallWrites()

	// The other manager should be able to commit, after deciding that the
	// old transaction expired.
	ctx, cancelCtx := concurr.ContextWithTimeout(ctx, tctx2.clock, 5*time.Minute)
	defer cancelCtx()

	ra, err = doReads(ctx, tctx, []string{key1, key2})
	assert.NoError(t, err)

	h2 := tm2.Begin(ctx, Data{
		Reads: ra,
		Writes: []WriteAccess{
			{Path: key1, Val: []byte("v4")},
			{Path: key2, Val: []byte("v4")},
		},
	})
	err = tm2.Commit(ctx, h2)
	assert.NoError(t, err)

	// The old manager can continue again.
	backend.ReleaseStalled()
	backend.StopStalling()

	// The old manager should now be unable to commit.
	assert.NoError(t, ctx.Err())
	tm.Reset(h1, Data{
		Writes: []WriteAccess{
			{Path: key1, Val: []byte("v5")},
		},
	})
	err = tm.Commit(ctx, h1)
	assert.ErrorIs(t, err, ErrAlreadyFinalized)
}

func commitAccess(t *testing.T, tm Algo, d Data) *Handle {
	t.Helper()
	ctx := context.Background()
	h := tm.Begin(ctx, d)
	err := tm.Commit(ctx, h)
	require.NoError(t, err)
	err = tm.End(ctx, h)
	require.NoError(t, err)
	return h
}

func commitWrites(t *testing.T, tm Algo, ws []WriteAccess) *Handle {
	t.Helper()
	return commitAccess(t, tm, Data{Writes: ws})
}

func flushWrites(t *testing.T, tm Algo, h *Handle) {
	t.Helper()
	err := tm.unlockAll(context.Background(), h)
	require.NoError(t, err)
}

func doRead(ctx context.Context, tctx testContext, path string) (ReadAccess, error) {
	reader := NewReader(tctx.local, tctx.global, tctx.tmon)
	rv, err := reader.Read(ctx, path, storage.MaxStaleness)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return ReadAccess{Path: path, Found: false}, nil
		}
		return ReadAccess{}, err
	}
	return ReadAccess{
		Path: path,
		Version: ReadVersion{
			Version:    rv.Version.B.Contents,
			LastWriter: rv.Version.Writer,
		},
		Found: true,
	}, nil
}

func doReads(ctx context.Context, tctx testContext, paths []string) ([]ReadAccess, error) {
	var res []ReadAccess
	for _, p := range paths {
		ra, err := doRead(ctx, tctx, p)
		if err != nil {
			return nil, fmt.Errorf("reading %q: %w", p, err)
		}
		res = append(res, ra)
	}
	return res, nil
}

func lockInfo(ctx context.Context, t *testing.T, tctx testContext, key string) storage.LockInfo {
	t.Helper()
	m, err := tctx.global.GetMetadata(ctx, key)
	require.NoError(t, err)
	info, err := storage.TagsLockInfo(m.Tags)
	require.NoError(t, err)
	return info
}
