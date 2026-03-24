package storage

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
)

const testCollName = "testp"

var collInfoContents = []byte("__foo__")

type testContext struct {
	backend backend.Backend
	global  Global
	local   Local
}

func testTLoggerContext(t *testing.T) (TLogger, testContext) {
	t.Helper()
	return newTLoggerFromBackend(t, memory.New())
}

func newTLoggerFromBackend(t *testing.T, b backend.Backend) (TLogger, testContext) {
	t.Helper()

	cache := cache.New(1024)
	local := NewLocal(cache)
	global := NewGlobal(b, local)
	tlogger := NewTLogger(global, local, testCollName)

	ctx := context.Background()
	_, err := global.Write(ctx, paths.CollectionInfo(testCollName), collInfoContents, nil)
	require.NoError(t, err)

	return tlogger, testContext{
		backend: b,
		global:  global,
		local:   local,
	}
}

func TestGetSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tl, _ := testTLoggerContext(t)
		ctx := context.Background()
		tx := data.TxID([]byte("tx1"))

		// Unknown transactions are not found.
		_, err := tl.Get(ctx, tx)
		assert.ErrorIs(t, err, backend.ErrNotFound)
		// Their status is unknown.
		status, err := tl.CommitStatus(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, TxCommitStatusUnknown, status.Status)

		beforeSetTs := time.Now().Truncate(time.Second)
		collection := paths.FromCollection("example", []byte("coll"))

		log := TxLog{
			ID:     tx,
			Status: TxCommitStatusOK,
			Writes: []TxWrite{
				{
					Path:  paths.FromKey(collection, []byte("key1")),
					Value: []byte("val"),
				},
			},
			Locks: []PathLock{
				{
					Path: paths.CollectionInfo(collection),
					Type: LockTypeWrite,
				},
				{
					Path: paths.FromKey(collection, []byte("key1")),
					Type: LockTypeCreate,
				},
				{
					Path: paths.FromKey(collection, []byte("key2")),
					Type: LockTypeUnknown,
				},
				{
					Path: paths.CollectionInfo(paths.FromCollection("example", []byte("coll2"))),
					Type: LockTypeRead,
				},
			},
		}

		// Write the log.
		_, err = tl.Set(ctx, log)
		assert.NoError(t, err)

		// The log comes back.
		gotlog, err := tl.Get(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, log.ID, gotlog.ID)
		assert.Equal(t, log.Status, gotlog.Status)
		assert.GreaterOrEqual(t, gotlog.Timestamp, beforeSetTs)
		assert.ElementsMatch(t, log.Locks, gotlog.Locks)
		assert.ElementsMatch(t, log.Writes, gotlog.Writes)

		// The status is correct.
		status, err = tl.CommitStatus(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, TxCommitStatusOK, status.Status)
		assert.GreaterOrEqual(t, status.LastUpdate, beforeSetTs)
	})
}

func TestPendingUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tl1, tctx := testTLoggerContext(t)
		tl2, _ := newTLoggerFromBackend(t, tctx.backend)
		ctx := context.Background()
		tx := data.TxID([]byte("tx1"))

		beforeSetTs := time.Now().Truncate(time.Second)

		vers, err := tl1.Set(ctx, TxLog{
			ID:     tx,
			Status: TxCommitStatusPending,
		})
		assert.NoError(t, err)

		// Check status and get.
		status, err := tl2.CommitStatus(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, TxCommitStatusPending, status.Status)
		assert.GreaterOrEqual(t, status.LastUpdate, beforeSetTs)
		log, err := tl2.Get(ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, tx, log.ID)
		assert.Equal(t, TxCommitStatusPending, log.Status)
		assert.GreaterOrEqual(t, log.Timestamp, beforeSetTs)

		// Wait some time.
		time.Sleep(5 * time.Second)
		beforeOverwriteTs := time.Now().Truncate(time.Second)

		// Overwrite the log with an updated pending state.
		_, err = tl1.SetIf(ctx, TxLog{
			ID:     tx,
			Status: TxCommitStatusPending,
		}, vers)
		assert.NoError(t, err)

		// We should see the updated timestamp.
		status, err = tl2.CommitStatus(ctx, tx)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, status.LastUpdate, beforeOverwriteTs)
		log, err = tl2.Get(ctx, tx)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, log.Timestamp, beforeOverwriteTs)
	})
}
