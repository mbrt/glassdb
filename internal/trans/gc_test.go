package trans

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
)

func TestGC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gc, tctx := newTestGC(t)
		txid := data.TxID([]byte("tx1"))

		_, err := gc.tl.Set(tctx.ctx, storage.TxLog{
			ID:        txid,
			Timestamp: time.Now(),
			Status:    storage.TxCommitStatusOK,
		})
		assert.NoError(t, err)

		tl, err := gc.tl.Get(tctx.ctx, txid)
		assert.NoError(t, err)
		assert.Equal(t, txid, tl.ID)

		gc.ScheduleTxCleanup(txid)

		waitForCondition(t, func() bool {
			_, err := gc.tl.Get(tctx.ctx, txid)
			return errors.Is(err, backend.ErrNotFound)
		})

		// Cancel the context to stop the GC goroutine before the bubble exits.
		tctx.cancel()
		synctest.Wait()
	})
}

type gcTestContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func newTestGC(t *testing.T) (*GC, gcTestContext) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	log := slog.New(nilHandler{})
	cache := cache.New(1024)
	local := storage.NewLocal(cache)
	global := storage.NewGlobal(memory.New(), local)
	tlogger := storage.NewTLogger(global, local, testCollName)
	background := concurr.NewBackground()
	t.Cleanup(background.Close)
	gc := NewGC(background, tlogger, log)
	gc.Start(ctx)

	return gc, gcTestContext{ctx, cancel}
}

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	time.Sleep(cleanupInterval * 3)
	synctest.Wait()
	assert.True(t, cond(), "condition not met")
}
