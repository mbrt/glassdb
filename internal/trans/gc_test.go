package trans

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/testkit"
)

const conditionTimeout = time.Second

func TestGC(t *testing.T) {
	gc, tctx := newTestGC(t)
	start := tctx.clock.Now()
	txid := data.TxID([]byte("tx1"))

	// Create a transaction log.
	_, err := gc.tl.Set(tctx.ctx, storage.TxLog{
		ID:        txid,
		Timestamp: start,
		Status:    storage.TxCommitStatusOK,
	})
	assert.NoError(t, err)

	// Make sure the log is there.
	tl, err := gc.tl.Get(tctx.ctx, txid)
	assert.NoError(t, err)
	assert.Equal(t, txid, tl.ID)

	gc.ScheduleTxCleanup(txid)

	// Wait for the GC to run.
	tctx.clock.Sleep(cleanupInterval * 2)

	waitForCondition(t, func() bool {
		_, err := gc.tl.Get(tctx.ctx, txid)
		return errors.Is(err, backend.ErrNotFound)
	})
}

type gcTestContext struct {
	ctx   context.Context
	clock clockwork.Clock
}

func newTestGC(t *testing.T) (*GC, gcTestContext) {
	t.Helper()

	// It's important to close the context at the end to stop
	// any background tasks.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	clock := testkit.NewAcceleratedClock(clockMultiplier)
	log := slog.New(nilHandler{})
	cache := cache.New(1024)
	local := storage.NewLocal(cache, clock)
	global := storage.NewGlobal(memory.New(), local, clock)
	tlogger := storage.NewTLogger(clock, global, local, testCollName)
	background := concurr.NewBackground()
	gc := NewGC(clock, background, tlogger, log)
	gc.Start(ctx)

	return gc, gcTestContext{
		ctx, clock,
	}
}

func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	for start := time.Now(); time.Since(start) < conditionTimeout; {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("condition not met within 1s")
}
