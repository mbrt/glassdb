package trans

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/storage"
)

const (
	cleanupInterval = time.Minute
	sizeLimit       = 1024
)

func NewGC(
	clock clockwork.Clock,
	bg *concurr.Background,
	tl storage.TLogger,
	log *slog.Logger,
) *GC {
	return &GC{
		clock: clock,
		bg:    bg,
		tl:    tl,
		log:   log,
	}
}

type GC struct {
	clock clockwork.Clock
	bg    *concurr.Background
	tl    storage.TLogger
	log   *slog.Logger
	items []cleanupItem
	m     sync.Mutex
}

func (g *GC) Start(ctx context.Context) {
	g.bg.Go(ctx, func(ctx context.Context) {
		tick := g.clock.NewTicker(cleanupInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.Chan():
				g.cleanupRound(ctx)
			}
		}
	})
}

func (g *GC) ScheduleTxCleanup(txid data.TxID) {
	t := g.clock.Now().Add(cleanupInterval)

	g.m.Lock()
	// Avoid growing indefinitely.
	if len(g.items) > sizeLimit {
		g.m.Unlock()
		g.log.Debug("gc: too many items to cleanup")
		return
	}
	g.items = append(g.items, cleanupItem{
		dueTime: t,
		txID:    txid,
	})
	g.m.Unlock()
}

func (g *GC) cleanupRound(ctx context.Context) {
	now := g.clock.Now()
	toCleanup := g.filterDueItems(now)

	for _, item := range toCleanup {
		if err := g.tl.Delete(ctx, item.txID); err != nil {
			g.log.Warn("failed to delete transaction log", "err", err, "txid", item.txID)
		}
	}
}

func (g *GC) filterDueItems(now time.Time) []cleanupItem {
	g.m.Lock()
	defer g.m.Unlock()

	var toCleanup []cleanupItem
	i := 0
	for ; i < len(g.items); i++ {
		if g.items[i].dueTime.After(now) {
			break
		}
		toCleanup = append(toCleanup, g.items[i])
	}
	g.items = g.items[i:]
	return toCleanup
}

type cleanupItem struct {
	dueTime time.Time
	txID    data.TxID
}
