// Package glassdb implements a key-value database on top of cloud object
// storage with serializable transactions.
package glassdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trace"
	"github.com/mbrt/glassdb/internal/trans"
)

var nameRegexp = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// DefaultOptions provides the options used by `Open`. They should be a good
// middle ground for a production deployment.
func DefaultOptions() Options {
	rc := concurr.DefaultRetryConfig()
	return Options{
		Logger:    slog.Default(),
		CacheSize: 5012 * 1024 * 1024, // 512 MiB
		Retry: RetryOptions{
			InitialInterval: rc.InitialInterval,
			MaxInterval:     rc.MaxInterval,
		},
	}
}

// Options makes it possible to tweak a client DB.
type Options struct {
	Logger *slog.Logger
	// Cache size is the number of bytes dedicated to caching objects and
	// and metadata. Setting this too small may impact performance, as
	// more calls to the backend would be necessary.
	CacheSize int
	// Retry tunes the exponential backoff used to retry transient
	// transaction-coordination operations.
	Retry RetryOptions
	// Rand is the source of randomness for transaction-ID prefixes. A nil
	// value uses crypto/rand. Deterministic tests can supply a seeded reader
	// to make transaction IDs (and thus a run) reproducible; the transaction
	// priority still comes from the clock. Rand must be safe for concurrent
	// use.
	Rand io.Reader
}

// RetryOptions tunes the exponential backoff the DB uses when retrying
// transient transaction-coordination operations, such as polling a peer
// transaction's commit status or writing a transaction's final log.
type RetryOptions struct {
	// InitialInterval is the delay before the first retry; it grows
	// exponentially up to MaxInterval. A non-positive value selects a default.
	InitialInterval time.Duration
	// MaxInterval caps the per-retry delay. A non-positive value selects a
	// default.
	MaxInterval time.Duration
	// DisableJitter turns off interval randomization. Jitter is enabled by
	// default because it spreads retries to avoid thundering-herd contention;
	// disable it only when deterministic retry timing is required, e.g. in
	// tests, since it draws from the process-global RNG.
	DisableJitter bool
}

// Open opens a database with the given name using default options.
func Open(ctx context.Context, name string, b backend.Backend) (*DB, error) {
	return OpenWith(ctx, name, b, DefaultOptions())
}

// OpenWith opens a database with the given name and custom options.
func OpenWith(ctx context.Context, name string, b backend.Backend, opts Options) (*DB, error) {
	if !nameRegexp.MatchString(name) {
		return nil, fmt.Errorf("name must be alphanumeric, got %q", name)
	}
	if err := checkOrCreateDBMeta(ctx, b, name); err != nil {
		return nil, err
	}

	cache := cache.New(opts.CacheSize)
	bg := concurr.NewBackground()

	backend := &statsBackend{inner: b}
	local := storage.NewLocal(cache)
	global := storage.NewGlobal(backend, local)
	tl := storage.NewTLogger(global, local, name)
	tmon := trans.NewMonitor(local, tl, bg, newRetrier(opts.Retry))
	locker := trans.NewLocker(local, global, tl, tmon)
	gc := trans.NewGC(bg, tl, opts.Logger)
	gc.Start(ctx)
	ta := trans.NewAlgo(
		global,
		local,
		locker,
		tmon,
		gc,
		bg,
		opts.Logger,
		data.NewTxIDSource(opts.Rand),
	)

	res := &DB{
		name:       name,
		backend:    backend,
		cache:      cache,
		background: bg,
		tmon:       tmon,
		gc:         gc,
		algo:       ta,
		logger:     opts.Logger,
	}
	res.root = res.openCollection(name)

	return res, nil
}

// DB represents an open glassdb database instance.
type DB struct {
	name       string
	backend    *statsBackend
	cache      *cache.Cache
	background *concurr.Background
	tmon       *trans.Monitor
	gc         *trans.GC
	algo       trans.Algo
	logger     *slog.Logger
	root       Collection
	stats      Stats
	statsM     sync.Mutex
}

// Close releases resources associated with the database.
func (d *DB) Close(context.Context) error {
	d.background.Close()
	return nil
}

// Collection returns a top-level collection with the given name.
func (d *DB) Collection(name []byte) Collection {
	p := paths.FromCollection(d.root.prefix, name)
	return d.openCollection(p)
}

// Tx executes f within a serializable transaction, retrying on conflicts.
func (d *DB) Tx(ctx context.Context, f func(tx *Tx) error) error {
	stats := &Stats{TxN: 1}
	begin := time.Now()
	ctx, task := trace.NewTask(ctx, "tx")
	err := d.txImpl(ctx, f, stats)
	task.End()
	stats.TxTime = time.Since(begin)
	d.updateStats(stats)
	return err
}

// Stats retrieves ongoing performance stats for the database. This is only
// updated when a transaction closes.
//
// Counters only increase over time and are never reset. If you need to measure
// a specific interval only, please see Stats.Sub.
func (d *DB) Stats() Stats {
	d.statsM.Lock()
	defer d.statsM.Unlock()

	// Update backend stats now.
	bstats := d.backend.StatsAndReset()
	d.stats.add(&bstats)
	return d.stats
}

func (d *DB) openCollection(prefix string) Collection {
	local := storage.NewLocal(d.cache)
	global := storage.NewGlobal(d.backend, local)
	return Collection{prefix, global, local, d.algo, d}
}

// newRetrier builds the internal retrier from the public retry options,
// filling unset (non-positive) intervals with defaults.
func newRetrier(o RetryOptions) concurr.Retrier {
	def := concurr.DefaultRetryConfig()
	cfg := concurr.RetryConfig{
		InitialInterval: o.InitialInterval,
		MaxInterval:     o.MaxInterval,
		Jitter:          !o.DisableJitter,
	}
	if cfg.InitialInterval <= 0 {
		cfg.InitialInterval = def.InitialInterval
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = def.MaxInterval
	}
	return concurr.NewRetrier(cfg)
}

func (d *DB) txImpl(ctx context.Context, fn func(tx *Tx) error, stats *Stats) (err error) {
	local := storage.NewLocal(d.cache)
	global := storage.NewGlobal(d.backend, local)

	tx := newTx(ctx, global, local, d.tmon)
	var handle *trans.Handle
	defer func() {
		// Wrapping this in func() is important, because handle is modified
		// after the defer statement is declared. We need to call End with
		// the updated parameter.
		if e := d.algo.End(ctx, handle); e != nil && err == nil {
			err = e
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var fnErr error
		trace.WithRegion(ctx, "user-tx", func() {
			fnErr = fn(tx)
		})
		if tx.aborted {
			return ErrAborted
		}

		// Collect the access.
		access := tx.collectAccesses()
		stats.TxReads += len(access.Reads)
		stats.TxWrites += len(access.Writes)
		if handle == nil {
			handle = d.algo.Begin(ctx, access)
		} else {
			d.algo.Reset(handle, access)
		}

		if fnErr != nil {
			// The user function returned an error. We need to check whether
			// this could be a result of a spurious read instead of a true
			// failure.
			// To do this, we validate only the reads.
			access.Writes = nil
			d.algo.Reset(handle, access)
			err := d.algo.ValidateReads(ctx, handle)
			if errors.Is(err, trans.ErrRetry) {
				// The failure may be spurious. Retry the transaction.
				tx.reset()
				stats.TxRetries++
				continue
			}
			// Otherwise we need to return the error coming from 'fn'.
			return fnErr
		}

		// Try to commit.
		trace.WithRegion(ctx, "commit", func() {
			err = d.algo.Commit(ctx, handle)
		})
		if err != nil {
			if errors.Is(err, trans.ErrWounded) {
				// A higher-priority transaction aborted us. Release whatever we
				// were holding and restart with a fresh ID that preserves our
				// priority, so we are not starved on the retry.
				_ = d.algo.End(ctx, handle)
				handle = d.algo.Rebegin(ctx, handle, access)
				tx.reset()
				stats.TxRetries++
				continue
			}
			if errors.Is(err, trans.ErrRetry) {
				// Retry the transaction.
				tx.reset()
				stats.TxRetries++
				continue
			}
			// Consider aborted.
			return err
		}

		// We are committed.
		break
	}

	return nil
}

func (d *DB) updateStats(s *Stats) {
	d.statsM.Lock()
	d.stats.add(s)
	d.statsM.Unlock()
}
