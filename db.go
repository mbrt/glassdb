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

package glassdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trace"
	"github.com/mbrt/glassdb/internal/trans"
)

const cacheSize = 512 * 1024 * 1024 // 512 MiB

var nameRegexp = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// DefaultOptions provides the options used by `Open`. They should be a good
// middle ground for a production deployment.
func DefaultOptions() Options {
	return Options{
		Clock:  clockwork.NewRealClock(),
		Logger: slog.Default(),
	}
}

// Options makes it possible to tweak a client DB.
//
// TODO: Add cache size and retry timing options.
type Options struct {
	Clock  clockwork.Clock
	Logger *slog.Logger
}

func Open(ctx context.Context, name string, b backend.Backend) (*DB, error) {
	return OpenWith(ctx, name, b, DefaultOptions())
}

func OpenWith(ctx context.Context, name string, b backend.Backend, opts Options) (*DB, error) {
	if !nameRegexp.MatchString(name) {
		return nil, fmt.Errorf("name must be alphanumeric, got %q", name)
	}
	if err := checkOrCreateDBMeta(ctx, b, name); err != nil {
		return nil, err
	}

	cache := cache.New(cacheSize)
	bg := concurr.NewBackground()

	backend := &statsBackend{inner: b}
	local := storage.NewLocal(cache, opts.Clock)
	global := storage.NewGlobal(backend, local, opts.Clock)
	tl := storage.NewTLogger(opts.Clock, global, local, name)
	tmon := trans.NewMonitor(opts.Clock, local, tl, bg)
	locker := trans.NewLocker(local, global, tl, opts.Clock, tmon)
	ta := trans.NewAlgo(
		opts.Clock,
		global,
		local,
		locker,
		tmon,
		bg,
		opts.Logger,
	)

	res := &DB{
		name:       name,
		backend:    backend,
		cache:      cache,
		background: bg,
		tmon:       tmon,
		algo:       ta,
		clock:      opts.Clock,
		logger:     opts.Logger,
	}
	res.root = res.openCollection(name)

	return res, nil
}

type DB struct {
	name       string
	backend    *statsBackend
	cache      *cache.Cache
	background *concurr.Background
	tmon       *trans.Monitor
	algo       trans.Algo
	clock      clockwork.Clock
	logger     *slog.Logger
	root       Collection
	stats      Stats
	statsM     sync.Mutex
}

func (d *DB) Close(context.Context) error {
	d.background.Close()
	return nil
}

func (d *DB) Collection(name []byte) Collection {
	p := paths.FromCollection(d.root.prefix, name)
	return d.openCollection(p)
}

func (d *DB) Tx(ctx context.Context, f func(tx *Tx) error) error {
	stats := &Stats{TxN: 1}
	begin := d.clock.Now()
	ctx, task := trace.NewTask(ctx, "tx")
	err := d.txImpl(ctx, f, stats)
	task.End()
	stats.TxTime = d.clock.Now().Sub(begin)
	d.updateStats(stats)
	return err
}

func (d *DB) Stats() Stats {
	d.statsM.Lock()
	defer d.statsM.Unlock()

	// Update backend stats now.
	bstats := d.backend.StatsAndReset()
	d.stats.add(&bstats)
	return d.stats
}

func (d *DB) openCollection(prefix string) Collection {
	local := storage.NewLocal(d.cache, d.clock)
	global := storage.NewGlobal(d.backend, local, d.clock)
	return Collection{prefix, global, local, d.algo, d}
}

func (d *DB) txImpl(ctx context.Context, fn func(tx *Tx) error, stats *Stats) (err error) {
	local := storage.NewLocal(d.cache, d.clock)
	global := storage.NewGlobal(d.backend, local, d.clock)

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
			handle = d.algo.Begin(access)
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
			if errors.Is(err, trans.ErrRetry) {
				// Retry the transaction.
				tx.reset()
				stats.TxRetries++
				continue
			}
			// Consider aborted.
			// TODO: Restart the transaction on trans.ErrTxAlreadyFinalized.
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
