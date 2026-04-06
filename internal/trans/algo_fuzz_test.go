package trans

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
)

const fuzzAlgoTxTimeout = 10 * time.Minute

// FuzzAlgoConcurrentTx uses fuzzer-provided bytes to control backend
// operation ordering via a ScheduledBackend inside a synctest bubble.
// Two Algo instances share the same backend, each running a mix of
// single-key RMW, multi-key RMW, and read-only transactions on
// overlapping keys. Invariants are checked after completion.
func FuzzAlgoConcurrentTx(f *testing.F) {
	f.Add(make([]byte, 64))
	f.Add(make([]byte, 256))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{255, 0, 255, 0, 255, 0, 255, 0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Add([]byte{128, 128, 128, 128, 128, 128, 128})

	f.Fuzz(func(t *testing.T, schedule []byte) {
		synctest.Test(t, func(t *testing.T) {
			fuzzAlgoConcurrentTx(t, schedule)
		})
	})
}

type fuzzAlgoEnv struct {
	algo   Algo
	reader Reader
	tctx   testContext
}

func newFuzzAlgoEnv(t testing.TB, b backend.Backend) fuzzAlgoEnv {
	t.Helper()

	log := slog.New(nilHandler{})
	c := cache.New(1024)
	local := storage.NewLocal(c)
	global := storage.NewGlobal(b, local)
	tlogger := storage.NewTLogger(global, local, testCollName)
	background := concurr.NewBackground()
	t.Cleanup(background.Close)
	tmon := NewMonitor(local, tlogger, background)
	locker := NewLocker(local, global, tlogger, tmon)
	gc := NewGC(background, tlogger, log)

	tm := NewAlgo(global, local, locker, tmon, gc, nil, log)
	return fuzzAlgoEnv{
		algo:   tm,
		reader: NewReader(local, global, tmon),
		tctx: testContext{
			backend: b,
			global:  global,
			local:   local,
			tlogger: tlogger,
			tmon:    tmon,
			locker:  locker,
		},
	}
}

// fuzzRead reads a key through the Reader, returning the ReadAccess and
// the current value atomically.
func fuzzRead(ctx context.Context, env fuzzAlgoEnv, key string) (ReadAccess, int64, error) {
	rv, err := env.reader.Read(ctx, key, storage.MaxStaleness)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return ReadAccess{Path: key, Found: false}, 0, nil
		}
		return ReadAccess{}, 0, err
	}
	ra := ReadAccess{
		Path: key,
		Version: ReadVersion{
			Version:    rv.Version.B.Contents,
			LastWriter: rv.Version.Writer,
		},
		Found: true,
	}
	return ra, readVal(rv.Value), nil
}

func fuzzAlgoConcurrentTx(t *testing.T, schedule []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := middleware.NewScheduler(schedule, time.Millisecond)
	b := middleware.NewScheduledBackend(memory.New(), sched)

	env1 := newFuzzAlgoEnv(t, b)
	env2 := newFuzzAlgoEnv(t, b)

	keys := []string{
		paths.FromKey(testCollName, []byte("k1")),
		paths.FromKey(testCollName, []byte("k2")),
		paths.FromKey(testCollName, []byte("k3")),
	}

	// Create the collection info object.
	_, err := env1.tctx.global.Write(ctx,
		paths.CollectionInfo(testCollName), collInfoContents, nil)
	require.NoError(t, err)

	// Initialize all keys to zero.
	initWrites := make([]WriteAccess, len(keys))
	for i, k := range keys {
		initWrites[i] = WriteAccess{Path: k, Val: writeVal(0)}
	}
	h := env1.algo.Begin(ctx, Data{Writes: initWrites})
	require.NoError(t, env1.algo.Commit(ctx, h))
	require.NoError(t, env1.algo.End(ctx, h))
	require.NoError(t, env1.algo.unlockAll(ctx, h))

	// Track successful increments per key from each algo.
	var a1k1, a1k2, a1k3 int64
	var a2k1, a2k2, a2k3 int64

	g := errgroup.Group{}

	// Algo1 workload.
	g.Go(func() error {
		// 1. Single-key RMW on k1 (3 iterations).
		n, err := fuzzAlgoRMW(ctx, env1, keys[0], 3)
		if err != nil {
			return fmt.Errorf("a1 rmw k1: %w", err)
		}
		a1k1 += n
		// 2. Multi-key RMW on k1+k2 (2 iterations).
		n, err = fuzzAlgoMultiRMW(ctx, env1, keys[0], keys[1], 2)
		if err != nil {
			return fmt.Errorf("a1 multi-rmw k1+k2: %w", err)
		}
		a1k1 += n
		a1k2 += n
		// 3. Read-only transaction.
		if err := fuzzAlgoReadOnly(ctx, env1, keys); err != nil {
			return fmt.Errorf("a1 read-only: %w", err)
		}
		// 4. Single-key RMW on k3 (2 iterations).
		n, err = fuzzAlgoRMW(ctx, env1, keys[2], 2)
		if err != nil {
			return fmt.Errorf("a1 rmw k3: %w", err)
		}
		a1k3 += n
		return nil
	})

	// Algo2 workload.
	g.Go(func() error {
		// 1. Single-key RMW on k2 (3 iterations).
		n, err := fuzzAlgoRMW(ctx, env2, keys[1], 3)
		if err != nil {
			return fmt.Errorf("a2 rmw k2: %w", err)
		}
		a2k2 += n
		// 2. Multi-key RMW on k2+k3 (2 iterations).
		n, err = fuzzAlgoMultiRMW(ctx, env2, keys[1], keys[2], 2)
		if err != nil {
			return fmt.Errorf("a2 multi-rmw k2+k3: %w", err)
		}
		a2k2 += n
		a2k3 += n
		// 3. Read-only transaction.
		if err := fuzzAlgoReadOnly(ctx, env2, keys); err != nil {
			return fmt.Errorf("a2 read-only: %w", err)
		}
		// 4. Single-key RMW on k1 (2 iterations).
		n, err = fuzzAlgoRMW(ctx, env2, keys[0], 2)
		if err != nil {
			return fmt.Errorf("a2 rmw k1: %w", err)
		}
		a2k1 += n
		return nil
	})

	err = g.Wait()
	require.NoError(t, err)

	// Verify invariants.
	for i, key := range keys {
		gr, err := env1.tctx.global.Read(ctx, key)
		require.NoError(t, err, "reading key %d", i)

		got := readVal(gr.Value)
		var want int64
		switch i {
		case 0:
			want = a1k1 + a2k1
		case 1:
			want = a1k2 + a2k2
		case 2:
			want = a1k3 + a2k3
		}
		assert.Equal(t, want, got, "key %d mismatch", i)
	}
}

// fuzzAlgoRMW runs n single-key read-modify-write transactions through
// the Algo Begin/Commit/Reset/End cycle.
func fuzzAlgoRMW(ctx context.Context, env fuzzAlgoEnv, key string, n int) (int64, error) {
	var count int64
	for i := range n {
		iterCtx, cancel := context.WithTimeout(ctx, fuzzAlgoTxTimeout)
		err := fuzzAlgoTx(iterCtx, env,
			[]string{key},
			func(reads []ReadAccess, vals []int64) Data {
				return Data{
					Reads: reads,
					Writes: []WriteAccess{
						{Path: key, Val: writeVal(vals[0] + 1)},
					},
				}
			},
		)
		cancel()
		if err != nil {
			return count, fmt.Errorf("iteration %d: %w", i, err)
		}
		count++
	}
	return count, nil
}

// fuzzAlgoMultiRMW runs n transactions that atomically increment two keys.
func fuzzAlgoMultiRMW(ctx context.Context, env fuzzAlgoEnv, keyA, keyB string, n int) (int64, error) {
	var count int64
	for i := range n {
		iterCtx, cancel := context.WithTimeout(ctx, fuzzAlgoTxTimeout)
		err := fuzzAlgoTx(iterCtx, env,
			[]string{keyA, keyB},
			func(reads []ReadAccess, vals []int64) Data {
				return Data{
					Reads: reads,
					Writes: []WriteAccess{
						{Path: keyA, Val: writeVal(vals[0] + 1)},
						{Path: keyB, Val: writeVal(vals[1] + 1)},
					},
				}
			},
		)
		cancel()
		if err != nil {
			return count, fmt.Errorf("iteration %d: %w", i, err)
		}
		count++
	}
	return count, nil
}

// fuzzAlgoReadOnly runs a read-only transaction that reads all keys and
// verifies values are non-negative.
func fuzzAlgoReadOnly(ctx context.Context, env fuzzAlgoEnv, keys []string) error {
	iterCtx, cancel := context.WithTimeout(ctx, fuzzAlgoTxTimeout)
	defer cancel()

	reads := make([]ReadAccess, len(keys))
	for i, k := range keys {
		ra, val, err := fuzzRead(iterCtx, env, k)
		if err != nil {
			return err
		}
		if ra.Found && val < 0 {
			return fmt.Errorf("key %q has negative value %d", k, val)
		}
		reads[i] = ra
	}

	h := env.algo.Begin(iterCtx, Data{Reads: reads})
	err := env.algo.Commit(iterCtx, h)
	if errors.Is(err, ErrRetry) {
		return env.algo.End(iterCtx, h)
	}
	if err != nil {
		_ = env.algo.End(iterCtx, h)
		return err
	}
	return env.algo.End(iterCtx, h)
}

// fuzzAlgoTx runs a full transaction with automatic retry. buildData is
// called with the current reads and their values to produce the Data.
func fuzzAlgoTx(
	ctx context.Context,
	env fuzzAlgoEnv,
	readKeys []string,
	buildData func(reads []ReadAccess, vals []int64) Data,
) error {
	reads, vals, err := fuzzReadAll(ctx, env, readKeys)
	if err != nil {
		return err
	}
	d := buildData(reads, vals)
	h := env.algo.Begin(ctx, d)

	for {
		err := env.algo.Commit(ctx, h)
		if err == nil {
			endErr := env.algo.End(ctx, h)
			if endErr != nil {
				return endErr
			}
			return env.algo.unlockAll(ctx, h)
		}
		if !errors.Is(err, ErrRetry) {
			_ = env.algo.End(ctx, h)
			return err
		}
		// Re-read and retry.
		reads, vals, err = fuzzReadAll(ctx, env, readKeys)
		if err != nil {
			_ = env.algo.End(ctx, h)
			return err
		}
		d = buildData(reads, vals)
		env.algo.Reset(h, d)
	}
}

func fuzzReadAll(ctx context.Context, env fuzzAlgoEnv, keys []string) ([]ReadAccess, []int64, error) {
	reads := make([]ReadAccess, len(keys))
	vals := make([]int64, len(keys))
	for i, k := range keys {
		ra, v, err := fuzzRead(ctx, env, k)
		if err != nil {
			return nil, nil, err
		}
		reads[i] = ra
		vals[i] = v
	}
	return reads, vals, nil
}

func writeVal(v int64) []byte {
	buf := make([]byte, 8)
	buf[0] = byte(v >> 56)
	buf[1] = byte(v >> 48)
	buf[2] = byte(v >> 40)
	buf[3] = byte(v >> 32)
	buf[4] = byte(v >> 24)
	buf[5] = byte(v >> 16)
	buf[6] = byte(v >> 8)
	buf[7] = byte(v)
	return buf
}

func readVal(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 |
		int64(b[3])<<32 | int64(b[4])<<24 | int64(b[5])<<16 |
		int64(b[6])<<8 | int64(b[7])
}
