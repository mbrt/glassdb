package glassdb_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
)

const fuzzTxTimeout = 10 * time.Minute

// FuzzConcurrentTx uses fuzzer-provided bytes to deterministically control
// the ordering of concurrent backend operations inside a synctest bubble.
// Two DB instances run a mix of transaction types on a shared set of keys,
// and invariants are checked after completion.
func FuzzConcurrentTx(f *testing.F) {
	f.Add(make([]byte, 64))
	f.Add(make([]byte, 256))
	// A few hand-picked seeds to cover edge cases.
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})            // all operations fire together
	f.Add([]byte{255, 0, 255, 0, 255, 0, 255, 0})    // alternating extremes
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})     // gradual ramp
	f.Add([]byte{128, 128, 128, 128, 128, 128, 128}) // uniform medium delay

	f.Fuzz(func(t *testing.T, schedule []byte) {
		synctest.Test(t, func(t *testing.T) {
			fuzzConcurrentTx(t, schedule)
		})
	})
}

func fuzzConcurrentTx(t *testing.T, schedule []byte) {
	ctx, cancel := context.WithCancel(context.Background())

	sched := middleware.NewScheduler(schedule, time.Millisecond)
	b := middleware.NewScheduledBackend(memory.New(), sched)
	db1 := initFuzzDB(t, b)
	db2 := initFuzzDB(t, b)
	defer func() {
		cancel()
		db1.Close(ctx)
		db2.Close(ctx)
	}()

	coll1 := db1.Collection([]byte("fuzz-coll"))
	coll2 := db2.Collection([]byte("fuzz-coll"))

	key1 := []byte("k1")
	key2 := []byte("k2")
	key3 := []byte("k3")

	// Initialize collection and keys to zero.
	err := coll1.Create(ctx)
	require.NoError(t, err)
	err = db1.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, k := range [][]byte{key1, key2, key3} {
			if err := tx.Write(coll1, k, writeInt(0)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Track successful increments per key from each DB.
	var db1k1, db1k2, db1k3 int64
	var db2k1, db2k2, db2k3 int64

	g := errgroup.Group{}

	// DB1 workload.
	g.Go(func() error {
		var err error
		// 1. Single-key RMW on k1.
		db1k1, err = fuzzRMW(ctx, db1, coll1, key1, 3)
		if err != nil {
			return fmt.Errorf("db1 rmw k1: %w", err)
		}
		// 2. Multi-key RMW on k1+k2.
		n, err := fuzzMultiRMW(ctx, db1, coll1, key1, key2, 2)
		if err != nil {
			return fmt.Errorf("db1 multi-rmw k1+k2: %w", err)
		}
		db1k1 += n
		db1k2 += n
		// 3. Read-only transaction: read all keys, verify non-negative.
		if err := fuzzReadOnly(ctx, db1, coll1, key1, key2, key3); err != nil {
			return fmt.Errorf("db1 read-only: %w", err)
		}
		// 4. Single-key RMW on k3.
		n, err = fuzzRMW(ctx, db1, coll1, key3, 2)
		if err != nil {
			return fmt.Errorf("db1 rmw k3: %w", err)
		}
		db1k3 += n
		return nil
	})

	// DB2 workload.
	g.Go(func() error {
		var err error
		// 1. Single-key RMW on k2.
		db2k2, err = fuzzRMW(ctx, db2, coll2, key2, 3)
		if err != nil {
			return fmt.Errorf("db2 rmw k2: %w", err)
		}
		// 2. Multi-key RMW on k2+k3.
		n, err := fuzzMultiRMW(ctx, db2, coll2, key2, key3, 2)
		if err != nil {
			return fmt.Errorf("db2 multi-rmw k2+k3: %w", err)
		}
		db2k2 += n
		db2k3 += n
		// 3. Read-only transaction: read all keys, verify non-negative.
		if err := fuzzReadOnly(ctx, db2, coll2, key1, key2, key3); err != nil {
			return fmt.Errorf("db2 read-only: %w", err)
		}
		// 4. Single-key RMW on k1.
		n, err = fuzzRMW(ctx, db2, coll2, key1, 2)
		if err != nil {
			return fmt.Errorf("db2 rmw k1: %w", err)
		}
		db2k1 += n
		return nil
	})

	err = g.Wait()
	require.NoError(t, err)

	// Verify invariants: final value of each key == total successful
	// increments from both DBs.
	v1, err := coll1.ReadStrong(ctx, key1)
	require.NoError(t, err)
	v2, err := coll1.ReadStrong(ctx, key2)
	require.NoError(t, err)
	v3, err := coll1.ReadStrong(ctx, key3)
	require.NoError(t, err)

	assert.Equal(t, db1k1+db2k1, readInt(v1), "k1 mismatch")
	assert.Equal(t, db1k2+db2k2, readInt(v2), "k2 mismatch")
	assert.Equal(t, db1k3+db2k3, readInt(v3), "k3 mismatch")
}

// fuzzRMW runs n single-key read-modify-write transactions, each
// incrementing the key by 1. Returns the number of successful increments.
func fuzzRMW(
	ctx context.Context,
	db *glassdb.DB,
	coll glassdb.Collection,
	key []byte,
	n int,
) (int64, error) {
	var count int64
	for i := range n {
		iterCtx, cancel := context.WithTimeout(ctx, fuzzTxTimeout)
		err := db.Tx(iterCtx, func(tx *glassdb.Tx) error {
			num, err := fuzzReadInt(tx, coll, key)
			if err != nil {
				return err
			}
			return tx.Write(coll, key, writeInt(num+1))
		})
		cancel()
		if err != nil {
			return count, fmt.Errorf("iteration %d: %w", i, err)
		}
		count++
	}
	return count, nil
}

// fuzzMultiRMW runs n transactions that atomically increment two keys.
// Returns the number of successful increments.
func fuzzMultiRMW(
	ctx context.Context,
	db *glassdb.DB,
	coll glassdb.Collection,
	keyA, keyB []byte,
	n int,
) (int64, error) {
	var count int64
	for i := range n {
		iterCtx, cancel := context.WithTimeout(ctx, fuzzTxTimeout)
		err := db.Tx(iterCtx, func(tx *glassdb.Tx) error {
			a, err := fuzzReadInt(tx, coll, keyA)
			if err != nil {
				return err
			}
			b, err := fuzzReadInt(tx, coll, keyB)
			if err != nil {
				return err
			}
			if err := tx.Write(coll, keyA, writeInt(a+1)); err != nil {
				return err
			}
			return tx.Write(coll, keyB, writeInt(b+1))
		})
		cancel()
		if err != nil {
			return count, fmt.Errorf("iteration %d: %w", i, err)
		}
		count++
	}
	return count, nil
}

// fuzzReadOnly runs a read-only transaction that reads all three keys
// and verifies each value is non-negative.
func fuzzReadOnly(
	ctx context.Context,
	db *glassdb.DB,
	coll glassdb.Collection,
	keys ...[]byte,
) error {
	iterCtx, cancel := context.WithTimeout(ctx, fuzzTxTimeout)
	defer cancel()
	return db.Tx(iterCtx, func(tx *glassdb.Tx) error {
		for _, k := range keys {
			val, err := tx.Read(coll, k)
			if err != nil {
				if errors.Is(err, backend.ErrNotFound) {
					continue
				}
				return err
			}
			v := readInt(val)
			if v < 0 {
				return fmt.Errorf("key %q has negative value %d", k, v)
			}
		}
		return nil
	})
}

func initFuzzDB(t testing.TB, b backend.Backend) *glassdb.DB {
	t.Helper()
	ctx := context.Background()
	opts := glassdb.DefaultOptions()
	opts.Logger = slog.New(nilHandler{})
	db, err := glassdb.OpenWith(ctx, "example", b, opts)
	if err != nil {
		t.Fatalf("creating DB: %v", err)
	}
	return db
}

func fuzzReadInt(tx *glassdb.Tx, c glassdb.Collection, k []byte) (int64, error) {
	val, err := tx.Read(c, k)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return readInt(val), nil
}
