package glassdb_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
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
	sched := middleware.NewScheduler(schedule, time.Millisecond)
	b := middleware.NewScheduledBackend(memory.New(), sched)
	fuzzConcurrentTxWorkload(t, b)
}

// TestConcurrentTxDeterministicOutcome locks in the determinism the fuzzer
// relies on. The algorithm deliberately issues backend operations through
// concurrent fan-out and the two clients run in parallel, so the exact
// operation interleaving is NOT reproducible (testing/synctest virtualizes the
// clock but does not serialize concurrently-runnable goroutines). What the
// deterministic fuzz settings do guarantee — seeded transaction-ID prefixes and
// disabled backoff jitter (see docs/adr/007 and docs/adr/008) together with the
// serializability guarantee — is that a given schedule always commits the same
// result.
//
// The test replays each schedule many times. Because every replay interleaves
// the two clients differently, any regression that lets the committed state
// depend on that interleaving — e.g. the ADR-007 lost update, where a committed
// increment could be silently dropped — would make the replays disagree and
// fail here. fuzzConcurrentTxWorkload additionally checks, on every run, that
// the committed values equal the increments the clients tracked.
func TestConcurrentTxDeterministicOutcome(t *testing.T) {
	t.Parallel()

	// Fixed schedules chosen to drive a range of interleavings, including
	// heavy contention (which triggers the aborts, retries and background
	// status/refresh workers where nondeterminism would surface).
	schedules := map[string][]byte{
		"all-zero":             make([]byte, 64),
		"alternating-extremes": {255, 0, 255, 0, 255, 0, 255, 0},
		"gradual-ramp":         {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		"uniform-medium":       {128, 128, 128, 128, 128, 128, 128},
		"mixed":                {7, 200, 13, 0, 255, 64, 1, 99, 128, 3, 240, 17},
	}

	const replays = 16

	for name, schedule := range schedules {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reference := runConcurrentTxOutcome(t, schedule)
			for i := 1; i < replays; i++ {
				got := runConcurrentTxOutcome(t, schedule)
				require.Equal(t, reference, got,
					"replay %d committed a different result than the first run: "+
						"the schedule's outcome is no longer deterministic", i)
			}
		})
	}
}

// runConcurrentTxOutcome runs the shared two-client workload for one schedule
// inside its own synctest bubble and returns the committed outcome.
func runConcurrentTxOutcome(t *testing.T, schedule []byte) fuzzOutcome {
	t.Helper()
	var out fuzzOutcome
	synctest.Test(t, func(t *testing.T) {
		sched := middleware.NewScheduler(schedule, time.Millisecond)
		b := middleware.NewScheduledBackend(memory.New(), sched)
		out = fuzzConcurrentTxWorkload(t, b)
	})
	return out
}

// fuzzOutcome is the observable committed result of fuzzConcurrentTxWorkload:
// the final value of every key together with the per-key increment totals the
// two clients believe they committed. For a given schedule it must be
// identical on every replay; see TestConcurrentTxDeterministicOutcome.
type fuzzOutcome struct {
	Final [3]int64 // committed value of k1, k2, k3 read back after the workload
	Want  [3]int64 // sum of successful increments both clients tracked per key
}

// fuzzConcurrentTxWorkload runs the two-client concurrent workload against the
// given backend and checks the post-conditions. It is the deterministic core
// shared by the fuzzer (FuzzConcurrentTx) and the determinism regression test
// (TestConcurrentTxDeterministicOutcome).
func fuzzConcurrentTxWorkload(t *testing.T, b backend.Backend) fuzzOutcome {
	ctx, cancel := context.WithCancel(context.Background())

	// Give each DB its own deterministic source of transaction-ID prefixes so a
	// given schedule reproduces exactly. Distinct seeds keep the two clients'
	// IDs disjoint; transaction priority comes from the (deterministic) clock,
	// so this does not bias which schedules the fuzzer explores. Backoff jitter
	// (the other RNG-driven source of nondeterminism) is disabled in initFuzzDB.
	db1 := initFuzzDB(t, b, newDetReader(1))
	db2 := initFuzzDB(t, b, newDetReader(2))
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

	return fuzzOutcome{
		Final: [3]int64{readInt(v1), readInt(v2), readInt(v3)},
		Want:  [3]int64{db1k1 + db2k1, db1k2 + db2k2, db1k3 + db2k3},
	}
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

// newDetReader returns a deterministic, concurrency-safe source of random bytes
// used as a DB's transaction-ID prefix source so a given schedule reproduces
// exactly under a fixed seed.
func newDetReader(seed int64) io.Reader {
	return &lockedRand{r: rand.New(rand.NewSource(seed))}
}

type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) Read(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Read(p)
}

func initFuzzDB(t testing.TB, b backend.Backend, rng io.Reader) *glassdb.DB {
	t.Helper()
	ctx := context.Background()
	opts := glassdb.DefaultOptions()
	opts.Logger = slog.New(nilHandler{})
	// Seed transaction IDs from a deterministic source so a run is
	// reproducible. Jitter stays off: the retrier runs in background goroutines
	// whose interleaving testing/synctest does not serialize, so jitter reads
	// from the seeded source would not be consumed in a reproducible order.
	opts.Rand = rng
	opts.Retry.DisableJitter = true
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
