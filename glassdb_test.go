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

package glassdb_test

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/internal/stringset"
	"github.com/mbrt/glassdb/internal/testkit"
)

const (
	txTimeout       = 10 * time.Second
	clockMultiplier = 1000
)

// debug flags.
var (
	debugBackend = flag.Bool("debug-backend", false, "debug backend requests")
	debugLogs    = flag.Bool("debug-logs", false, "debug db logs")
)

func initGCSBackend(t testing.TB) backend.Backend {
	t.Helper()
	ctx := context.Background()
	client := testkit.NewGCSClient(ctx, t, false)
	bucket := client.Bucket("test")
	if err := bucket.Create(ctx, "", nil); err != nil {
		t.Fatalf("creating 'test' bucket: %v", err)
	}
	backend := gcs.New(bucket)
	if *debugBackend {
		return middleware.NewBackendLogger(backend, "Backend", glassdb.ConsoleLogger{})
	}
	return backend
}

func initDB(t testing.TB, b backend.Backend, c clockwork.Clock) *glassdb.DB {
	t.Helper()
	ctx := context.Background()
	opts := glassdb.Options{
		Clock: c,
	}
	if *debugLogs {
		opts.Logger = glassdb.ConsoleLogger{}
		opts.Tracer = glassdb.ConsoleLogger{}
	} else {
		opts.Logger = glassdb.NoLogger{}
		opts.Tracer = glassdb.NoLogger{}
	}
	db, err := glassdb.OpenWith(ctx, "example", b, opts)
	if err != nil {
		t.Fatalf("Failed creating DB: %v", err)
	}
	t.Cleanup(func() {
		db.Close(ctx)
	})
	return db
}

func TestRW(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	db := initDB(t, back, clockwork.NewRealClock())

	key := []byte("key1")
	val := []byte("value1")

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	// Read and write.
	err = coll.Write(ctx, key, val)
	assert.NoError(t, err)
	buf, err := coll.ReadStrong(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, string(val), string(buf))

	stats := db.Stats()
	assert.Equal(t, 2, stats.TxN)
	assert.Equal(t, 1, stats.TxWrites)
	assert.Equal(t, 0, stats.TxRetries)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	db := initDB(t, back, clockwork.NewRealClock())

	key := []byte("key1")
	val := []byte("value1")

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	// Write and delete.
	err = coll.Write(ctx, key, val)
	assert.NoError(t, err)
	err = coll.Delete(ctx, key)
	assert.NoError(t, err)

	_, err = coll.ReadStrong(ctx, key)
	assert.ErrorIs(t, err, backend.ErrNotFound)

	stats := db.Stats()
	assert.Equal(t, 3, stats.TxN)
	assert.Equal(t, 2, stats.TxWrites)
	// TODO: Get rid of this retry by caching deletes locally.
	assert.LessOrEqual(t, stats.TxRetries, 2)
}

func TestReadFromAnother(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db1 := initDB(t, back, clock)
	db2 := initDB(t, back, clock)

	coll := []byte("rw-another")
	key := []byte("key1")
	val := []byte("value1")

	// Write from the first DB.
	err := db1.Collection(coll).Create(ctx)
	assert.NoError(t, err)
	err = db1.Collection(coll).Write(ctx, key, val)
	assert.NoError(t, err)

	// Read from the second.
	buf, err := db2.Collection(coll).ReadStrong(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, string(val), string(buf))
}

func TestReadDeletedFromAnother(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db1 := initDB(t, back, clock)
	db2 := initDB(t, back, clock)

	coll := []byte("rw-delete-another")
	key1 := []byte("key1")
	key2 := []byte("key2")
	val := []byte("value1")
	newval := []byte("value1-modified")

	// Write from the first DB.
	db1coll := db1.Collection(coll)
	err := db1coll.Create(ctx)
	assert.NoError(t, err)

	err = db1.Tx(ctx, func(tx *glassdb.Tx) error {
		if err := tx.Write(db1coll, key1, val); err != nil {
			return err
		}
		return tx.Write(db1coll, key2, val)
	})
	assert.NoError(t, err)

	// Delete one value and change the other from the second DB.
	db2coll := db2.Collection(coll)
	err = db2.Tx(ctx, func(tx *glassdb.Tx) error {
		if err := tx.Write(db2coll, key1, newval); err != nil {
			return err
		}
		return tx.Delete(db2coll, key2)
	})
	assert.NoError(t, err)

	// Check the results from the first DB.
	var key1Read []byte
	key2Found := true
	err = db1.Tx(ctx, func(tx *glassdb.Tx) error {
		key1Read, err = tx.Read(db1coll, key1)
		if err != nil {
			return err
		}
		_, err := tx.Read(db1coll, key2)
		if err == nil {
			key2Found = true
			return nil
		}
		if errors.Is(err, backend.ErrNotFound) {
			key2Found = false
			return nil
		}
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, string(newval), string(key1Read))
	assert.False(t, key2Found)

	// Check stats.
	stats := db1.Stats()
	assert.Equal(t, 2, stats.TxN)
	assert.Equal(t, 2, stats.TxWrites)
	assert.LessOrEqual(t, 4, stats.TxReads)
	assert.LessOrEqual(t, 1, stats.TxRetries)
}

func readInt(b []byte) int64 {
	res, _ := binary.Varint(b)
	return res
}

func writeInt(num int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, num)
	return buf[:n]
}

func readIntFromT(tx *glassdb.Tx, c glassdb.Collection, k []byte) (int64, error) {
	val, err := tx.Read(c, k)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			// Assume zero if the value was not found
			// (i.e. initialize it).
			return 0, nil
		}
		return 0, err
	}
	return readInt(val), nil
}

func rmw(ctx context.Context, db *glassdb.DB, coll glassdb.Collection, key []byte) error {
	for i := 0; i < 30; i++ {
		// This is to make sure we catch deadlocks.
		ctx, cancel := context.WithTimeout(ctx, txTimeout)
		defer cancel()

		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			// Increment the value and write it back.
			num, err := readIntFromT(tx, coll, key)
			if err != nil {
				return err
			}
			return tx.Write(coll, key, writeInt(num+1))
		})
		if err != nil {
			return fmt.Errorf("iteration #%d: %w", i, err)
		}
	}

	return nil
}

func TestRMW(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)

	coll := []byte("rmw-c")
	key := []byte("key")

	// Initialize the collection.
	err := db.Collection(coll).Create(ctx)
	assert.NoError(t, err)

	err = rmw(ctx, db, db.Collection(coll), key)
	assert.NoError(t, err)

	// Check stats.
	stats := db.Stats()
	assert.Equal(t, 30, stats.TxN)
	assert.Equal(t, 30, stats.TxReads)
	assert.Equal(t, 30, stats.TxWrites)
	// Make sure there are no retries in this case.
	assert.Equal(t, 0, stats.TxRetries)
	// Only one read, because the rest were cached.
	// It may be that we have a second read, because the first transaction
	// unlock is done asynchronously w.r.t. the second transaction.
	assert.LessOrEqual(t, stats.ObjReadN, 2)

	// Read the final result.
	val, err := db.Collection(coll).ReadStrong(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(30), readInt(val))
}

func TestConcurrentRMW(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db1 := initDB(t, back, clock)
	db2 := initDB(t, back, clock)

	coll := []byte("rmw-c")
	key := []byte("key")

	// Initialize the collection.
	err := db1.Collection(coll).Create(ctx)
	assert.NoError(t, err)

	g := errgroup.Group{}
	g.Go(func() error {
		if err := rmw(ctx, db1, db1.Collection(coll), key); err != nil {
			return fmt.Errorf("DB1: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := rmw(ctx, db2, db2.Collection(coll), key); err != nil {
			return fmt.Errorf("DB2: %w", err)
		}
		return nil
	})

	err = g.Wait()
	assert.NoError(t, err)

	// Read the final result.
	val, err := db2.Collection(coll).ReadStrong(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(60), readInt(val))
}

func multipleRMW(
	ctx context.Context,
	db *glassdb.DB,
	coll glassdb.Collection,
	key1, key2 []byte,
) error {

	for i := 0; i < 30; i++ {
		// This is to make sure we catch deadlocks.
		ctx, cancel := context.WithTimeout(ctx, txTimeout)
		defer cancel()

		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			// Increment key1
			num, err := readIntFromT(tx, coll, key1)
			if err != nil {
				return err
			}
			if err := tx.Write(coll, key1, writeInt(num+1)); err != nil {
				return err
			}

			// Increment key2
			num, err = readIntFromT(tx, coll, key2)
			if err != nil {
				return err
			}
			return tx.Write(coll, key2, writeInt(num+1))
		})
		if err != nil {
			return fmt.Errorf("iteration #%d: %w", i, err)
		}
	}

	return nil
}

func TestMultipleRMW(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)

	coll := []byte("multiple-rmw-c")
	key1 := []byte("key1")
	key2 := []byte("key2")

	// Initialize.
	err := db.Collection(coll).Create(ctx)
	assert.NoError(t, err)

	err = multipleRMW(ctx, db, db.Collection(coll), key1, key2)
	assert.NoError(t, err)

	// Read the final result.
	val, err := db.Collection(coll).ReadStrong(ctx, key1)
	require.NoError(t, err)
	assert.Equal(t, int64(30), readInt(val))

	assert.Equal(t, 31, db.Stats().TxN)
	// Make sure there are no retries in this case.
	assert.Equal(t, 0, db.Stats().TxRetries)
}

func TestReadMulti(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	keys := make([]glassdb.FQKey, 15)
	for i := 0; i < len(keys); i++ {
		keys[i] = glassdb.FQKey{
			Collection: coll,
			Key:        []byte(fmt.Sprintf("key%d", i)),
		}
	}

	// Initialize the values.
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, k := range keys {
			_ = tx.Write(coll, k.Key, writeInt(0))
		}
		return nil
	})
	assert.NoError(t, err)

	// Read and write.
	for i := 0; i < 30; i++ {
		err = db.Tx(ctx, func(tx *glassdb.Tx) error {
			res := tx.ReadMulti(keys)
			for i, r := range res {
				if r.Err != nil {
					return err
				}
				err := tx.Write(keys[i].Collection, keys[i].Key, writeInt(readInt(r.Value)+1))
				if err != nil {
					return err
				}
			}
			return nil
		})
		assert.NoError(t, err)
	}

	assert.Equal(t, 31, db.Stats().TxN)
	assert.Equal(t, 0, db.Stats().TxRetries)

	for i := 0; i < len(keys); i++ {
		b, err := coll.ReadStrong(ctx, keys[i].Key)
		assert.NoError(t, err)
		assert.Equal(t, int64(30), readInt(b))
	}
}

func TestConcurrentMultipleRMW(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db1 := initDB(t, back, clock)
	db2 := initDB(t, back, clock)

	coll := []byte("rmw-c")
	key1 := []byte("key1")
	key2 := []byte("key2")

	// Initialize.
	err := db1.Collection(coll).Create(ctx)
	assert.NoError(t, err)

	g := errgroup.Group{}
	g.Go(func() error {
		if err := multipleRMW(ctx, db1, db1.Collection(coll), key1, key2); err != nil {
			return fmt.Errorf("DB1: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := multipleRMW(ctx, db2, db2.Collection(coll), key1, key2); err != nil {
			return fmt.Errorf("DB2: %w", err)
		}
		return nil
	})

	err = g.Wait()
	assert.NoError(t, err)

	// Read the final result.
	val, err := db2.Collection(coll).ReadStrong(ctx, key1)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), readInt(val))
	val, err = db2.Collection(coll).ReadStrong(ctx, key2)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), readInt(val))
}

func TestReadWeak(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	// This test relies on timing. Relax the speed a bit to allow for slower
	// machines to complete correctly.
	clock := testkit.NewAcceleratedClock(clockMultiplier / 10)
	db := initDB(t, back, clock)

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)
	key := []byte("key")

	for i := 0; i < 30; i++ {
		// This is to make sure we catch deadlocks.
		ctx, cancel := context.WithTimeout(ctx, txTimeout)
		defer cancel()

		// Increment the value.
		err = db.Tx(ctx, func(tx *glassdb.Tx) error {
			// Do a read just to avoid making this a blind write.
			// We don't really need the value, because we can use 'i'.
			_, _ = readIntFromT(tx, coll, key)
			return tx.Write(coll, key, writeInt(int64(i)))
		})
		assert.NoError(t, err)

		// Weak read.
		val, err := coll.ReadWeak(ctx, key, 3*time.Second)
		assert.NoError(t, err)
		readNum := readInt(val)
		// This can be max 3 seconds behind, which means i - 3.
		assert.LessOrEqual(t, readNum, int64(i))
		if i >= 3 {
			assert.GreaterOrEqual(t, readNum, int64(i-3))
		}

		// Let some time pass.
		clock.Sleep(time.Second)
	}

	stats := db.Stats()
	assert.Equal(t, 30, stats.TxN)
	assert.Equal(t, 0, stats.TxRetries)
}

func TestListKeys(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	// Generate a random but deterministic set of keys.
	rnd := rand.New(rand.NewSource(424242)) // #nosec
	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = make([]byte, 4)
		rnd.Read(keys[i])
	}

	// Write all the keys into the collection.
	testVal := []byte("val")
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, k := range keys {
			_ = tx.Write(coll, k, testVal)
		}
		return nil
	})
	assert.NoError(t, err)

	// Iterate and collect the keys.
	var gotKeys []string
	iter, err := coll.Keys(ctx)
	require.NoError(t, err)
	for k, ok := iter.Next(); ok; k, ok = iter.Next() {
		gotKeys = append(gotKeys, string(k))
	}
	assert.NoError(t, iter.Err())

	// Make sure all of them are there.
	assert.Len(t, gotKeys, len(keys))
	gotSet := stringset.New(gotKeys...)
	for _, k := range keys {
		if !gotSet.Has(string(k)) {
			t.Errorf("Expected to iterate through key %v, but didn't", k)
		}
	}

	// Make sure they are sorted.
	assert.True(t, sort.SliceIsSorted(gotKeys, func(i, j int) bool {
		return gotKeys[i] < gotKeys[j]
	}))

	// Check statistics.
	stats := db.Stats()
	assert.Equal(t, 101, stats.ObjListN)
}

func TestListCollections(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	// Generate a random but deterministic set of collections.
	rnd := rand.New(rand.NewSource(424242)) // #nosec
	colls := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		colls[i] = make([]byte, 4)
		rnd.Read(colls[i])
	}

	// Create all the collections.
	for _, c := range colls {
		err = coll.Collection(c).Create(ctx)
		assert.NoError(t, err)
	}
	// Create one key per collection.
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, c := range colls {
			_ = tx.Write(coll.Collection(c), []byte("key"), c)
		}
		return nil
	})
	assert.NoError(t, err)

	// Iterate and collect the collections.
	var gotCollNames []string
	iter, err := coll.Collections(ctx)
	require.NoError(t, err)
	for c, ok := iter.Next(); ok; c, ok = iter.Next() {
		gotCollNames = append(gotCollNames, string(c))
	}
	assert.NoError(t, iter.Err())

	// Make sure all of them are there.
	assert.Len(t, gotCollNames, len(colls))
	gotSet := stringset.New(gotCollNames...)
	for _, c := range colls {
		if !gotSet.Has(string(c)) {
			t.Errorf("Expected to iterate through collection %v, but didn't", c)
		}
	}

	// Make sure they are sorted.
	assert.True(t, sort.SliceIsSorted(gotCollNames, func(i, j int) bool {
		return gotCollNames[i] < gotCollNames[j]
	}))

	// Make sure every collection has the key.
	values := make(map[string]string)
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, c := range colls {
			val, err := tx.Read(coll.Collection(c), []byte("key"))
			if err != nil {
				return err
			}
			values[string(c)] = string(val)
		}
		return nil
	})
	assert.NoError(t, err)

	assert.Len(t, values, len(gotCollNames))
	for k, v := range values {
		assert.Equal(t, k, v)
	}

	// Check statistics.
	stats := db.Stats()
	assert.Equal(t, 101, stats.ObjListN)
}

func TestReadonly(t *testing.T) {
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db := initDB(t, back, clock)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	coll := db.Collection([]byte("demo-coll"))
	err := coll.Create(ctx)
	assert.NoError(t, err)

	keys := make([][]byte, 15)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}

	// Initialize the values.
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for _, k := range keys {
			_ = tx.Write(coll, k, writeInt(0))
		}
		return nil
	})
	assert.NoError(t, err)

	// Concurrently update the values.
	g := errgroup.Group{}
	g.Go(func() error {
		for i := 0; i < 30; i++ {
			// This is to make sure we catch deadlocks.
			ctx, cancel := context.WithTimeout(ctx, txTimeout)
			defer cancel()

			err := db.Tx(ctx, func(tx *glassdb.Tx) error {
				for _, k := range keys {
					num, err := readIntFromT(tx, coll, k)
					if err != nil {
						return err
					}
					if err := tx.Write(coll, k, writeInt(num+1)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Readonly transactions.
	for i := 0; i < 30; i++ {
		// This is to make sure we catch deadlocks.
		ctx, cancel := context.WithTimeout(ctx, txTimeout)
		defer cancel()

		reads := make([]int64, len(keys))
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			for j, k := range keys {
				num, err := readIntFromT(tx, coll, k)
				if err != nil {
					return err
				}
				reads[j] = num
			}
			return nil
		})
		assert.NoError(t, err)
		requireAllEqual(t, keys, reads)
	}

	// Wait for the updates to finish.
	err = g.Wait()
	assert.NoError(t, err)
}

func TestStaleAbort(t *testing.T) {
	ctx := context.Background()
	back := initGCSBackend(t)
	clock := testkit.NewAcceleratedClock(clockMultiplier)
	db1 := initDB(t, back, clock)
	db2 := initDB(t, back, clock)

	key1 := []byte("key1")
	key2 := []byte("key2")

	// Initialize they keys.
	coll1 := db1.Collection([]byte("demo-coll"))
	err := coll1.Create(ctx)
	assert.NoError(t, err)
	err = coll1.Write(ctx, key1, []byte("v1"))
	assert.NoError(t, err)
	err = coll1.Write(ctx, key2, []byte("v1"))
	assert.NoError(t, err)

	// Change the keys from another DB.
	// Changing both keys increases the chance for the next transaction to read
	// a stale version.
	coll2 := db2.Collection([]byte("demo-coll"))
	err = db2.Tx(ctx, func(tx *glassdb.Tx) error {
		if err := tx.Write(coll2, key1, []byte("v2")); err != nil {
			return err
		}
		return tx.Write(coll2, key2, []byte("v2"))
	})
	assert.NoError(t, err)

	errStale := errors.New("read a stale version")

	// Now, DB1 shouldn't know already about the updated version.
	// This means that the first read will be stale. If the transaction
	// is not robust to retries, we would get spurious aborts.
	err = db1.Tx(ctx, func(tx *glassdb.Tx) error {
		r, err := tx.Read(coll1, key1)
		if err != nil {
			return err
		}
		if string(r) != "v2" {
			// If this error is bubbled up outside the transaction, it would
			// mean that the algorithm takes spurious failures (i.e. the ones
			// from stale executions) seriously.
			return errStale
		}
		// Update the second key.
		return tx.Write(coll1, key2, []byte("v3"))
	})
	assert.NoError(t, err)

	// Do the same test with DB2, but use the "immediate abort" error instead.
	err = db2.Tx(ctx, func(tx *glassdb.Tx) error {
		r, err := tx.Read(coll2, key2)
		if err != nil {
			return err
		}
		if string(r) != "v3" {
			// Even though this is a spurios failure, we are using a special
			// error that skips the validation and aborts immediately.
			return tx.Abort()
		}
		return nil
	})
	assert.ErrorIs(t, err, glassdb.ErrAborted)
}

func requireAllEqual(t *testing.T, ks [][]byte, vals []int64) {
	t.Helper()
	unique := map[int64]string{}
	for i, a := range vals {
		unique[a] = string(ks[i])
	}
	if len(unique) <= 1 {
		return
	}
	var (
		vs []int64
		ps []string
	)
	for v, p := range unique {
		vs = append(vs, v)
		ps = append(ps, p)
	}
	t.Fatalf("for paths %v, got distinct elements: %v, expected all to be equal", ps, vs)
}
