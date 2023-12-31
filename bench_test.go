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
	"errors"
	"flag"
	"fmt"
	"testing"

	"github.com/sourcegraph/conc"
	"github.com/stretchr/testify/require"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/testkit"
)

var printStats = flag.Bool("print-stats", false, "print DB stats after benchmarking")

func BenchmarkSingleRMW(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := initDB(b, tb.B, clock)
			collName := []byte("single-rmw")
			key := []byte("key")

			// Initialize.
			coll := db.Collection(collName)
			err := coll.Create(ctx)
			require.NoError(b, err)

			istat := db.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Tx(ctx, func(tx *glassdb.Tx) error {
					num, err := readIntFromT(tx, coll, key)
					if err != nil {
						return err
					}
					return tx.Write(coll, key, writeInt(num+1))
				})
				require.NoError(b, err)
			}
			b.StopTimer()

			benchStats(b, db.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db.Stats())
			}
		})
	}
}

func Benchmark10RMW(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := initDB(b, tb.B, clock)
			collName := []byte("rmw-mb")

			// Initialize.
			coll := db.Collection(collName)
			err := coll.Create(ctx)
			require.NoError(b, err)

			keys := make([]glassdb.FQKey, 10)
			for i := 0; i < 10; i++ {
				keys[i] = glassdb.FQKey{
					Collection: coll,
					Key:        []byte(fmt.Sprintf("key%d", i)),
				}
			}

			istat := db.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Tx(ctx, func(tx *glassdb.Tx) error {
					// Increment all the keys together in the transaction.
					res := tx.ReadMulti(keys)
					for i, rv := range res {
						var val int64
						if rv.Err != nil && !errors.Is(err, backend.ErrNotFound) {
							return err
						}
						val = readInt(rv.Value)
						if err := tx.Write(coll, keys[i].Key, writeInt(val+1)); err != nil {
							return err
						}
					}
					return nil
				})
				require.NoError(b, err)
			}
			b.StopTimer()

			benchStats(b, db.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db.Stats())
			}
		})
	}

}

func BenchmarkConcurrMultipleRMW(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db1 := initDB(b, tb.B, clock)
			db2 := initDB(b, tb.B, clock)

			collName := []byte("rmw-b")
			key1 := []byte("key1")
			key2 := []byte("key2")

			// Initialize.
			err := db1.Collection(collName).Create(ctx)
			require.NoError(b, err)

			updateF := func(db *glassdb.DB, c glassdb.Collection) error {
				return db.Tx(ctx, func(tx *glassdb.Tx) error {
					num, err := readIntFromT(tx, c, key1)
					if err != nil {
						return err
					}
					if err := tx.Write(c, key1, writeInt(num+1)); err != nil {
						return err
					}
					num, err = readIntFromT(tx, c, key2)
					if err != nil {
						return err
					}
					return tx.Write(c, key2, writeInt(num+1))
				})
			}

			var wg conc.WaitGroup
			wg.Go(func() {
				for ctx.Err() == nil {
					// We need to ignore context cancel errors, because we
					// rely on that to stop the test.
					if err := updateF(db1, db1.Collection(collName)); err != nil && ctx.Err() == nil {
						b.Fatal(b, err)
					}
				}
			})

			istat := db2.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := updateF(db2, db2.Collection(collName))
				require.NoError(b, err)
			}
			b.StopTimer()

			cancel()
			wg.Wait()

			benchStats(b, db2.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db2.Stats())
			}
		})
	}
}

func Benchmark10R(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := initDB(b, tb.B, clock)
			collName := []byte("rmw-mb")

			// Initialize.
			err := db.Collection(collName).Create(ctx)
			coll := db.Collection(collName)
			require.NoError(b, err)

			keys := make([]glassdb.FQKey, 10)
			for i := 0; i < 10; i++ {
				keys[i] = glassdb.FQKey{
					Collection: coll,
					Key:        []byte(fmt.Sprintf("key%d", i)),
				}
			}

			// Write the values.
			err = db.Tx(ctx, func(tx *glassdb.Tx) error {
				for i, k := range keys {
					if err := tx.Write(coll, k.Key, writeInt(int64(i))); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(b, err)

			istat := db.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.Tx(ctx, func(tx *glassdb.Tx) error {
					res := tx.ReadMulti(keys)
					for _, rv := range res {
						if rv.Err != nil && !errors.Is(err, backend.ErrNotFound) {
							return err
						}
					}
					return nil
				})
			}
			b.StopTimer()

			benchStats(b, db.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db.Stats())
			}
		})
	}
}

func BenchmarkSharedR(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := initDB(b, tb.B, clock)
			keyR := []byte("key-r")
			keyW1 := []byte("key-w1")
			keyW2 := []byte("key-w2")

			// Initialize.
			coll := db.Collection([]byte("shr-b"))
			err := coll.Create(ctx)
			require.NoError(b, err)

			err = db.Tx(ctx, func(tx *glassdb.Tx) error {
				_ = tx.Write(coll, keyR, writeInt(1))
				_ = tx.Write(coll, keyW1, writeInt(0))
				_ = tx.Write(coll, keyW2, writeInt(0))
				return nil
			})
			require.NoError(b, err)

			updateF := func(keyW []byte) error {
				return db.Tx(ctx, func(tx *glassdb.Tx) error {
					num, err := readIntFromT(tx, coll, keyR)
					if err != nil {
						return err
					}
					return tx.Write(coll, keyW, writeInt(num+1))
				})
			}

			var wg conc.WaitGroup
			wg.Go(func() {
				for ctx.Err() == nil {
					// We need to ignore context cancel errors, because we
					// rely on that to stop the test.
					if err := updateF(keyW2); err != nil && ctx.Err() == nil {
						b.Fatal(err)
					}
				}
			})

			istat := db.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := updateF(keyW1)
				require.NoError(b, err)
			}
			b.StopTimer()

			cancel()
			wg.Wait()

			benchStats(b, db.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db.Stats())
			}
		})
	}
}

func Benchmark100W(b *testing.B) {
	clock := testkit.NewAcceleratedClock(clockMultiplier)

	for _, tb := range allBackends(b, clock) {
		b.Run(tb.Name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			db := initDB(b, tb.B, clock)
			collName := []byte("mw")

			// Initialize.
			err := db.Collection(collName).Create(ctx)
			coll := db.Collection(collName)
			require.NoError(b, err)

			istat := db.Stats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Tx(ctx, func(tx *glassdb.Tx) error {
					// Increment all the keys together in the transaction.
					for j := 0; j < 100; j++ {
						k := fmt.Sprintf("k%d", i*100+j)
						if err := tx.Write(coll, []byte(k), writeInt(int64(j))); err != nil {
							return err
						}
					}
					return nil
				})
				require.NoError(b, err)
			}
			b.StopTimer()

			benchStats(b, db.Stats().Sub(istat))
			if b.N > 1 && *printStats {
				b.Logf("Stats: %+v", db.Stats())
			}
		})
	}
}

func benchStats(b *testing.B, stats glassdb.Stats) {
	b.ReportMetric(float64(stats.TxTime)/float64(b.N), "txns/op")
	b.ReportMetric(float64(stats.TxRetries)/float64(b.N), "retries/op")
	b.ReportMetric(float64(stats.ObjWrites)/float64(b.N), "w/op")
	b.ReportMetric(float64(stats.ObjReads)/float64(b.N), "r/op")
	b.ReportMetric(float64(stats.MetaWrites)/float64(b.N), "metaw/op")
	b.ReportMetric(float64(stats.MetaReads)/float64(b.N), "metar/op")
}
