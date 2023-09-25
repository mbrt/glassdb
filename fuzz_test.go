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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/internal/testkit/simulator"
)

func deterministicRandBytes(n int) []byte {
	r := rand.New(rand.NewSource(42))
	res := make([]byte, n)
	_, err := r.Read(res)
	if err != nil {
		panic(err)
	}
	return res
}

func selectDB(dbs []*glassdb.DB, second bool) *glassdb.DB {
	if second {
		return dbs[1]
	}
	return dbs[0]
}

func FuzzReadWhileWrite(f *testing.F) {
	rnd := deterministicRandBytes(1024 * 3)
	f.Add(false, false, int8(1), rnd[:1024])
	f.Add(false, true, int8(2), rnd[1024:2048])
	f.Add(true, false, int8(2), rnd[2048:])

	f.Fuzz(func(t *testing.T, rdb2, wdb2 bool, numKeys int8, rnd []byte) {
		// The test should never last more than 500 ms in real time.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		sim := simulator.New(t, rnd)
		dbs := []*glassdb.DB{
			sim.DBInstance(), sim.DBInstance(),
		}

		// This will always run before everything else.
		initDB := dbs[0]
		coll := initDB.Collection([]byte("simc"))
		coll.Create(context.Background())

		keys := make([]glassdb.FQKey, int(numKeys))
		for i := 0; i < int(numKeys); i++ {
			keys[i] = glassdb.FQKey{
				Collection: coll,
				Key:        []byte(fmt.Sprintf("key%d", i)),
			}
		}

		// These will run in parallel pseudo-deterministically.
		sim.Run(ctx, "init-tx", dbs[0], func(ctx context.Context, db *glassdb.DB) error {
			return db.Tx(ctx, func(tx *glassdb.Tx) error {
				for _, k := range keys {
					tx.Write(k.Collection, k.Key, []byte{0})
				}
				return nil
			})
		})
		sim.Run(ctx, "rtx", selectDB(dbs, rdb2), func(ctx context.Context, db *glassdb.DB) error {
			return db.Tx(ctx, func(tx *glassdb.Tx) error {
				res := tx.ReadMulti(keys)
				for _, r := range res {
					if r.Err != nil {
						return r.Err
					}
				}
				return nil
			})
		})
		sim.Run(ctx, "w-tx", selectDB(dbs, wdb2), func(ctx context.Context, db *glassdb.DB) error {
			return db.Tx(ctx, func(tx *glassdb.Tx) error {
				res := tx.ReadMulti(keys)
				for i, r := range res {
					if r.Err != nil {
						return r.Err
					}
					if err := tx.Write(keys[i].Collection, keys[i].Key, []byte{1}); err != nil {
						return err
					}
				}
				return nil
			})
		})
		sim.Wait()
		if err := sim.Verify(keys); err != nil {
			f.Failed()
		}
	})
}
