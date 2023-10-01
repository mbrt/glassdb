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

	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/internal/testkit/simulator"
)

func deterministicRandBytes(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
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
	f.Add(false, false, uint8(1), deterministicRandBytes(42, 1024))
	f.Add(false, true, uint8(2), deterministicRandBytes(3, 1024))
	f.Add(true, false, uint8(2), deterministicRandBytes(666, 1024))

	f.Fuzz(func(t *testing.T, rdb2, wdb2 bool, numKeys uint8, rnd []byte) {
		// The test should never last more than 500 ms in real time.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		sim := simulator.New(t, rnd)
		dbs := []*glassdb.DB{
			sim.DBInstance(), sim.DBInstance(),
		}
		collName := []byte("simc")
		keyNames := make([]simulator.CollectionKey, int(numKeys))
		for i := 0; i < int(numKeys); i++ {
			keyNames[i] = simulator.CollectionKey{
				Collection: collName,
				Key:        []byte(fmt.Sprintf("key%d", i)),
			}
		}

		toFQKey := func(db *glassdb.DB) []glassdb.FQKey {
			res := make([]glassdb.FQKey, len(keyNames))
			for i := 0; i < int(numKeys); i++ {
				res[i] = glassdb.FQKey{
					Collection: db.Collection(keyNames[i].Collection),
					Key:        keyNames[i].Key,
				}
			}
			return res
		}

		// This will always run before everything else.
		err := sim.Init(ctx, func(ctx context.Context, db *glassdb.DB) error {
			coll := db.Collection(collName)
			return coll.Create(context.Background())
		})
		assert.NoError(t, err)

		// These will run in parallel pseudo-deterministically.
		sim.Run(ctx, "init-tx", dbs[0], func(ctx context.Context, db *glassdb.DB) error {
			return db.Tx(ctx, func(tx *glassdb.Tx) error {
				coll := db.Collection(collName)
				for _, k := range keyNames {
					tx.Write(coll, k.Key, []byte{0})
				}
				return nil
			})
		})
		sim.Run(ctx, "r-tx", selectDB(dbs, rdb2), func(ctx context.Context, db *glassdb.DB) error {
			keys := toFQKey(db)
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
			keys := toFQKey(db)
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
		_ = sim.Wait() // The error here doesn't matter.

		if err := sim.Verify(ctx, keyNames); err != nil {
			f.Failed()
		}
	})
}
