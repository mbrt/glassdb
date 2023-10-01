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

package simulator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/internal/data"
)

func TestBase(t *testing.T) {
	ctx := context.Background()
	// The first sleeps more than the second (10 vs 5 ms).
	sim := New(t, []byte{1, 1})
	ch := make(chan int, 2)
	sim.Run(ctx, "one", nil, func(ctx context.Context, d *glassdb.DB) error {
		ch <- 0
		return nil
	})
	sim.Run(ctx, "two", nil, func(ctx context.Context, d *glassdb.DB) error {
		ch <- 1
		return nil
	})
	err := sim.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 0}, []int{<-ch, <-ch})
}

func TestDeterministicTxID(t *testing.T) {
	ctx := context.Background()
	sim := New(t, []byte{0})
	db := sim.DBInstance()

	err := sim.Init(ctx, func(ctx context.Context, d *glassdb.DB) error {
		coll := db.Collection([]byte("foo"))
		return coll.Create(ctx)
	})
	assert.NoError(t, err)

	sim.Run(ctx, "tx-id", db, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection([]byte("foo"))
		coll.Write(ctx, []byte("key"), []byte("val"))
		return nil
	})
	err = sim.Wait()
	assert.NoError(t, err)

	txs := sim.backend.CommitOrder()
	assert.Equal(t, []data.TxID{data.TxID([]byte("tx-id"))}, txs)
}

func TestVerify(t *testing.T) {
	ctx := context.Background()
	sim := New(t, []byte{0})
	collName := []byte("coll")

	err := sim.Init(ctx, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection(collName)
		return coll.Create(ctx)
	})
	assert.NoError(t, err)

	testDB := sim.DBInstance()
	sim.Run(ctx, "tx0", testDB, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection(collName)
		coll.Write(ctx, []byte("key1"), []byte("val"))
		return nil
	})
	sim.Run(ctx, "tx1", testDB, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection(collName)
		coll.Write(ctx, []byte("key2"), []byte("val"))
		return nil
	})
	err = sim.Wait()
	assert.NoError(t, err)

	err = sim.Verify(ctx, []CollectionKey{
		{Collection: collName, Key: []byte("key1")},
		{Collection: collName, Key: []byte("key2")},
	})
	assert.NoError(t, err)
}

func TestVerifyRead(t *testing.T) {
	ctx := context.Background()
	sim := New(t, []byte{0})
	collName := []byte("coll")

	err := sim.Init(ctx, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection(collName)
		if err := coll.Create(ctx); err != nil {
			return err
		}
		return coll.Write(ctx, []byte("key"), []byte("val"))
	})
	assert.NoError(t, err)

	testDB := sim.DBInstance()
	sim.Run(ctx, "tx-r", testDB, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection(collName)
		val, err := coll.ReadStrong(ctx, []byte("key"))
		if err != nil {
			return err
		}
		sim.NotifyReadValue(ctx, CollectionKey{Collection: collName, Key: []byte("key")}, val)
		return nil
	})
	err = sim.Wait()
	assert.NoError(t, err)

	err = sim.Verify(ctx, []CollectionKey{
		{Collection: collName, Key: []byte("key")},
	})
	assert.NoError(t, err)
}

func TestDB(t *testing.T) {
	// The test should never last more than 100 milliseconds in real time.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	sim := New(t, []byte{0})
	db := sim.DBInstance()
	err := sim.Init(ctx, func(ctx context.Context, d *glassdb.DB) error {
		coll := db.Collection([]byte("foo"))
		return coll.Create(ctx)
	})
	assert.NoError(t, err)

	sim.Run(ctx, "one", db, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection([]byte("foo"))
		if err := coll.Write(ctx, []byte("key"), []byte("val")); err != nil {
			return err
		}
		rv, err := coll.ReadStrong(ctx, []byte("key"))
		if err != nil {
			return err
		}
		if string(rv) != "val" {
			return fmt.Errorf("unexpected read: %q", string(rv))
		}
		return nil
	})
	err = sim.Wait()

	// TODO: Better check for more specific errors.
	assert.NotErrorIs(t, err, context.Canceled)
}
