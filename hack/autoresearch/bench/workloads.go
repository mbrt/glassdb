package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
)

// Fixed iteration counts. They are deliberately constant so the primary score
// stays comparable across runs and machines.
const (
	singleRMWTx   = 200
	multiRMWTx    = 100
	multiRMWKeys  = 10
	batchReadTx   = 200
	batchReadKeys = 10
	batchWriteTx  = 50
	batchWriteKB  = 100
	readRepeatTx  = 200
)

// workload is one measured scenario. setup runs unmeasured (collection
// creation, seeding); body runs exactly the measured transactions.
type workload struct {
	name  string
	setup func(ctx context.Context, db *glassdb.DB) (any, error)
	body  func(ctx context.Context, db *glassdb.DB, st any) error
}

func workloads() []workload {
	return []workload{
		{name: "singleRMW", setup: setupSingleRMW, body: bodySingleRMW},
		{name: "multiRMW10", setup: setupMultiRMW, body: bodyMultiRMW},
		{name: "batchRead10", setup: setupBatchRead, body: bodyBatchRead},
		{name: "batchWrite100", setup: setupBatchWrite, body: bodyBatchWrite},
		{name: "readRepeat", setup: setupReadRepeat, body: bodyReadRepeat},
	}
}

func createColl(ctx context.Context, db *glassdb.DB, name string) (glassdb.Collection, error) {
	coll := db.Collection([]byte(name))
	if err := coll.Create(ctx); err != nil {
		return glassdb.Collection{}, err
	}
	return coll, nil
}

func keysFor(coll glassdb.Collection, n int) []glassdb.FQKey {
	keys := make([]glassdb.FQKey, n)
	for i := range keys {
		keys[i] = glassdb.FQKey{Collection: coll, Key: fmt.Appendf(nil, "key%d", i)}
	}
	return keys
}

func setupSingleRMW(ctx context.Context, db *glassdb.DB) (any, error) {
	return createColl(ctx, db, "single")
}

func bodySingleRMW(ctx context.Context, db *glassdb.DB, st any) error {
	coll := st.(glassdb.Collection)
	key := []byte("k")
	for range singleRMWTx {
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			v, err := tx.Read(coll, key)
			if err != nil && !errors.Is(err, backend.ErrNotFound) {
				return err
			}
			return tx.Write(coll, key, writeInt(readInt(v)+1))
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func setupMultiRMW(ctx context.Context, db *glassdb.DB) (any, error) {
	coll, err := createColl(ctx, db, "multi")
	if err != nil {
		return nil, err
	}
	return keysFor(coll, multiRMWKeys), nil
}

func bodyMultiRMW(ctx context.Context, db *glassdb.DB, st any) error {
	keys := st.([]glassdb.FQKey)
	for range multiRMWTx {
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			res := tx.ReadMulti(keys)
			for i, rv := range res {
				if rv.Err != nil && !errors.Is(rv.Err, backend.ErrNotFound) {
					return rv.Err
				}
				if err := tx.Write(keys[i].Collection, keys[i].Key, writeInt(readInt(rv.Value)+1)); err != nil {
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
}

func setupBatchRead(ctx context.Context, db *glassdb.DB) (any, error) {
	coll, err := createColl(ctx, db, "bread")
	if err != nil {
		return nil, err
	}
	keys := keysFor(coll, batchReadKeys)
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		for i, k := range keys {
			if err := tx.Write(coll, k.Key, writeInt(int64(i))); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func bodyBatchRead(ctx context.Context, db *glassdb.DB, st any) error {
	keys := st.([]glassdb.FQKey)
	for range batchReadTx {
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			res := tx.ReadMulti(keys)
			for _, rv := range res {
				if rv.Err != nil {
					return rv.Err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func setupBatchWrite(ctx context.Context, db *glassdb.DB) (any, error) {
	return createColl(ctx, db, "bwrite")
}

func bodyBatchWrite(ctx context.Context, db *glassdb.DB, st any) error {
	coll := st.(glassdb.Collection)
	for i := range batchWriteTx {
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			for j := range batchWriteKB {
				k := fmt.Appendf(nil, "k%d", i*batchWriteKB+j)
				if err := tx.Write(coll, k, writeInt(int64(j))); err != nil {
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
}

func setupReadRepeat(ctx context.Context, db *glassdb.DB) (any, error) {
	coll, err := createColl(ctx, db, "rrepeat")
	if err != nil {
		return nil, err
	}
	key := []byte("k")
	err = db.Tx(ctx, func(tx *glassdb.Tx) error {
		return tx.Write(coll, key, writeInt(42))
	})
	if err != nil {
		return nil, err
	}
	return coll, nil
}

func bodyReadRepeat(ctx context.Context, db *glassdb.DB, st any) error {
	coll := st.(glassdb.Collection)
	key := []byte("k")
	for range readRepeatTx {
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			_, err := tx.Read(coll, key)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
