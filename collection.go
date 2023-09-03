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
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trans"
)

var collInfoContents = []byte("__collection__")

type Collection struct {
	prefix string
	global storage.Global
	local  storage.Local
	algo   trans.Algo
	db     *DB
}

func (c Collection) ReadStrong(ctx context.Context, key []byte) ([]byte, error) {
	var (
		res     []byte
		readErr error
	)

	err := c.db.Tx(ctx, func(tx *Tx) error {
		res, readErr = tx.Read(c, key)
		if !errors.Is(readErr, backend.ErrNotFound) {
			// We need to validate the transaction even when we don't find the
			// value. It's not enough to abort.
			return readErr
		}
		return nil
	})

	// If the transaction failed, return that error.
	// Otherwise, return the read error.
	if err != nil {
		return res, err
	}
	return res, readErr
}

func (c Collection) ReadWeak(
	ctx context.Context,
	key []byte,
	maxStaleness time.Duration,
) ([]byte, error) {
	p := paths.FromKey(c.prefix, key)
	lr, ok := c.local.Read(p, maxStaleness)
	if ok {
		return lr.Value, nil
	}
	// We don't have the value ready. We could read it from global storage,
	// but we would need to account for locked items. In case of lock create
	// we could read an empty object where instead it should be either not
	// found, or assume the first value.
	// Better to just use a readonly transaction.
	//
	// TODO: Use storage.Reader instead.
	return c.ReadStrong(ctx, key)
}

func (c Collection) Write(ctx context.Context, key, value []byte) error {
	return c.db.Tx(ctx, func(tx *Tx) error {
		return tx.Write(c, key, value)
	})
}

func (c Collection) Delete(ctx context.Context, key []byte) error {
	return c.db.Tx(ctx, func(tx *Tx) error {
		return tx.Delete(c, key)
	})
}

func (c Collection) Update(
	ctx context.Context,
	key []byte,
	f func(old []byte) ([]byte, error),
) ([]byte, error) {
	var newb []byte

	err := c.db.Tx(ctx, func(tx *Tx) error {
		old, err := tx.Read(c, key)
		if err != nil {
			return err
		}
		newb, err = f(old)
		if err != nil {
			return err
		}
		return tx.Write(c, key, newb)
	})

	return newb, err
}

func (c Collection) Collection(name []byte) Collection {
	p := paths.FromCollection(c.prefix, name)
	return c.db.openCollection(p)
}

func (c Collection) Create(ctx context.Context) error {
	p := paths.CollectionInfo(c.prefix)
	_, err := c.global.GetMetadata(ctx, p)
	if err == nil {
		// All good, the collection is there.
		return nil
	}
	if !errors.Is(err, backend.ErrNotFound) {
		// Some other error. Bail out.
		return err
	}
	// The collection is not there; create it.
	_, err = c.global.WriteIfNotExists(ctx, p, collInfoContents, nil)
	if errors.Is(err, backend.ErrPrecondition) {
		// This was created concurrently. Assume it's there.
		return nil
	}
	return err
}

func (c Collection) Keys(ctx context.Context) (*KeysIter, error) {
	keysPrefix := paths.KeysPrefix(c.prefix)
	iter, err := c.db.backend.List(ctx, keysPrefix)
	if err != nil {
		return nil, err
	}
	return &KeysIter{inner: iter}, nil
}

func (c Collection) Collections(ctx context.Context) (*CollectionsIter, error) {
	cprefix := paths.CollectionsPrefix(c.prefix)
	iter, err := c.db.backend.List(ctx, cprefix)
	if err != nil {
		return nil, err
	}
	return &CollectionsIter{inner: iter}, nil
}
