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

// Collection represents a named group of key-value pairs within a database.
type Collection struct {
	prefix string
	global storage.Global
	local  storage.Local
	algo   trans.Algo
	db     *DB
}

// ReadStrong reads the value for key with strong consistency guarantees.
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

// ReadWeak reads the value for key allowing stale results up to maxStaleness.
func (c Collection) ReadWeak(
	ctx context.Context,
	key []byte,
	maxStaleness time.Duration,
) ([]byte, error) {
	p := paths.FromKey(c.prefix, key)
	r := trans.NewReader(c.local, c.global, c.db.tmon)
	rv, err := r.Read(ctx, p, maxStaleness)
	return rv.Value, err
}

func (c Collection) Write(ctx context.Context, key, value []byte) error {
	return c.db.Tx(ctx, func(tx *Tx) error {
		return tx.Write(c, key, value)
	})
}

// Delete removes the value associated with key within a transaction.
func (c Collection) Delete(ctx context.Context, key []byte) error {
	return c.db.Tx(ctx, func(tx *Tx) error {
		return tx.Delete(c, key)
	})
}

// Update atomically reads the value for key, applies f, and writes the result.
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

// Collection returns a sub-collection with the given name.
func (c Collection) Collection(name []byte) Collection {
	p := paths.FromCollection(c.prefix, name)
	return c.db.openCollection(p)
}

// Create ensures the collection exists in the backend, creating it if necessary.
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

// Keys returns an iterator over the keys in the collection.
func (c Collection) Keys(ctx context.Context) (*KeysIter, error) {
	keysPrefix := paths.KeysPrefix(c.prefix)
	iter, err := c.db.backend.List(ctx, keysPrefix)
	if err != nil {
		return nil, err
	}
	return &KeysIter{inner: iter}, nil
}

// Collections returns an iterator over the sub-collections in this collection.
func (c Collection) Collections(ctx context.Context) (*CollectionsIter, error) {
	cprefix := paths.CollectionsPrefix(c.prefix)
	iter, err := c.db.backend.List(ctx, cprefix)
	if err != nil {
		return nil, err
	}
	return &CollectionsIter{inner: iter}, nil
}
