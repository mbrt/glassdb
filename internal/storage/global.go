// Package storage manages global and local storage layers with caching and
// version tracking.
package storage

import (
	"context"
	"fmt"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
)

// NewGlobal returns a Global that reads and writes through the given backend,
// using l as a local cache.
func NewGlobal(b backend.Backend, l Local) Global {
	return Global{
		backend: b,
		local:   l,
	}
}

// Global wraps a backend with a local cache, performing read-through and
// write-through caching of storage objects and metadata.
type Global struct {
	backend backend.Backend
	local   Local
}

func (s Global) Read(ctx context.Context, key string) (GlobalRead, error) {
	// If we have the object in local storage, read it only if it was modified.
	// Otherwise read without optimizations.
	if e, ok := s.local.Read(key, MaxStaleness); ok {
		// If this is a local override, or we are 100% sure the value is
		// outdated, it's better to do a regular read.
		if !e.Outdated && !e.Version.B.IsNull() {
			modified := true
			r, err := s.backend.ReadIfModified(ctx, key, backend.WriterID(e.Version.Writer))
			if err != nil {
				if !errors.Is(err, backend.ErrPrecondition) {
					return GlobalRead{}, fmt.Errorf("backend read of %q: %w", key, err)
				}
				modified = false
			}
			if modified {
				// The local value was stale. Take the updated value from the backend.
				meta := backend.Metadata{Tags: r.Tags, Version: r.Version}
				s.local.WriteWithMeta(key, r.Contents, meta)

				return GlobalRead{
					Value:   r.Contents,
					Version: VersionFromMeta(meta),
				}, nil
			}
			// The cached value is up to date, use that.
			return GlobalRead{
				Value:   e.Value,
				Version: e.Version,
			}, nil
		}
	}

	// No value in cache. Read directly.
	r, err := s.backend.Read(ctx, key)
	if err != nil {
		return GlobalRead{}, fmt.Errorf("backend read of %q: %w", key, err)
	}
	meta := backend.Metadata{Tags: r.Tags, Version: r.Version}
	s.local.WriteWithMeta(key, r.Contents, meta)

	return GlobalRead{
		Value:   r.Contents,
		Version: VersionFromMeta(meta),
	}, nil
}

// GetMetadata fetches the object metadata from the backend and updates the
// local cache.
func (s Global) GetMetadata(ctx context.Context, key string) (backend.Metadata, error) {
	meta, err := s.backend.GetMetadata(ctx, key)
	if err != nil {
		return backend.Metadata{}, err
	}
	s.local.SetMeta(key, meta)
	return meta, nil
}

// SetTagsIf conditionally sets tags on the object if its current version
// matches expected, and updates the local metadata cache.
func (s Global) SetTagsIf(
	ctx context.Context,
	key string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := s.backend.SetTagsIf(ctx, key, expected, t)
	if err != nil {
		return backend.Metadata{}, err
	}
	s.local.SetMeta(key, meta)
	return meta, nil
}

func (s Global) Write(
	ctx context.Context,
	key string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := s.backend.Write(ctx, key, value, t)
	if err != nil {
		return meta, err
	}
	s.local.WriteWithMeta(key, value, meta)
	return meta, nil
}

// WriteIf conditionally writes the value if the current version matches
// expected, and updates the local cache.
func (s Global) WriteIf(
	ctx context.Context,
	key string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := s.backend.WriteIf(ctx, key, value, expected, t)
	if err != nil {
		return backend.Metadata{}, err
	}
	s.local.WriteWithMeta(key, value, meta)
	return meta, nil
}

// WriteIfNotExists writes the value only if the object does not already exist,
// and updates the local cache on success.
func (s Global) WriteIfNotExists(
	ctx context.Context,
	key string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	meta, err := s.backend.WriteIfNotExists(ctx, key, value, t)
	if err != nil {
		return meta, err
	}
	s.local.WriteWithMeta(key, value, meta)
	return meta, nil
}

// Delete removes the object from the backend and the local cache.
func (s Global) Delete(ctx context.Context, key string) error {
	if err := s.backend.Delete(ctx, key); err != nil {
		return err
	}
	s.local.Delete(key)
	return nil
}

// DeleteIf conditionally deletes the object if its version matches expected,
// and removes it from the local cache on success.
func (s Global) DeleteIf(ctx context.Context, key string, expected backend.Version) error {
	if err := s.backend.DeleteIf(ctx, key, expected); err != nil {
		return err
	}
	s.local.Delete(key)
	return nil
}

// GlobalRead holds the result of reading a value from global storage.
type GlobalRead struct {
	Value   []byte
	Version Version
}

// Writer returns the transaction ID of the last writer of the value, as
// recorded in the metadata tags. Returns nil if the value has no recorded
// writer (e.g. an externally-created object).
func (r GlobalRead) Writer() data.TxID {
	return r.Version.Writer
}
