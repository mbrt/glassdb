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

package storage

import (
	"context"
	"fmt"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/errors"
)

func NewGlobal(b backend.Backend, l Local, clock clockwork.Clock) Global {
	return Global{
		backend: b,
		local:   l,
		clock:   clock,
	}
}

type Global struct {
	backend backend.Backend
	local   Local
	clock   clockwork.Clock
}

func (s Global) Read(ctx context.Context, key string) (GlobalRead, error) {
	// If we have the object in local storage, read it only if it was modified.
	// Otherwise read without optimizations.
	if e, ok := s.local.Read(key, MaxStaleness); ok {
		// If this is a local override, or we are 100% sure the value is
		// outdated, it's better to do a regular read.
		if !e.Outdated && !e.Version.B.IsNull() {
			modified := true
			r, err := s.backend.ReadIfModified(ctx, key, e.Version.B.Contents)
			if err != nil {
				if !errors.Is(err, backend.ErrPrecondition) {
					return GlobalRead{}, fmt.Errorf("backend read of %q: %w", key, err)
				}
				modified = false
			}
			if modified {
				// The local value was stale. Take the updated value from the backend.
				// Update local storage.
				s.local.Write(key, r.Contents, Version{B: r.Version})

				return GlobalRead{
					Value:   r.Contents,
					Version: r.Version.Contents,
				}, nil
			}
			// The cached value is up to date, use that.
			return GlobalRead{
				Value:   e.Value,
				Version: e.Version.B.Contents,
			}, nil
		}
	}

	// No value in cache. Read directly.
	r, err := s.backend.Read(ctx, key)
	if err != nil {
		return GlobalRead{}, fmt.Errorf("backend read of %q: %w", key, err)
	}
	// Update local storage.
	s.local.Write(key, r.Contents, Version{B: r.Version})

	return GlobalRead{
		Value:   r.Contents,
		Version: r.Version.Contents,
	}, nil
}

func (s Global) GetMetadata(ctx context.Context, key string) (backend.Metadata, error) {
	meta, err := s.backend.GetMetadata(ctx, key)
	if err != nil {
		return backend.Metadata{}, err
	}
	s.local.SetMeta(key, meta)
	return meta, nil
}

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

func (s Global) Delete(ctx context.Context, key string) error {
	if err := s.backend.Delete(ctx, key); err != nil {
		return err
	}
	s.local.Delete(key)
	return nil
}

func (s Global) DeleteIf(ctx context.Context, key string, expected backend.Version) error {
	if err := s.backend.DeleteIf(ctx, key, expected); err != nil {
		return err
	}
	s.local.Delete(key)
	return nil
}

type GlobalRead struct {
	Value   []byte
	Version int64
}
