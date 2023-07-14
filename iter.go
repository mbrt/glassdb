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
	"fmt"
	"strings"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data/paths"
)

type KeysIter struct {
	inner  backend.ListIter
	prefix string
	err    error
}

func (it *KeysIter) Next() (key []byte, ok bool) {
	backendPath, ok := it.inner.Next()
	if !ok {
		return nil, false
	}

	if it.prefix == "" {
		r, err := paths.Parse(backendPath)
		if err != nil {
			it.err = fmt.Errorf("parsing path %q: %w", backendPath, err)
			return nil, false
		}
		if r.Type != paths.KeyType {
			it.err = fmt.Errorf("got path type %q, expected %q", r.Type, paths.KeyType)
			return nil, false
		}
		it.prefix = r.Prefix + "/"
	}
	trimmed := strings.TrimPrefix(backendPath, it.prefix)
	k, err := paths.ToKey(trimmed)
	if err != nil {
		it.err = err
		return nil, false
	}
	return k, true
}

func (it *KeysIter) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Err()
}

type CollectionsIter struct {
	inner  backend.ListIter
	prefix string
	err    error
}

func (it *CollectionsIter) Next() (name []byte, ok bool) {
	backendPath, ok := it.inner.Next()
	if !ok {
		return nil, false
	}
	// Iterating over directories causes a trailing slash we need to remove.
	backendPath = strings.TrimSuffix(backendPath, "/")

	if it.prefix == "" {
		r, err := paths.Parse(backendPath)
		if err != nil {
			it.err = fmt.Errorf("parsing path %q: %w", backendPath, err)
			return nil, false
		}
		if r.Type != paths.CollectionType {
			it.err = fmt.Errorf("got path type %q, expected %q", r.Type, paths.CollectionType)
			return nil, false
		}
		it.prefix = r.Prefix + "/"
	}
	trimmed := strings.TrimPrefix(backendPath, it.prefix)
	c, err := paths.ToCollection(trimmed)
	if err != nil {
		it.err = err
		return nil, false
	}
	return c, true
}

func (it *CollectionsIter) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Err()
}
