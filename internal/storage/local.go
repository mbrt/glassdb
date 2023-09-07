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
	"math"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/cache"
)

const MaxStaleness = time.Duration(math.MaxInt64)

func NewLocal(c *cache.Cache, clock clockwork.Clock) Local {
	return Local{c, clock}
}

type Local struct {
	cache *cache.Cache
	clock clockwork.Clock
}

func (c Local) Read(key string, maxStale time.Duration) (LocalRead, bool) {
	e, ok := c.entry(key)
	if !ok || e.V == nil {
		return LocalRead{}, false
	}
	if c.isStale(e.V.Updated, maxStale) {
		return LocalRead{}, false
	}

	return LocalRead{
		Value:    e.V.Value,
		Version:  e.V.Version,
		Deleted:  e.V.Deleted,
		Outdated: e.isValueOutdated(),
	}, true
}

func (c Local) GetMeta(key string, maxStale time.Duration) (LocalMetadata, bool) {
	e, ok := c.entry(key)
	if !ok || e.M == nil {
		return LocalMetadata{}, false
	}
	if c.isStale(e.M.Updated, maxStale) {
		// The value is too stale.
		return LocalMetadata{}, false
	}
	return LocalMetadata{
		M:        e.M.Meta,
		Outdated: e.isMetaOutdated(),
	}, true
}

func (c Local) WriteWithMeta(key string, value []byte, meta backend.Metadata) {
	updated := c.clock.Now()
	newEntry := cacheEntry{
		V: &cacheValue{
			Value:   value,
			Version: VersionFromMeta(meta),
			Updated: updated,
		},
		M: &cacheMeta{
			Meta:    meta,
			Updated: updated,
		},
	}
	c.cache.Set(key, newEntry)
}

func (c Local) Write(key string, value []byte, v Version) {
	newValue := &cacheValue{
		Value:   value,
		Version: v,
		Updated: c.clock.Now(),
	}
	c.cache.Update(key, func(v cache.Value) cache.Value {
		if v == nil {
			return cacheEntry{V: newValue}
		}
		entry := v.(cacheEntry)
		entry.V = newValue
		return entry
	})
}

func (c Local) SetMeta(key string, meta backend.Metadata) {
	newMeta := &cacheMeta{
		Meta:    meta,
		Updated: c.clock.Now(),
	}

	c.cache.Update(key, func(v cache.Value) cache.Value {
		if v == nil {
			return cacheEntry{M: newMeta}
		}
		entry := v.(cacheEntry)
		entry.M = newMeta
		return entry
	})
}

// MarkStale annotates the given key value as outdated, only if it's currently
// set at the given version.
func (c Local) MarkValueOutated(key string, v Version) {
	c.cache.Update(key, func(old cache.Value) cache.Value {
		if old == nil {
			// Nothing to do. The value is not there anymore.
			return nil
		}
		entry := old.(cacheEntry)
		// Mark as outdated only if the version is the same.
		if entry.V != nil && entry.V.Version.EqualContents(v) {
			// Do not change the value directly to avoid race conditions.
			// Return a copy instead.
			newval := *entry.V
			newval.Outdated = true
			entry = cacheEntry{
				V: &newval,
				M: entry.M,
			}
		}
		return entry
	})
}

func (c Local) MarkDeleted(key string, v Version) {
	newValue := &cacheValue{
		Deleted: true,
		Version: v,
		Updated: c.clock.Now(),
	}
	c.cache.Update(key, func(v cache.Value) cache.Value {
		if v == nil {
			return cacheEntry{V: newValue}
		}
		entry := v.(cacheEntry)
		entry.V = newValue
		return entry
	})
}

func (c Local) Delete(key string) {
	c.cache.Delete(key)
}

func (c Local) entry(key string) (cacheEntry, bool) {
	e, ok := c.cache.Get(key)
	if !ok {
		return cacheEntry{}, false
	}
	return e.(cacheEntry), true
}

func (c Local) isStale(updated time.Time, maxStaleness time.Duration) bool {
	staleness := c.clock.Now().Sub(updated)
	return staleness > maxStaleness
}

type LocalRead struct {
	Value   []byte
	Version Version
	Deleted bool
	// Outdated is true if the value read is certainly outdated.
	Outdated bool
}

type LocalMetadata struct {
	M backend.Metadata
	// Outdated is true if the metadata is certainly outdated.
	Outdated bool
}

type cacheEntry struct {
	V *cacheValue
	M *cacheMeta
}

func (c cacheEntry) SizeB() int {
	var res int
	if c.V != nil {
		res += len(c.V.Value) + len(c.V.Version.Writer)
	}
	if c.M != nil {
		// Estimate 16 bytes per tag.
		res += len(c.M.Meta.Tags) * 16
	}
	return res
}

func (c cacheEntry) isMetaOutdated() bool {
	// Assume c.M is non nil.
	if c.V == nil || c.V.Version.B.IsNull() {
		return false
	}
	valVersion := c.V.Version.B
	metaVersion := c.M.Meta.Version
	if valVersion == metaVersion {
		return false
	}
	// The versions are different. If the value was updated last,
	// then metadata is definitely outdated.
	return c.V.Updated.After(c.M.Updated)
}

func (c cacheEntry) isValueOutdated() bool {
	if c.V.Outdated {
		// The value is outdated for sure.
		return true
	}
	// Assume c.V is non nil.
	if c.M == nil {
		return false
	}
	valVersion := c.V.Version.B
	metaVersion := c.M.Meta.Version
	if valVersion.IsNull() {
		// We have a local override (only version.Writer != nil).
		// We don't know whether it's outdated.
		return false
	}
	if valVersion.Contents == metaVersion.Contents {
		return false
	}
	// The versions are different. If metadata was updated last,
	// then the value is definitely outdated.
	return c.M.Updated.After(c.V.Updated)
}

type cacheValue struct {
	Value   []byte
	Deleted bool
	// Marks the given value as outdated for sure.
	// If false we don't know.
	Outdated bool
	Version  Version
	Updated  time.Time
}

type cacheMeta struct {
	Meta    backend.Metadata
	Updated time.Time
}
