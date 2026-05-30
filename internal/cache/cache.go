// Package cache implements a thread-safe LRU cache with size-based eviction.
package cache

import (
	"container/list"
	"sync"

	"github.com/mbrt/glassdb/internal/shard"
)

// New creates a new LRU cache with the given maximum size in bytes. The budget
// is split evenly across shards to reduce lock contention.
func New(maxSizeB int) *Cache {
	per := maxSizeB / shard.Count()
	return &Cache{
		sh: shard.New(func(int) *cacheShard { return newShard(per) }),
	}
}

// Value is an interface for cache entries that report their size in bytes.
type Value interface {
	SizeB() int
}

// Cache is a thread-safe LRU cache that evicts the least recently used entries
// when the total size exceeds the configured maximum. It is partitioned into
// independent shards, each with its own lock and byte budget.
type Cache struct {
	sh shard.Sharded[cacheShard]
}

// Get returns the value for the given key, moving it to the front of the LRU list.
func (c *Cache) Get(key string) (Value, bool) {
	return c.sh.For(key).get(key)
}

// Set stores a value in the cache for the given key, evicting old entries if necessary.
func (c *Cache) Set(key string, val Value) {
	c.sh.For(key).set(key, val)
}

// Update updates the given cache value while locked.
// If the key is not present, nil is passed to fn. To remove
// the value, return nil in fn.
func (c *Cache) Update(key string, fn func(v Value) Value) {
	c.sh.For(key).update(key, fn)
}

// Delete removes the entry for the given key from the cache.
func (c *Cache) Delete(key string) {
	c.sh.For(key).delete(key)
}

// SizeB returns the current total size of the cache in bytes across all shards.
func (c *Cache) SizeB() int {
	var total int
	c.sh.Each(func(s *cacheShard) {
		total += s.sizeB()
	})
	return total
}

// newShard returns a cache shard with the given maximum size in bytes.
func newShard(maxSizeB int) *cacheShard {
	return &cacheShard{
		maxSizeB: maxSizeB,
		entries:  make(map[string]*list.Element),
		evicts:   list.New(),
	}
}

// cacheShard is one independent partition of the cache, holding its own lock,
// entries map, LRU list, and byte budget.
type cacheShard struct {
	m         sync.Mutex
	maxSizeB  int
	currSizeB int
	entries   map[string]*list.Element
	evicts    *list.List
}

func (c *cacheShard) get(key string) (Value, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	if e, ok := c.entries[key]; ok {
		c.evicts.MoveToFront(e)
		return e.Value.(entry).value, true
	}
	return nil, false
}

func (c *cacheShard) set(key string, val Value) {
	c.update(key, func(Value) Value {
		return val
	})
}

func (c *cacheShard) update(key string, fn func(v Value) Value) {
	c.m.Lock()
	defer c.m.Unlock()

	if e, ok := c.entries[key]; ok {
		c.evicts.MoveToFront(e)
		old := e.Value.(entry)
		newv := fn(old.value)
		if newv == nil {
			// We had a value and now it's gone.
			c.deleteEntry(key, e)
			return
		}
		c.currSizeB += newv.SizeB() - old.value.SizeB()
		e.Value = entry{key: key, value: newv}
	} else {
		newv := fn(nil)
		if newv == nil {
			// Nothing happened. We had nothing, we got nothing.
			return
		}
		e := c.evicts.PushFront(entry{key: key, value: newv})
		c.entries[key] = e
		c.currSizeB += newv.SizeB()
	}

	c.removeOldest()
}

func (c *cacheShard) delete(key string) {
	c.m.Lock()
	defer c.m.Unlock()

	if e, ok := c.entries[key]; ok {
		c.deleteEntry(key, e)
	}
}

func (c *cacheShard) sizeB() int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.currSizeB
}

func (c *cacheShard) deleteEntry(key string, e *list.Element) {
	ent := e.Value.(entry)
	c.currSizeB -= ent.value.SizeB()
	c.evicts.Remove(e)
	delete(c.entries, key)
}

func (c *cacheShard) removeOldest() {
	for c.currSizeB > c.maxSizeB {
		it := c.evicts.Back()
		if it == nil || it == c.evicts.Front() {
			// Never evict the most-recently-used entry, even if it alone
			// exceeds the shard budget. Otherwise a freshly written value
			// (e.g. one larger than maxSizeB/shards) would be dropped
			// immediately, defeating the write and breaking callers that
			// read back their own writes. Overshoot is bounded to one entry
			// per shard.
			return
		}
		ent := it.Value.(entry)
		c.currSizeB -= ent.value.SizeB()
		delete(c.entries, ent.key)
		c.evicts.Remove(it)
	}
}

type entry struct {
	key   string
	value Value
}
