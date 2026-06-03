// Package cache implements a thread-safe LRU cache with size-based eviction.
package cache

import (
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
	c := &cacheShard{
		maxSizeB: maxSizeB,
		entries:  make(map[string]*node),
	}
	// The LRU list is a circular doubly-linked list with a sentinel: root.next
	// is the most-recently-used entry and root.prev is the least.
	c.root.next = &c.root
	c.root.prev = &c.root
	return c
}

// node is one entry in a shard's intrusive LRU list. Embedding the key/value
// directly in the list node (rather than using container/list) avoids the
// per-write allocation of a list element plus the interface boxing of the
// entry, which is the dominant cache-write cost on multi-key transactions.
type node struct {
	key   string
	value Value
	prev  *node
	next  *node
}

// cacheShard is one independent partition of the cache, holding its own lock,
// entries map, intrusive LRU list, and byte budget.
type cacheShard struct {
	m         sync.Mutex
	maxSizeB  int
	currSizeB int
	entries   map[string]*node
	root      node
}

func (c *cacheShard) get(key string) (Value, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	if n, ok := c.entries[key]; ok {
		c.moveToFront(n)
		return n.value, true
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

	if n, ok := c.entries[key]; ok {
		c.moveToFront(n)
		newv := fn(n.value)
		if newv == nil {
			// We had a value and now it's gone.
			c.removeNode(n)
			return
		}
		c.currSizeB += newv.SizeB() - n.value.SizeB()
		n.value = newv
	} else {
		newv := fn(nil)
		if newv == nil {
			// Nothing happened. We had nothing, we got nothing.
			return
		}
		n := &node{key: key, value: newv}
		c.pushFront(n)
		c.entries[key] = n
		c.currSizeB += newv.SizeB()
	}

	c.removeOldest()
}

func (c *cacheShard) delete(key string) {
	c.m.Lock()
	defer c.m.Unlock()

	if n, ok := c.entries[key]; ok {
		c.removeNode(n)
	}
}

func (c *cacheShard) sizeB() int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.currSizeB
}

// pushFront inserts n at the front (most-recently-used position) of the list.
func (c *cacheShard) pushFront(n *node) {
	n.prev = &c.root
	n.next = c.root.next
	c.root.next.prev = n
	c.root.next = n
}

// moveToFront promotes an already-linked node to the most-recently-used position.
func (c *cacheShard) moveToFront(n *node) {
	if c.root.next == n {
		return
	}
	n.prev.next = n.next
	n.next.prev = n.prev
	c.pushFront(n)
}

// removeNode unlinks n and drops it from the entries map, updating the size.
func (c *cacheShard) removeNode(n *node) {
	c.currSizeB -= n.value.SizeB()
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
	delete(c.entries, n.key)
}

func (c *cacheShard) removeOldest() {
	for c.currSizeB > c.maxSizeB {
		lru := c.root.prev
		if lru == &c.root || lru == c.root.next {
			// Never evict the most-recently-used entry (root.next), even if it
			// alone exceeds the shard budget. Otherwise a freshly written value
			// larger than the shard budget would be dropped immediately,
			// defeating the write and breaking callers that read back their own
			// writes. Overshoot is bounded to one entry per shard.
			return
		}
		c.removeNode(lru)
	}
}
