package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb/internal/shard"
)

type testEntry string

func (e testEntry) SizeB() int {
	return len(e)
}

func TestGetSet(t *testing.T) {
	c := newShard(100)
	assert.Zero(t, c.sizeB())

	// Set and get.
	c.set("a", testEntry("foo"))
	got, ok := c.get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("foo"))
	assert.Equal(t, 3, c.sizeB())

	// Modify and get again.
	c.set("a", testEntry("barbaz"))
	got, ok = c.get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, 6, c.sizeB())
}

func TestDelete(t *testing.T) {
	c := newShard(100)
	keys := []string{"k1", "k2"}

	// Set and get all keys.
	for _, key := range keys {
		c.set(key, testEntry(key))
	}
	for _, key := range keys {
		_, ok := c.get(key)
		assert.True(t, ok)
	}
	assert.Equal(t, 4, c.sizeB())

	// Delete one key.
	c.delete(keys[0])
	// One is deleted, the other is still there.
	_, ok := c.get(keys[0])
	assert.False(t, ok)
	_, ok = c.get(keys[1])
	assert.True(t, ok)
	assert.Equal(t, 2, c.sizeB())
}

func TestUpdate(t *testing.T) {
	c := newShard(100)
	c.set("a", testEntry("foo"))
	assert.Equal(t, 3, c.sizeB())

	c.update("a", func(_ Value) Value {
		return testEntry("barbaz")
	})
	got, ok := c.get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, 6, c.sizeB())
}

func TestUpdateNew(t *testing.T) {
	c := newShard(100)
	c.update("a", func(old Value) Value {
		assert.Nil(t, old)
		return testEntry("bar")
	})
	got, ok := c.get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("bar"))
	assert.Equal(t, 3, c.sizeB())
}

func TestUpdateDelete(t *testing.T) {
	c := newShard(100)
	c.set("a", testEntry("foo"))
	assert.Equal(t, 3, c.sizeB())

	c.update("a", func(_ Value) Value {
		return nil
	})
	_, ok := c.get("a")
	assert.False(t, ok)
	assert.Zero(t, c.sizeB())
}

func TestUpdateNope(t *testing.T) {
	c := newShard(100)
	c.update("a", func(old Value) Value {
		assert.Nil(t, old)
		return nil
	})
	assert.Zero(t, c.sizeB())
}

func TestEviction(t *testing.T) {
	// Budget for two 3-byte entries.
	c := newShard(6)
	c.set("a", testEntry("aaa"))
	c.set("b", testEntry("bbb"))
	assert.Equal(t, 6, c.sizeB())

	// Adding a third entry evicts the least recently used ("a").
	c.set("c", testEntry("ccc"))
	_, ok := c.get("a")
	assert.False(t, ok)
	_, ok = c.get("b")
	assert.True(t, ok)
	_, ok = c.get("c")
	assert.True(t, ok)
	assert.Equal(t, 6, c.sizeB())
}

func TestSharded(t *testing.T) {
	n := shard.Count()
	if n < 2 {
		t.Skip("sharded behavior requires GOMAXPROCS >= 2")
	}
	// Per-shard budget of exactly 6 bytes.
	c := New(6 * n)

	// Two distinct keys in shard 0 and one in shard 1.
	s0 := keysForShard(0, n, 2)
	s1 := keysForShard(1, n, 1)

	// Routing across shards and SizeB summation.
	c.Set(s0[0], testEntry("aaa")) // 3 bytes in shard 0
	c.Set(s1[0], testEntry("bbb")) // 3 bytes in shard 1
	assert.Equal(t, 6, c.SizeB())

	// Overflowing shard 0 only evicts within shard 0; shard 1 is untouched.
	c.Set(s0[1], testEntry("cccc")) // 4 bytes pushes shard 0 to 7 > 6
	_, ok := c.Get(s0[0])
	assert.False(t, ok, "least recently used entry in shard 0 should be evicted")
	_, ok = c.Get(s0[1])
	assert.True(t, ok)
	_, ok = c.Get(s1[0])
	assert.True(t, ok, "entry in shard 1 must be unaffected by shard 0 eviction")
}

// BenchmarkParallel exercises the cache from many goroutines at once, which is
// the contention scenario the sharding is meant to relieve.
func BenchmarkParallel(b *testing.B) {
	const keys = 1024
	c := New(64 * 1024 * 1024)
	ks := make([]string, keys)
	for i := range ks {
		ks[i] = fmt.Sprintf("key-%d", i)
		c.Set(ks[i], testEntry(ks[i]))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := ks[i%keys]
			if i%8 == 0 {
				c.Set(k, testEntry(k))
			} else {
				c.Get(k)
			}
			i++
		}
	})
}

// keysForShard returns count distinct keys that hash to the given shard.
func keysForShard(target, n, count int) []string {
	res := make([]string, 0, count)
	for i := 0; len(res) < count; i++ {
		k := fmt.Sprintf("k%d", i)
		if shard.Index(k, n) == target {
			res = append(res, k)
		}
	}
	return res
}
