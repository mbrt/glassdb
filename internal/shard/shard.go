// Package shard partitions a shared structure into independent shards selected
// by key hash, reducing lock contention on hot DB-level maps.
package shard

import "runtime"

const (
	fnvOffset32 = 2166136261
	fnvPrime32  = 16777619
)

// Count returns the recommended number of shards: the next power of two
// greater than or equal to GOMAXPROCS.
func Count() int {
	return nextPow2(runtime.GOMAXPROCS(0))
}

// Index returns the shard index for key given n shards. n must be a power of
// two, so the modulo reduces to a bit mask.
func Index(key string, n int) int {
	return int(fnv1a(key) & uint32(n-1))
}

// Sharded owns Count() independent shards of type T, routed by key hash. It is
// meant to be embedded in a wrapper that delegates per-key operations to the
// shard returned by For.
type Sharded[T any] struct {
	shards []*T
}

// New builds Count() shards, initializing shard i with newShard(i).
func New[T any](newShard func(i int) *T) Sharded[T] {
	n := Count()
	s := Sharded[T]{shards: make([]*T, n)}
	for i := range s.shards {
		s.shards[i] = newShard(i)
	}
	return s
}

// For returns the shard responsible for key.
func (s Sharded[T]) For(key string) *T {
	return s.shards[Index(key, len(s.shards))]
}

// Len returns the number of shards.
func (s Sharded[T]) Len() int {
	return len(s.shards)
}

// Each calls fn once for every shard. The order is unspecified.
func (s Sharded[T]) Each(fn func(*T)) {
	for _, sh := range s.shards {
		fn(sh)
	}
}

// fnv1a is an inline FNV-1a 32-bit hash that avoids allocating a hash.Hash.
func fnv1a(key string) uint32 {
	h := uint32(fnvOffset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= fnvPrime32
	}
	return h
}

// nextPow2 returns the smallest power of two greater than or equal to n,
// returning 1 for n <= 1.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
