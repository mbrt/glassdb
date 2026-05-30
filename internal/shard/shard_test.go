package shard

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountIsPowerOfTwo(t *testing.T) {
	n := Count()
	assert.GreaterOrEqual(t, n, 1)
	assert.Zero(t, n&(n-1), "Count() must be a power of two, got %d", n)
}

func TestNextPow2(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{-3, 1},
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{8, 8},
		{9, 16},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d", tc.in), func(t *testing.T) {
			assert.Equal(t, tc.want, nextPow2(tc.in))
		})
	}
}

func TestIndexInRange(t *testing.T) {
	const n = 8
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		idx := Index(key, n)
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, n)
	}
}

func TestIndexDeterministic(t *testing.T) {
	const n = 16
	assert.Equal(t, Index("some-key", n), Index("some-key", n))
}

func TestIndexDistribution(t *testing.T) {
	const (
		n    = 8
		keys = 8000
	)
	counts := make([]int, n)
	for i := range keys {
		counts[Index(fmt.Sprintf("key-%d", i), n)]++
	}
	// Every shard should get a non-trivial share of the keys.
	for shard, c := range counts {
		assert.Greater(t, c, keys/n/2, "shard %d underfilled: %d", shard, c)
	}
}

func TestShardedForAndEach(t *testing.T) {
	type counter struct{ id int }
	s := New(func(i int) *counter { return &counter{id: i} })
	require.Equal(t, Count(), s.Len())

	// For is stable for a given key.
	first := s.For("a-key")
	assert.Same(t, first, s.For("a-key"))
	assert.NotNil(t, first)

	// Each visits every shard exactly once.
	visited := make(map[int]int)
	s.Each(func(c *counter) { visited[c.id]++ })
	assert.Len(t, visited, s.Len())
	for id := range s.Len() {
		assert.Equal(t, 1, visited[id], "shard %d not visited exactly once", id)
	}
}
