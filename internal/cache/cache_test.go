package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntry string

func (e testEntry) SizeB() int {
	return len(e)
}

func TestGetSet(t *testing.T) {
	c := New(100)
	assert.Zero(t, c.SizeB())

	// Set and get.
	c.Set("a", testEntry("foo"))
	got, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("foo"))
	assert.Equal(t, 3, c.SizeB())

	// Modify and get again.
	c.Set("a", testEntry("barbaz"))
	got, ok = c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, 6, c.SizeB())
}

func TestDelete(t *testing.T) {
	c := New(100)
	keys := []string{"k1", "k2"}

	// Set and get all keys.
	for _, key := range keys {
		c.Set(key, testEntry(key))
	}
	for _, key := range keys {
		_, ok := c.Get(key)
		assert.True(t, ok)
	}
	assert.Equal(t, 4, c.SizeB())

	// Delete one key.
	c.Delete(keys[0])
	// One is deleted, the other is still there.
	_, ok := c.Get(keys[0])
	assert.False(t, ok)
	_, ok = c.Get(keys[1])
	assert.True(t, ok)
	assert.Equal(t, 2, c.SizeB())
}

func TestUpdate(t *testing.T) {
	c := New(100)
	c.Set("a", testEntry("foo"))
	assert.Equal(t, 3, c.SizeB())

	c.Update("a", func(_ Value) Value {
		return testEntry("barbaz")
	})
	got, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, 6, c.SizeB())
}

func TestUpdateNew(t *testing.T) {
	c := New(100)
	c.Update("a", func(old Value) Value {
		assert.Nil(t, old)
		return testEntry("bar")
	})
	got, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("bar"))
	assert.Equal(t, 3, c.SizeB())
}

func TestUpdateDelete(t *testing.T) {
	c := New(100)
	c.Set("a", testEntry("foo"))
	assert.Equal(t, 3, c.SizeB())

	c.Update("a", func(_ Value) Value {
		return nil
	})
	_, ok := c.Get("a")
	assert.False(t, ok)
	assert.Zero(t, c.SizeB())
}

func TestUpdateNope(t *testing.T) {
	c := New(100)
	c.Update("a", func(old Value) Value {
		assert.Nil(t, old)
		return nil
	})
	assert.Zero(t, c.SizeB())
}
