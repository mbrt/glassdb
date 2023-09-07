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
	assert.Equal(t, int64(3), c.SizeB())

	// Modify and get again.
	c.Set("a", testEntry("barbaz"))
	got, ok = c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, int64(6), c.SizeB())
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
	assert.Equal(t, int64(4), c.SizeB())

	// Delete one key.
	c.Delete(keys[0])
	// One is deleted, the other is still there.
	_, ok := c.Get(keys[0])
	assert.False(t, ok)
	_, ok = c.Get(keys[1])
	assert.True(t, ok)
	assert.Equal(t, int64(2), c.SizeB())
}

func TestUpdate(t *testing.T) {
	c := New(100)
	c.Set("a", testEntry("foo"))
	assert.Equal(t, int64(3), c.SizeB())

	c.Update("a", func(old Value) Value {
		return testEntry("barbaz")
	})
	got, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, got, testEntry("barbaz"))
	assert.Equal(t, int64(6), c.SizeB())
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
	assert.Equal(t, int64(3), c.SizeB())
}

func TestUpdateDelete(t *testing.T) {
	c := New(100)
	c.Set("a", testEntry("foo"))
	assert.Equal(t, int64(3), c.SizeB())

	c.Update("a", func(old Value) Value {
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
