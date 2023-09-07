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
	"container/list"
	"sync"
)

func New(maxSizeB int) *Cache {
	return &Cache{
		maxSizeB: maxSizeB,
		entries:  make(map[string]*list.Element),
		evicts:   list.New(),
	}
}

type Value interface {
	SizeB() int
}

type Cache struct {
	m         sync.Mutex
	maxSizeB  int
	currSizeB int
	entries   map[string]*list.Element
	evicts    *list.List
}

func (c *Cache) Get(key string) (Value, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	if e, ok := c.entries[key]; ok {
		c.evicts.MoveToFront(e)
		return e.Value.(entry).value, true
	}
	return nil, false
}

func (c *Cache) Set(key string, val Value) {
	c.Update(key, func(Value) Value {
		return val
	})
}

// Update updates the given cache value while locked.
// If the key is not present, nil is passed to fn. To remove
// the value, return nil in fn.
func (c *Cache) Update(key string, fn func(v Value) Value) {
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

func (c *Cache) Delete(key string) {
	c.m.Lock()
	defer c.m.Unlock()

	if e, ok := c.entries[key]; ok {
		c.deleteEntry(key, e)
	}
}

func (c *Cache) SizeB() int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.currSizeB
}

func (c *Cache) deleteEntry(key string, e *list.Element) {
	ent := e.Value.(entry)
	c.currSizeB -= ent.value.SizeB()
	c.evicts.Remove(e)
	delete(c.entries, key)
}

func (c *Cache) removeOldest() {
	for c.currSizeB > c.maxSizeB {
		it := c.evicts.Back()
		if it == nil {
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
