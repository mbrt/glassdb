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

package data

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// TxID represents a transaction id.
type TxID []byte

func (t TxID) String() string {
	return hex.EncodeToString(t)
}

func (t TxID) Equal(other TxID) bool {
	return bytes.Equal(t, other)
}

func NewTId() TxID {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("Cannot read from random device: %v", err))
	}
	return b
}

func NewTxIDSet(ids ...TxID) TxIDSet {
	res := make(TxIDSet, 0, len(ids))
	for _, id := range ids {
		res, _ = res.Add(id)
	}
	return res
}

// TxIDSet is a set of transaction IDs optimized for small sizes.
type TxIDSet []TxID

func (s TxIDSet) Add(a TxID) (TxIDSet, bool) {
	if s.Contains(a) {
		return s, false
	}
	return append(s, a), true
}

func (s TxIDSet) AddMulti(ids ...TxID) TxIDSet {
	res := s
	for _, e := range ids {
		res, _ = res.Add(e)
	}
	return res
}

func (s TxIDSet) Contains(a TxID) bool {
	for _, id := range s {
		if id.Equal(a) {
			return true
		}
	}
	return false
}

func (s TxIDSet) IndexOf(a TxID) int {
	for i, id := range s {
		if id.Equal(a) {
			return i
		}
	}
	return -1
}

// TxIDSetDiff is a set difference between a and b.
// It returns a new set where all elements of b have been removed
// from a.
func TxIDSetDiff(a, b TxIDSet) TxIDSet {
	var res TxIDSet
	for _, candidate := range a {
		if !b.Contains(candidate) {
			// We can keep it.
			res = append(res, candidate)
		}
	}
	return res
}

// TxIDSetIntersect is a set intersection between a and b.
// It returns a new set with only elements common between a and b.
func TxIDSetIntersect(a, b TxIDSet) TxIDSet {
	var res TxIDSet
	for _, candidate := range a {
		if b.Contains(candidate) {
			// We can keep it.
			res = append(res, candidate)
		}
	}
	return res
}

// TxIDSetUnion returns a new set with the elements of both a and b.
func TxIDSetUnion(a, b TxIDSet) TxIDSet {
	return NewTxIDSet(a...).AddMulti(b...)
}
