// Package data defines common data types for transaction identifiers and
// related operations.
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

// Equal reports whether t and other are the same transaction ID.
func (t TxID) Equal(other TxID) bool {
	return bytes.Equal(t, other)
}

// NewTId generates a new random 128-bit transaction ID.
func NewTId() TxID {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("Cannot read from random device: %v", err))
	}
	return b
}

// NewTxIDSet creates a TxIDSet containing the given transaction IDs, deduplicating any repeats.
func NewTxIDSet(ids ...TxID) TxIDSet {
	res := make(TxIDSet, 0, len(ids))
	for _, id := range ids {
		res, _ = res.Add(id)
	}
	return res
}

// TxIDSet is a set of transaction IDs optimized for small sizes.
type TxIDSet []TxID

// Add inserts a transaction ID into the set, returning the updated set and whether it was added.
func (s TxIDSet) Add(a TxID) (TxIDSet, bool) {
	if s.Contains(a) {
		return s, false
	}
	return append(s, a), true
}

// AddMulti inserts multiple transaction IDs into the set, deduplicating any repeats.
func (s TxIDSet) AddMulti(ids ...TxID) TxIDSet {
	res := s
	for _, e := range ids {
		res, _ = res.Add(e)
	}
	return res
}

// Contains reports whether the set contains the given transaction ID.
func (s TxIDSet) Contains(a TxID) bool {
	for _, id := range s {
		if id.Equal(a) {
			return true
		}
	}
	return false
}

// IndexOf returns the index of the given transaction ID in the set, or -1 if not found.
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
