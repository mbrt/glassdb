// Package data defines common data types for transaction identifiers and
// related operations.
package data

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

const (
	txIDLen   = 16
	txIDTSOff = 8 // [0:8] random prefix, [8:16] big-endian UnixNano timestamp.
)

// TxID represents a transaction id.
//
// The layout is [8 bytes random][8 bytes big-endian UnixNano timestamp]. The
// random bytes come first so that transaction-log keys (_t/<tx-id>) keep a
// high-entropy prefix, spreading writes across object-storage partitions
// instead of clustering sequential commits into a single hot partition. The
// timestamp suffix encodes the transaction priority used by the wound-wait
// rule: an earlier timestamp means an older, higher-priority transaction.
type TxID []byte

func (t TxID) String() string {
	return hex.EncodeToString(t)
}

// Equal reports whether t and other are the same transaction ID.
func (t TxID) Equal(other TxID) bool {
	return bytes.Equal(t, other)
}

// Older reports whether t has strictly higher priority than other, i.e. it
// carries an earlier timestamp.
//
// Priority depends only on the timestamp, never on the random prefix. This is
// essential for the wound-wait rule: RenewTID preserves the timestamp but mints
// a fresh prefix on every restart, so a prefix-based tiebreak would let two
// equal-timestamp transactions flip their relative order on each wound and
// livelock by wounding each other forever. Transactions sharing a timestamp are
// therefore never ordered against each other; that rare tie is left to the
// serial-locking deadlock safety net.
func (t TxID) Older(other TxID) bool {
	return t.priority() < other.priority()
}

func (t TxID) priority() uint64 {
	return binary.BigEndian.Uint64(t[txIDTSOff:])
}

// NewTId generates a new transaction ID using the current time as its priority.
func NewTId() TxID {
	return newTID(time.Now())
}

// RenewTID returns a transaction ID that preserves the priority (timestamp) of
// old but uses a fresh random prefix. A wounded transaction reuses its priority
// on restart to avoid starvation, while the new prefix gives it a distinct log
// object that lands in a different storage partition.
func RenewTID(old TxID) TxID {
	res := make([]byte, txIDLen)
	randPrefix(res)
	copy(res[txIDTSOff:], old[txIDTSOff:])
	return res
}

// TIDWithPriority builds a transaction ID from an explicit timestamp and random
// prefix. It is meant for tests that need deterministic priorities.
func TIDWithPriority(ts time.Time, prefix []byte) TxID {
	res := make([]byte, txIDLen)
	copy(res[:txIDTSOff], prefix)
	binary.BigEndian.PutUint64(res[txIDTSOff:], uint64(ts.UnixNano()))
	return res
}

// NewTIDFromSource generates a transaction ID using ts as its priority and src
// as the source of the random prefix. It lets deterministic tests (e.g. the
// serializability fuzzer) control the otherwise-random prefix so that a given
// input reproduces exactly; production code uses NewTId (crypto/rand).
func NewTIDFromSource(src io.Reader, ts time.Time) TxID {
	res := make([]byte, txIDLen)
	prefixFromSource(src, res)
	binary.BigEndian.PutUint64(res[txIDTSOff:], uint64(ts.UnixNano()))
	return res
}

// RenewTIDFromSource behaves like RenewTID but draws the fresh prefix from src,
// keeping wounded-transaction restarts deterministic under a fixed source.
func RenewTIDFromSource(src io.Reader, old TxID) TxID {
	res := make([]byte, txIDLen)
	prefixFromSource(src, res)
	copy(res[txIDTSOff:], old[txIDTSOff:])
	return res
}

func newTID(ts time.Time) TxID {
	res := make([]byte, txIDLen)
	randPrefix(res)
	binary.BigEndian.PutUint64(res[txIDTSOff:], uint64(ts.UnixNano()))
	return res
}

func randPrefix(b []byte) {
	if _, err := rand.Read(b[:txIDTSOff]); err != nil {
		panic(fmt.Sprintf("Cannot read from random device: %v", err))
	}
}

func prefixFromSource(src io.Reader, b []byte) {
	if _, err := io.ReadFull(src, b[:txIDTSOff]); err != nil {
		panic(fmt.Sprintf("Cannot read from random source: %v", err))
	}
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
