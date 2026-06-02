package data

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTIdLayout(t *testing.T) {
	id := NewTID()
	assert.Len(t, []byte(id), txIDLen)
	// The timestamp suffix should decode to something close to now.
	got := time.Unix(0, int64(id.priority()))
	assert.WithinDuration(t, time.Now(), got, time.Minute)
}

func TestTxIDOlder(t *testing.T) {
	base := time.Unix(1000, 0)
	older := TIDWithPriority(base, []byte("zzzzzzzz"))
	younger := TIDWithPriority(base.Add(time.Second), []byte("aaaaaaaa"))

	// The timestamp wins over the random prefix.
	assert.True(t, older.Older(younger))
	assert.False(t, younger.Older(older))
}

func TestTxIDOlderTimestampDominatesPrefix(t *testing.T) {
	// Even with a "larger" random prefix, the earlier timestamp is older.
	older := TIDWithPriority(time.Unix(1, 0), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	younger := TIDWithPriority(time.Unix(2, 0), []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	assert.True(t, older.Older(younger))
}

func TestTxIDOlderEqualTimestampNotOrdered(t *testing.T) {
	ts := time.Unix(42, 0)
	a := TIDWithPriority(ts, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	b := TIDWithPriority(ts, []byte{0, 0, 0, 0, 0, 0, 0, 2})

	// Same timestamp: neither is older, regardless of the random prefix. This
	// prevents a wound ping-pong livelock across restarts, since RenewTID mints
	// a fresh prefix but preserves the timestamp.
	assert.False(t, a.Older(b))
	assert.False(t, b.Older(a))
}

func TestRenewTIDDoesNotFlipOrdering(t *testing.T) {
	ts := time.Unix(42, 0)
	a := TIDWithPriority(ts, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	b := TIDWithPriority(ts, []byte{0, 0, 0, 0, 0, 0, 0, 2})

	// Restarting either transaction must not make it suddenly older than its
	// equal-timestamp peer, which is what would cause repeated mutual wounds.
	for range 100 {
		a = RenewTID(a)
		b = RenewTID(b)
		assert.False(t, a.Older(b))
		assert.False(t, b.Older(a))
	}
}

func TestRenewTIDPreservesPriority(t *testing.T) {
	orig := NewTID()
	renewed := RenewTID(orig)

	require.Len(t, []byte(renewed), txIDLen)
	// Same priority, different identity.
	assert.Equal(t, orig.priority(), renewed.priority())
	assert.False(t, orig.Equal(renewed))
	// Neither is strictly older than the other only if prefixes happen to
	// match; in practice the fresh random prefix differs.
	assert.NotEqual(t, []byte(orig)[:txIDTSOff], []byte(renewed)[:txIDTSOff])
}

func TestNewTIdUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for range 1000 {
		id := NewTID()
		_, ok := seen[string(id)]
		require.False(t, ok, "duplicate TxID generated")
		seen[string(id)] = struct{}{}
	}
}
