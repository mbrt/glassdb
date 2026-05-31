package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mbrt/glassdb/internal/data"
)

// older and younger build deterministic TxIDs whose priority follows their
// names: older has an earlier timestamp than younger.
var (
	tsOlder   = time.Unix(100, 0)
	tsYounger = time.Unix(200, 0)
)

func TestComputeLockUpdateWoundWait(t *testing.T) {
	older := data.TIDWithPriority(tsOlder, []byte("old"))
	younger := data.TIDWithPriority(tsYounger, []byte("new"))
	holder := data.TIDWithPriority(time.Unix(150, 0), []byte("hold"))

	pending := func(ids ...data.TxID) []TxPathState {
		var res []TxPathState
		for _, id := range ids {
			res = append(res, TxPathState{Tx: id, Status: TxCommitStatusPending})
		}
		return res
	}

	tests := []struct {
		name        string
		curr        LockInfo
		req         LockRequest
		txs         []TxPathState
		wantWound   []data.TxID
		wantWaitFor []data.TxID
	}{
		{
			name:        "write requester older than write holder wounds it",
			curr:        LockInfo{Type: LockTypeWrite, LockedBy: []data.TxID{younger}},
			req:         LockRequest{Type: LockTypeWrite, Lockers: []data.TxID{older}},
			txs:         pending(younger),
			wantWound:   []data.TxID{younger},
			wantWaitFor: nil,
		},
		{
			name:        "write requester younger than write holder waits",
			curr:        LockInfo{Type: LockTypeWrite, LockedBy: []data.TxID{older}},
			req:         LockRequest{Type: LockTypeWrite, Lockers: []data.TxID{younger}},
			txs:         pending(older),
			wantWound:   nil,
			wantWaitFor: []data.TxID{older},
		},
		{
			name:        "read requester older than write holder wounds it",
			curr:        LockInfo{Type: LockTypeWrite, LockedBy: []data.TxID{younger}},
			req:         LockRequest{Type: LockTypeRead, Lockers: []data.TxID{older}},
			txs:         pending(younger),
			wantWound:   []data.TxID{younger},
			wantWaitFor: nil,
		},
		{
			name:        "read requester younger than write holder waits",
			curr:        LockInfo{Type: LockTypeWrite, LockedBy: []data.TxID{older}},
			req:         LockRequest{Type: LockTypeRead, Lockers: []data.TxID{younger}},
			txs:         pending(older),
			wantWound:   nil,
			wantWaitFor: []data.TxID{older},
		},
		{
			name:        "write requester partitions mixed read holders",
			curr:        LockInfo{Type: LockTypeRead, LockedBy: []data.TxID{older, younger}},
			req:         LockRequest{Type: LockTypeWrite, Lockers: []data.TxID{holder}},
			txs:         pending(older, younger),
			wantWound:   []data.TxID{younger},
			wantWaitFor: []data.TxID{older},
		},
		{
			name:        "requester does not wound itself among readers",
			curr:        LockInfo{Type: LockTypeRead, LockedBy: []data.TxID{older, younger}},
			req:         LockRequest{Type: LockTypeWrite, Lockers: []data.TxID{younger}},
			txs:         pending(older, younger),
			wantWound:   nil,
			wantWaitFor: []data.TxID{older, younger},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops, err := ComputeLockUpdate(tc.curr, tc.req, tc.txs)
			require.NoError(t, err)
			assert.Equal(t, tc.wantWound, ops.Wound)
			assert.Equal(t, tc.wantWaitFor, ops.WaitFor)
			// Conflicts never produce a storage update.
			assert.False(t, ops.HasUpdate)
			assert.Empty(t, ops.LockedFor)
		})
	}
}
