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

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/internal/cache"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/testkit"
)

const (
	testCollName    = "testp"
	clockMultiplier = 1000
)

var collInfoContents = []byte("__foo__")

type testContext struct {
	backend backend.Backend
	clock   clockwork.Clock
	global  Global
	local   Local
}

func testTLoggerContext(t *testing.T) (TLogger, testContext) {
	t.Helper()
	return newTLoggerFromBackend(t, memory.New())
}

func newTLoggerFromBackend(t *testing.T, b backend.Backend) (TLogger, testContext) {
	t.Helper()

	clock := testkit.NewAcceleratedClock(clockMultiplier)
	cache := cache.New(1024)
	local := NewLocal(cache, clock)
	global := NewGlobal(b, local, clock)
	tlogger := NewTLogger(clock, global, local, testCollName)

	// Create test collection.
	ctx := context.Background()
	_, err := global.Write(ctx, paths.CollectionInfo(testCollName), collInfoContents, nil)
	require.NoError(t, err)

	return tlogger, testContext{
		backend: b,
		clock:   clock,
		global:  global,
		local:   local,
	}
}

func TestGetSet(t *testing.T) {
	tl, tctx := testTLoggerContext(t)
	ctx := context.Background()
	tx := data.TxID([]byte("tx1"))

	// Unknown transactions are not found.
	_, err := tl.Get(ctx, tx)
	assert.ErrorIs(t, err, backend.ErrNotFound)
	// Their status is unknown.
	status, err := tl.CommitStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, TxCommitStatusUnknown, status.Status)

	// We don't handle sub-second precision from within TLogger.
	beforeSetTs := tctx.clock.Now().Truncate(time.Second)
	collection := paths.FromCollection("example", []byte("coll"))

	log := TxLog{
		ID:     tx,
		Status: TxCommitStatusOK,
		Writes: []TxWrite{
			{
				Path:  paths.FromKey(collection, []byte("key1")),
				Value: []byte("val"),
			},
		},
		Locks: []PathLock{
			{
				Path: paths.CollectionInfo(collection),
				Type: LockTypeWrite,
			},
			{
				Path: paths.FromKey(collection, []byte("key1")),
				Type: LockTypeCreate,
			},
			{
				Path: paths.FromKey(collection, []byte("key2")),
				Type: LockTypeUnknown,
			},
			{
				Path: paths.CollectionInfo(paths.FromCollection("example", []byte("coll2"))),
				Type: LockTypeRead,
			},
		},
	}

	// Write the log.
	_, err = tl.Set(ctx, log)
	assert.NoError(t, err)

	// The log comes back.
	gotlog, err := tl.Get(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, log.ID, gotlog.ID)
	assert.Equal(t, log.Status, gotlog.Status)
	assert.GreaterOrEqual(t, gotlog.Timestamp, beforeSetTs)
	assert.ElementsMatch(t, log.Locks, gotlog.Locks)
	assert.ElementsMatch(t, log.Writes, gotlog.Writes)

	// The status is correct.
	status, err = tl.CommitStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, TxCommitStatusOK, status.Status)
	assert.GreaterOrEqual(t, status.LastUpdate, beforeSetTs)
}

func TestPendingUpdate(t *testing.T) {
	tl1, tctx := testTLoggerContext(t)
	tl2, _ := newTLoggerFromBackend(t, tctx.backend)
	ctx := context.Background()
	tx := data.TxID([]byte("tx1"))

	// We don't handle sub-second precision from within TLogger.
	beforeSetTs := tctx.clock.Now().Truncate(time.Second)

	vers, err := tl1.Set(ctx, TxLog{
		ID:     tx,
		Status: TxCommitStatusPending,
	})
	assert.NoError(t, err)

	// Check status and get.
	status, err := tl2.CommitStatus(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, TxCommitStatusPending, status.Status)
	assert.GreaterOrEqual(t, status.LastUpdate, beforeSetTs)
	log, err := tl2.Get(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, tx, log.ID)
	assert.Equal(t, TxCommitStatusPending, log.Status)
	assert.GreaterOrEqual(t, log.Timestamp, beforeSetTs)

	// Wait some time.
	tctx.clock.Sleep(5 * time.Second)
	beforeOverwriteTs := tctx.clock.Now().Truncate(time.Second)

	// Overwrite the log with an updated pending state.
	_, err = tl1.SetIf(ctx, TxLog{
		ID:     tx,
		Status: TxCommitStatusPending,
	}, vers)
	assert.NoError(t, err)

	// We should see the updated timestamp.
	status, err = tl2.CommitStatus(ctx, tx)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, status.LastUpdate, beforeOverwriteTs)
	log, err = tl2.Get(ctx, tx)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, log.Timestamp, beforeOverwriteTs)
}
