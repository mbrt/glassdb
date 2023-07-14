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

package trans

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trace"
)

var ErrRetry = errors.New("retry transaction")

type statusType int

const (
	algoConcurrency       = 10
	backgroundConcurrency = 3
)

const (
	statusNew statusType = iota
	statusValidating
	statusCommitted
)

type vResult int

const (
	vResultUnknown vResult = iota
	vResultOK
	vResultRetry
	vResultNeedsCLock
)

const (
	lockTimeout      = 30 * time.Second
	bgCleanupTimeout = time.Minute
)

var (
	errLockTimeout   = errors.New("lock timeout")
	errNoSingleWrite = errors.New("cannot validate transaction with multiple writes")
	errValidateRetry = errors.New("retry validation")
)

func NewAlgo(
	c clockwork.Clock,
	g storage.Global,
	local storage.Local,
	locker *Locker,
	mon *Monitor,
	bg *concurr.Background,
	log Logger,
) Algo {
	return Algo{
		clock:      c,
		global:     g,
		local:      local,
		reader:     NewReader(local, g, mon),
		locker:     locker,
		mon:        mon,
		background: bg,
		log:        log,
	}
}

type Algo struct {
	clock      clockwork.Clock
	global     storage.Global
	local      storage.Local
	reader     Reader
	locker     *Locker
	mon        *Monitor
	background *concurr.Background
	log        Logger
}

func (t Algo) Begin(d Data) *Handle {
	return &Handle{
		id:     data.NewTId(),
		data:   d,
		status: statusNew,
	}
}

func (t Algo) Commit(ctx context.Context, tx *Handle) error {
	if tx.status == statusNew {
		t.mon.BeginTx(ctx, tx.id)
		tx.status = statusValidating
	}
	vstate := initValidation(tx)
	t.traceVstate(*vstate, tx)

	for {
		t.log.Tracef("tx=%v, event=Commit round BEGIN", tx.id)
		err := t.validateRound(ctx, vstate, tx)
		t.log.Tracef("tx=%v, event=Commit round END, err=%v", tx.id, err)

		if err == nil {
			// Committed.
			break
		}
		if !errors.Is(err, errValidateRetry) {
			// Other errors.
			// Before retrying, update possible stale values.
			t.updateLocalCache(*vstate)
			return err
		}
		// Retry the validation without retrying the transaction.
	}

	// Validation succeeded.
	if err := t.commitWrites(ctx, tx.data.Writes, tx.id); err != nil {
		return fmt.Errorf("committing writes for tx %v: %w", tx.id, err)
	}
	// We are now considered committed.
	tx.status = statusCommitted
	t.asyncCleanup(ctx, tx)
	return nil
}

func (t Algo) ValidateReads(ctx context.Context, tx *Handle) error {
	if tx.status == statusNew {
		t.mon.BeginTx(ctx, tx.id)
		tx.status = statusValidating
	}
	if len(tx.data.Writes) > 0 {
		return errors.New("cannot validate only reads when writes are present")
	}
	vstate := initValidation(tx)

	if err := t.validateReadonly(ctx, vstate, tx); err != nil {
		t.updateLocalCache(*vstate)
		return err
	}
	return nil
}

func (t Algo) validateRound(ctx context.Context, vstate *validationState, tx *Handle) error {
	if tx.requireLocks {
		return t.validateReadWrite(ctx, vstate, tx)
	}
	if len(tx.data.Writes) == 0 {
		return t.validateReadonly(ctx, vstate, tx)
	}
	// TODO: Blind write optimization.
	if isSingleRW(tx.data) {
		err := t.commitSingleRW(ctx, tx)
		if errors.Is(err, errNoSingleWrite) || errors.Is(err, ErrRetry) {
			// We need to fallback to regular validation from now on.
			// We also do our first validation now, as this way we start
			// acquiring locks early.
			tx.requireLocks = true
			return errValidateRetry
		}
		return err
	}

	// Fallback: no optimizations are possible.
	return t.validateReadWrite(ctx, vstate, tx)
}

func (t Algo) Reset(tx *Handle, data Data) {
	if tx.status == statusCommitted {
		panic("Cannot reset a committed transaction")
	}
	// Do not reset the locked items. They should persist.
	tx.data = data
}

func (t Algo) End(ctx context.Context, tx *Handle) error {
	if tx == nil || tx.status == statusCommitted {
		return nil
	}
	// Release all the locks and abort.
	if err := t.mon.AbortTx(ctx, tx.id); err != nil {
		t.log.Logf("tx=%v, event=Commit End, err=%v", tx.id, err)
		// Schedule follow-up cleanup.
		t.asyncCleanup(ctx, tx)
		return err
	}
	return nil
}

func (t Algo) validateReadWrite(ctx context.Context, vstate *validationState, tx *Handle) error {
	if tx.serialLocking {
		return t.serialValidate(ctx, vstate, tx)
	}
	if err := t.parallelValidate(ctx, vstate, tx); err != nil {
		if !errors.Is(err, errLockTimeout) {
			return err
		}
		// We are most likely deadlocked.
		// We should start again with serialized locking.
		tx.serialLocking = true
		return errValidateRetry
	}
	return nil
}

func (t Algo) commitSingleRW(ctx context.Context, tx *Handle) error {
	t.log.Tracef("tx=%v, event=Commit single RW BEGIN", tx.id)
	read := tx.data.Reads[0]
	write := tx.data.Writes[0]

	meta, err := t.reader.GetMetadata(ctx, read.Path, storage.MaxStaleness)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			// We would need to lock the collection. Fallback to regular validate.
			return errNoSingleWrite
		}
		return fmt.Errorf("getting metadata for %q: %w", read.Path, err)
	}

	// We can try validating twice without retrying the transaction, because
	// we could be potentially racing against the last transaction flush and
	// still be valid.
	// TODO: Use a flag in tx instead of this loop.
	for i := 0; i < 2; i++ {
		if err := t.checkReadVersionUnlocked(read.Version, meta); err != nil {
			if errors.Is(err, ErrRetry) {
				t.local.MarkValueOutated(write.Path, read.Version.ToStorageVersion())
			}
			return err
		}
		// We can proceed committing (if nothing else changes in the meantime).
		slocker := storage.NewLocker(t.global)
		err = slocker.UpdateLock(ctx, read.Path, meta.Version, storage.LockUpdate{
			Type:   storage.LockTypeNone,
			Writer: tx.id,
			Value: storage.TValue{
				Value: write.Val,
			},
		})
		if err == nil {
			// Committed.
			return nil
		}
		if !errors.Is(err, backend.ErrPrecondition) {
			// Unknown error.
			return err
		}

		// Some other transaction raced against us.
		// Let's retry, after refreshing metadata with a strong read.
		meta, err = t.global.GetMetadata(ctx, read.Path)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				// We would need to lock the collection. Fallback to regular validate.
				return errNoSingleWrite
			}
			return fmt.Errorf("getting metadata for %q: %w", read.Path, err)
		}
	}

	// We tried our best, but we keep getting raced against. Let's just do
	// regular validations from now on.
	t.local.MarkValueOutated(read.Path, read.Version.ToStorageVersion())
	return ErrRetry
}

func (t Algo) validateReadonly(ctx context.Context, vstate *validationState, tx *Handle) error {
	t.log.Tracef("tx=%v, event=Commit readonly BEGIN", tx.id)
	// We can validate that the read is consistent (i.e. not modified by anyone
	// since, nor locked by others) and avoid any locking.
	err := t.fanout(ctx, len(vstate.Paths), func(ctx context.Context, i int) error {
		item := &vstate.Paths[i]
		if item.ReadVersion.IsLocal() {
			return t.validateLocalRead(ctx, item, tx)
		}
		return t.validateBackendRead(ctx, item, tx)
	})

	if err != nil {
		return err
	}
	err = vstate.ToError()
	if errors.Is(err, ErrRetry) {
		// Things changed in the meantime. To avoid retrying too often, just do
		// regular validation after locking next time.
		tx.requireLocks = true
	}
	return err
}

func (t Algo) validateLocalRead(
	ctx context.Context,
	item *pathState,
	tx *Handle,
) error {
	// We need the freshest possible meta, because we don't have a lock.
	meta, err := t.global.GetMetadata(ctx, item.Path)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			if item.NotFound {
				// All good here, the item is still not found now.
				item.Result = vResultOK
				return nil
			}
			// The item is not there anymore. Retry.
			item.Result = vResultRetry
			return nil
		}
		return err
	}
	readFromT := item.ReadVersion.Writer

	// Check the current state of the item.
	li, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}

	// If the item is unlocked or locked in read, the last writer must be
	// the one we read from.
	if li.Type == storage.LockTypeNone || li.Type == storage.LockTypeRead {
		if !li.LastWriter.Equal(readFromT) {
			item.Result = vResultRetry
			return nil
		}
		item.Result = vResultOK
		return nil
	}

	// The item is still locked in write or create. Two valid cases:
	// - If the transaction is committed, it must be the one we read from.
	// - If not committed, the last writer must be the one we read from.
	if len(li.LockedBy) != 1 {
		return fmt.Errorf("bad lock: %q with %d lockers", li.Type, len(li.LockedBy))
	}
	locker := li.LockedBy[0]
	status, err := t.mon.TxStatus(ctx, locker)
	if err != nil {
		return err
	}

	// Determine what's the expected last writer.
	var (
		expectedWriter data.TxID
		expectedVal    KeyCommitStatus
	)
	switch status {
	case storage.TxCommitStatusOK:
		v, err := t.mon.CommittedValue(ctx, item.Path, locker)
		if err != nil {
			return err
		}
		if v.Value.NotWritten {
			expectedWriter = li.LastWriter
		} else {
			expectedWriter = locker
			expectedVal = v
		}

	case storage.TxCommitStatusAborted, storage.TxCommitStatusPending:
		expectedWriter = li.LastWriter

	default:
		return fmt.Errorf("unknown tx commit status: %v", status)
	}

	if readFromT.Equal(expectedWriter) {
		item.Result = vResultOK
		return nil
	}

	// We read from an old value.
	// Let's update our local copy and retry.
	item.Result = vResultRetry

	if expectedVal.Status == storage.TxCommitStatusUnknown {
		// Fetch the expected committed value.
		expectedVal, err = t.mon.CommittedValue(ctx, item.Path, expectedWriter)
		if err != nil || expectedVal.Value.NotWritten {
			// No need to report an error here.
			// We just can't update the local cache.
			return nil
		}
	}

	t.updateLocal(
		WriteAccess{
			Path:   item.Path,
			Val:    expectedVal.Value.Value,
			Delete: expectedVal.Value.Deleted},
		expectedWriter)
	return nil
}

func (t Algo) validateBackendRead(
	ctx context.Context,
	item *pathState,
	tx *Handle,
) error {
	if item.NotFound {
		return t.validateBackendReadNotFound(ctx, item, tx)
	}
	// Here the item was found.
	// We need the freshest possible meta, because we don't have a lock.
	meta, err := t.global.GetMetadata(ctx, item.Path)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			item.Result = vResultRetry
			return nil
		}
		return err
	}
	readVersion := item.ReadVersion.B.Contents

	// We read from the backend directly.
	// The read was inconsistent if the contents already changed in the
	// meantime.
	if readVersion != meta.Version.Contents {
		item.Result = vResultRetry
		return nil
	}

	// If not, we need to make sure no other transaction is locking the
	// item in write or create and already committed.

	// Check the current lock state of the item.
	li, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}
	switch li.Type {
	case storage.LockTypeNone, storage.LockTypeRead:
		// The item is not being changed right now.
		item.Result = vResultOK
		return nil
	case storage.LockTypeCreate:
		// We should have read a NotFound here, but we read an empty value.
		// Get rid of inconsistent value in local cache.
		item.Result = vResultRetry
		return nil
	}

	// This was locked in write.
	if len(li.LockedBy) != 1 {
		return fmt.Errorf("bad lock: %q with %d lockers", li.Type, len(li.LockedBy))
	}
	locker := li.LockedBy[0]
	status, err := t.mon.TxStatus(ctx, locker)
	if err != nil {
		return err
	}
	switch status {
	case storage.TxCommitStatusOK:
		// Continue below.
	case storage.TxCommitStatusAborted, storage.TxCommitStatusPending:
		item.Result = vResultOK
		return nil
	default:
		return fmt.Errorf("unknown status for locker %v: %v", locker, status)
	}

	// This was committed. Let's fetch the value and check whether it was
	// modified.
	v, err := t.mon.CommittedValue(ctx, item.Path, locker)
	if err != nil {
		return err
	}
	if v.Value.NotWritten {
		// It was not updated by the transaction.
		item.Result = vResultOK
		return nil
	}
	// The value was committed, but we read the previous version.
	// Let's update our local copy and retry.
	item.Result = vResultRetry
	t.updateLocal(
		WriteAccess{
			Path:   item.Path,
			Val:    v.Value.Value,
			Delete: v.Value.Deleted},
		locker)
	item.Result = vResultRetry
	return nil
}

func (t Algo) validateBackendReadNotFound(
	ctx context.Context,
	item *pathState,
	tx *Handle,
) error {
	meta, err := t.global.GetMetadata(ctx, item.Path)
	if errors.Is(err, backend.ErrNotFound) {
		// All good here, the item is still not found now.
		item.Result = vResultOK
		return nil
	}
	if err != nil {
		return err
	}
	// It was not found, but now it's there. There's only one valid
	// state: which is a non-committed lock-create.
	li, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}
	if li.Type != storage.LockTypeCreate {
		item.Result = vResultRetry
		return nil
	}
	if len(li.LockedBy) != 1 {
		return fmt.Errorf("bad lock: %q with %d lockers", li.Type, len(li.LockedBy))
	}
	status, err := t.mon.TxStatus(ctx, li.LockedBy[0])
	if err != nil {
		return err
	}
	if status == storage.TxCommitStatusOK {
		item.Result = vResultRetry
		return nil
	}
	item.Result = vResultOK
	return nil
}

func (t Algo) checkReadVersionUnlocked(rv ReadVersion, meta backend.Metadata) error {
	// Get info about the state of the lock.
	linfo, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}

	if rv.IsLocal() {
		// We read from cache. We don't have a well defined backend version to
		// check, but we can verify that the last writer is still actual.
		// The last writer committed and flushed and no other transaction locked it.
		sameLastWriter := linfo.LastWriter.Equal(rv.LastWriter) &&
			linfo.Type == storage.LockTypeNone
		// The last writer committed, but didn't flush yet.
		lockedByWriter := len(linfo.LockedBy) == 1 &&
			linfo.LockedBy[0].Equal(rv.LastWriter)

		if !sameLastWriter && !lockedByWriter {
			return ErrRetry
		}
		return nil
	}

	// We read directly from the backend. Make sure the item wasn't locked
	// and the read version is the same.
	if linfo.Type != storage.LockTypeNone || meta.Version.Contents != rv.Version {
		return ErrRetry
	}

	return nil
}

func (t Algo) parallelValidate(ctx context.Context, vstate *validationState, tx *Handle) error {
	t.log.Tracef("tx=%v, event=Commit parallel BEGIN", tx.id)
	// Parallel locking may lead to deadlock. Make sure we detect those
	// through a timeout and resort to serial locking and validation instead.
	ctx, cancel := concurr.ContextWithTimeout(ctx, t.clock, lockTimeout)
	defer cancel()

	err := func() error {
		// First lock the relevant collections if necessary.
		if err := t.lockCollections(ctx, vstate, tx); err != nil {
			return fmt.Errorf("locking collections: %w", err)
		}
		// Lock all the objects and validate.
		if err := t.lockValidate(ctx, vstate, tx); err != nil {
			return fmt.Errorf("failed validation: %w", err)
		}
		return nil
	}()

	if err != nil {
		if ctx.Err() != nil {
			return errLockTimeout
		}
		return err
	}
	return vstate.ToError()
}

func (t Algo) lockCollections(ctx context.Context, vstate *validationState, tx *Handle) error {
	colocks, err := collectionsLocks(vstate)
	if err != nil || len(colocks) == 0 {
		return err
	}

	err = t.fanout(ctx, len(colocks), func(ctx context.Context, i int) error {
		err := t.lockPath(ctx, colocks[i].Path, colocks[i].Type, tx.id)
		if err != nil {
			return fmt.Errorf("locking collection %q: %w", colocks[i].Path, err)
		}
		return nil
	})

	return err
}

func (t Algo) serialValidate(ctx context.Context, vstate *validationState, tx *Handle) error {
	t.log.Tracef("tx=%v, event=Commit serial BEGIN", tx.id)
	if !t.alreadyLocked(vstate, tx) {
		// If we need to lock anything, we need to do it in the right order.
		// To do that we need to first unlock.
		if err := t.unlockAll(ctx, tx); err != nil {
			return fmt.Errorf("unlocking before serial validate for tx %v: %w", tx.id, err)
		}
		// Reset validation state as well, as we had to unlock and lock again.
		for i := range vstate.Paths {
			item := &vstate.Paths[i]
			item.Result = vResultUnknown
		}
	}
	// It's important to lock all the paths in a well defined order.
	// First collections and then keys, both of which should be sorted, to avoid deadlocks.
	// Then we proceed with the validation in order.

	// Lock the collections first.
	colocks, err := collectionsLocks(vstate)
	if err != nil {
		return err
	}
	if len(colocks) > 0 {
		sort.Slice(colocks, func(i, j int) bool {
			return colocks[i].Path < colocks[j].Path
		})
		for _, cl := range colocks {
			if err := t.lockPath(ctx, cl.Path, cl.Type, tx.id); err != nil {
				return fmt.Errorf("locking collection %q: %w", cl.Path, err)
			}
		}
	}

	// Then sort the keys and validate them.
	sort.Slice(vstate.Paths, func(i, j int) bool {
		return vstate.Paths[i].Path < vstate.Paths[j].Path
	})

	for i := range vstate.Paths {
		item := &vstate.Paths[i]
		if err := t.lockValidateKey(ctx, item, tx); err != nil {
			return err
		}
	}

	return vstate.ToError()
}

func (t Algo) alreadyLocked(vstate *validationState, tx *Handle) bool {
	// Put together all the locks we need.
	needLocks := map[string]storage.LockType{}

	for _, ps := range vstate.Paths {
		pathLocks, _ := ps.NeedsLocks()
		for _, pl := range pathLocks {
			_, ok := needLocks[pl.Path]
			// We need to update the lock type if the new one is a write.
			if !ok || pl.Type == storage.LockTypeWrite {
				needLocks[pl.Path] = pl.Type
			}
		}
	}

	// Check whether we hold the right locks already.
	held := t.locker.LockedPaths(tx.id)
	for p, elt := range needLocks {
		for _, lp := range held {
			if lp.Path == p {
				// We need the expected lock, or we need to hold a lock write.
				if lp.Type != elt && lp.Type != storage.LockTypeWrite {
					return false
				}
				break
			}
		}
	}

	return true
}

func (t Algo) lockValidate(ctx context.Context, vstate *validationState, tx *Handle) error {
	err := t.fanout(ctx, len(vstate.Paths), func(ctx context.Context, i int) error {
		item := &vstate.Paths[i]
		return t.lockValidateKey(ctx, item, tx)
	})
	return err
}

func (t Algo) lockValidateKey(ctx context.Context, item *pathState, tx *Handle) error {
	if item.Result == vResultOK {
		// Avoid revalidating in case of partial retries.
		return nil
	}
	if item.NotFound {
		return t.lockValidateNotFoundKey(ctx, item, tx)
	}
	return t.lockValidateFoundKey(ctx, item, tx)
}

func (t Algo) lockValidateFoundKey(ctx context.Context, item *pathState, tx *Handle) error {
	// First thing, lock.
	var err error
	if item.Write {
		err = t.lockWrite(ctx, item.Path, tx.id)
	} else if item.Read {
		err = t.lockRead(ctx, item.Path, tx.id)
	}

	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			// The item is not there. Here we have two cases:
			// - If the item was read, we need to retry (stale read).
			// - If this was a blind write, we need to mark the item as not
			//   found and continue the validation.
			if item.Read {
				item.Result = vResultRetry
				return nil
			}
			item.NotFound = true
			if t.isKeyCollectionLocked(item.Path, storage.LockTypeWrite, tx) {
				// The collection is already locked. Proceed with validation.
				return t.lockValidateNotFoundKey(ctx, item, tx)
			}
			item.Result = vResultNeedsCLock
			return nil
		}
		// Terminate early in case of lock errors.
		return fmt.Errorf("failed locking: %w", err)
	}
	if !item.Read {
		item.Result = vResultOK
		return nil
	}

	// This was read. After locked, check the version.
	meta, err := t.reader.GetMetadata(ctx, item.Path, storage.MaxStaleness)
	if err != nil {
		return err
	}
	if !sameVersionAfterLock(item.ReadVersion, meta) {
		item.Result = vResultRetry
		return nil
	}
	item.Result = vResultOK
	return nil
}

func (t Algo) lockValidateNotFoundKey(ctx context.Context, item *pathState, tx *Handle) error {
	if item.Read && item.Write {
		// The item was read, not found and written to. We need to lock it
		// in write and check that it's still not found afterwards.
		// Lock create will do exactly that.
		err := t.lockCreate(ctx, item.Path, tx.id)
		if err != nil {
			if errors.Is(err, backend.ErrPrecondition) {
				// The item is there now, so the read is stale.
				item.Result = vResultRetry
				return nil
			}
			return err
		}
		item.Result = vResultOK
		return nil
	}

	if item.Read && !item.Write {
		// If the item was read and not found, we need to check that it's
		// still not present.
		_, err := t.global.GetMetadata(ctx, item.Path)
		if errors.Is(err, backend.ErrNotFound) {
			// All good, the item is still not found.
			item.Result = vResultOK
			return nil
		}
		if err != nil {
			// Not sure what went wrong. Stop the process.
			return err
		}
		// There's no error so the item is there now!
		// We may have a local read that was a delete. Just lock read now and validate.
		err = t.lockRead(ctx, item.Path, tx.id)
		if errors.Is(err, backend.ErrNotFound) {
			// All good. The item disappeared.
			item.Result = vResultOK
			return nil
		}
		if err != nil {
			return err
		}
		// We were able to lock the item in read. This means we need to retry.
		item.Result = vResultRetry
		return nil
	}

	if item.Write {
		// The item was written to, not read and it turned out to be not
		// found while locking it. We need to lock-create it.
		err := t.lockCreate(ctx, item.Path, tx.id)
		if err == nil {
			// All good.
			item.Result = vResultOK
			return nil
		}
		if !errors.Is(err, backend.ErrPrecondition) {
			return err
		}
		// The item was found now, so we should lock it in write normally instead.
		err = t.lockWrite(ctx, item.Path, tx.id)
		if err != nil {
			return err
		}
		item.Result = vResultOK
		return nil
	}

	return nil
}

func (t Algo) lockPath(
	ctx context.Context,
	path string,
	lt storage.LockType,
	tid data.TxID,
) error {
	var err error

	switch lt {
	case storage.LockTypeRead:
		err = t.lockRead(ctx, path, tid)
	case storage.LockTypeWrite:
		err = t.lockWrite(ctx, path, tid)
	case storage.LockTypeCreate:
		err = t.lockCreate(ctx, path, tid)
	default:
		return fmt.Errorf("unsupported lock type %v", lt)
	}
	if err != nil {
		return fmt.Errorf("locking path %q: %w", path, err)
	}

	return nil
}

func (t Algo) lockRead(
	ctx context.Context,
	key string,
	tid data.TxID,
) error {
	t.log.Tracef("tx=%v, path=%s, event=LockRead BEGIN", tid, key)
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.LockRead(ctx, key, tid)
	})
	t.log.Tracef("tx=%v, path=%s, event=LockRead END, err=%v", tid, key, err)
	return err
}

func (t Algo) lockWrite(
	ctx context.Context,
	key string,
	tid data.TxID,
) error {
	t.log.Tracef("tx=%v, path=%s, event=LockWrite BEGIN", tid, key)
	var err error
	trace.WithRegion(ctx, "lock-write", func() {
		err = t.locker.LockWrite(ctx, key, tid)
	})
	t.log.Tracef("tx=%v, path=%s, event=LockWrite END, err=%v", tid, key, err)
	return err
}

func (t Algo) lockCreate(
	ctx context.Context,
	key string,
	tid data.TxID,
) error {
	t.log.Tracef("tx=%v, path=%s, event=LockCreate BEGIN", tid, key)
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.LockCreate(ctx, key, tid)
	})
	t.log.Tracef("tx=%v, path=%s, event=LockCreate END, err=%v", tid, key, err)
	return err
}

func (t Algo) unlock(
	ctx context.Context,
	key string,
	tid data.TxID,
) error {
	t.log.Tracef("tx=%v, path=%s, event=Unlock BEGIN", tid, key)
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.Unlock(ctx, key, tid)
	})
	t.log.Tracef("tx=%v, path=%s, event=Unlock END, err=%v", tid, key, err)
	return err
}

func (t Algo) updateLocalCache(vstate validationState) {
	for _, ps := range vstate.Paths {
		if ps.Result == vResultRetry {
			t.local.MarkValueOutated(ps.Path, ps.ReadVersion)
		}
	}
}

func (t Algo) commitWrites(ctx context.Context, writes []WriteAccess, id data.TxID) error {
	defer trace.StartRegion(ctx, "commit-writes").End()

	tl, err := toLog(id, writes)
	if err != nil {
		return err
	}

	tl.Locks = t.locker.LockedPaths(id)
	if err := t.mon.CommitTx(ctx, tl); err != nil {
		return fmt.Errorf("creating transaction object: %w", err)
	}
	return nil
}

func (t Algo) updateLocal(w WriteAccess, tid data.TxID) {
	version := storage.Version{Writer: tid}
	if w.Delete {
		t.local.MarkDeleted(w.Path, version)
	} else {
		t.local.Write(w.Path, w.Val, version)
	}
}

func (t Algo) unlockAll(ctx context.Context, h *Handle) error {
	ps := t.locker.LockedPaths(h.id)
	if len(ps) == 0 {
		return nil
	}
	tid := h.id
	// Collect the errors here so that we don't stop the fanout even if some
	// unlocks fail.
	errs := make([]error, len(ps))

	// Unlock everything synchronously, but in parallel.
	_ = t.fanout(ctx, len(ps), func(ctx context.Context, i int) error {
		pl := ps[i]
		if err := t.unlock(ctx, pl.Path, tid); err != nil {
			errs[i] = fmt.Errorf("unlocking %q: %w", pl, err)
		}
		return nil
	})

	if err := errors.Combine(errs...); err != nil {
		return fmt.Errorf("unlocking all for tx %q: %v", tid, err)
	}
	return nil
}

func (t Algo) asyncCleanup(ctx context.Context, h *Handle) {
	if t.background == nil {
		// Background tasks are disabled.
		return
	}

	ps := t.locker.LockedPaths(h.id)
	tid := h.id

	if len(ps) == 0 {
		return
	}

	t.background.Go(ctx, func(ctx context.Context) {
		// Collect the errors here so that we don't stop the fanout even if some
		// unlocks fail.
		errs := make([]error, len(ps))

		// Do everything asynchronously.
		// First schedule a fanout on all the objects to unlock.
		f := concurr.NewFanout(backgroundConcurrency)
		// Make sure we don't have unbound background work here.
		ctx, cancel := concurr.ContextWithTimeout(ctx, t.clock, bgCleanupTimeout)
		defer cancel()

		w := f.Spawn(ctx, len(ps), func(ctx context.Context, i int) error {
			pl := ps[i]
			if err := t.unlock(ctx, pl.Path, tid); err != nil {
				errs[i] = fmt.Errorf("unlocking %q: %w", pl, err)
			}
			return nil
		})
		_ = w.Wait()
		if err := errors.Combine(errs...); err != nil {
			// Something went wrong during some unlocking. Skip the next phase.
			// Avoid logging if we are shutting down.
			if ctx.Err() == nil {
				t.log.Logf("tx=%v, event=AsyncCleanup, err=%v", tid, err)
			}
			return
		}
		// TODO: Delete transaction log when safe to do so.
	})
}

func (t Algo) fanout(
	ctx context.Context,
	num int,
	fn func(context.Context, int) error,
) error {
	if num == 0 {
		return nil
	}
	f := concurr.NewFanout(algoConcurrency)
	return f.Spawn(ctx, num, fn).Wait()
}

func (t Algo) isKeyCollectionLocked(key string, expected storage.LockType, h *Handle) bool {
	pr, err := paths.Parse(key)
	if err != nil {
		return false
	}
	cpath := paths.CollectionInfo(pr.Prefix)
	got := t.locker.LockType(cpath, h.id)
	return got == expected
}

func (t Algo) traceVstate(vstate validationState, h *Handle) {
	for _, p := range vstate.Paths {
		t.log.Tracef("tx=%v, path=%s, event=NewVstate, extra=readv:%+v;atype:%s",
			h.id, p.Path, p.ReadVersion, p.AccessType())
	}
}

type Logger interface {
	Logf(format string, v ...any)
	Tracef(format string, v ...any)
}

type Data struct {
	Reads  []ReadAccess
	Writes []WriteAccess
}

type ReadAccess struct {
	Path    string
	Version ReadVersion
	Found   bool
}

type ReadVersion struct {
	Version    int64
	LastWriter data.TxID
}

func (r ReadVersion) IsLocal() bool {
	return r.Version == 0
}

func (r ReadVersion) ToStorageVersion() storage.Version {
	return storage.Version{
		B:      backend.Version{Contents: r.Version},
		Writer: r.LastWriter,
	}
}

type WriteAccess struct {
	Path   string
	Val    []byte
	Delete bool
}

type Handle struct {
	data          Data
	status        statusType
	id            data.TxID
	requireLocks  bool
	serialLocking bool
}

type validationState struct {
	Paths []pathState
}

func (s validationState) ToError() error {
	var (
		retry      []string
		unknown    []string
		needsclock []string
	)

	for _, vp := range s.Paths {
		switch vp.Result {
		case vResultRetry:
			retry = append(retry, vp.Path)
		case vResultUnknown:
			unknown = append(unknown, vp.Path)
		case vResultNeedsCLock:
			needsclock = append(needsclock, vp.Path)
		}
	}

	// Retry wins on everything else.
	if len(retry) == 1 {
		return fmt.Errorf("stale read of %q: %w", retry[0], ErrRetry)
	}
	if len(retry) > 1 {
		return fmt.Errorf("stale read of %d/%d keys: %w",
			len(retry), len(s.Paths), ErrRetry)
	}
	// Unknown and needs collection locks warrant a validation retry.
	if len(unknown) > 0 || len(needsclock) > 0 {
		return errValidateRetry
	}
	return nil
}

type pathState struct {
	Path        string
	Read        bool
	Write       bool
	NotFound    bool
	Delete      bool
	ReadVersion storage.Version
	Result      vResult
}

func (p pathState) AccessType() accessType {
	return accessType(p)
}

func (p pathState) NeedsLocks() ([]storage.PathLock, error) {
	var lt storage.LockType
	switch {
	case p.Read:
		lt = storage.LockTypeRead
	case p.Write, p.Delete:
		lt = storage.LockTypeWrite
	default:
		return nil, nil
	}

	res := []storage.PathLock{{Path: p.Path, Type: lt}}
	if !p.NotFound {
		return res, nil
	}

	// Collection lock as well.
	pr, err := paths.Parse(p.Path)
	if err != nil {
		return nil, err
	}
	if pr.Type != paths.KeyType {
		return nil, fmt.Errorf("expected only keys while locking, got path %q", p.Path)
	}
	cpath := paths.CollectionInfo(pr.Prefix)

	return append(res, storage.PathLock{Path: cpath, Type: lt}), nil
}

func initValidation(h *Handle) *validationState {
	m := map[string]pathState{}
	for _, r := range h.data.Reads {
		i := m[r.Path]
		i.Path = r.Path
		i.Read = true
		i.ReadVersion = r.Version.ToStorageVersion()
		i.NotFound = !r.Found
		m[r.Path] = i
	}
	for _, w := range h.data.Writes {
		i := m[w.Path]
		i.Path = w.Path
		i.Write = true
		i.Delete = w.Delete
		m[w.Path] = i
	}

	res := make([]pathState, 0, len(m))
	for _, v := range m {
		res = append(res, v)
	}
	return &validationState{
		Paths: res,
	}
}

type accessType pathState

func (a accessType) String() string {
	res := []byte("----")
	if a.Read {
		res[0] = 'r'
	}
	if a.NotFound {
		res[1] = 'N'
	}
	if a.Write {
		res[2] = 'w'
	}
	if a.Delete {
		res[3] = 'D'
	}
	return string(res)
}

func isSingleRW(data Data) bool {
	if len(data.Reads) != 1 || len(data.Writes) != 1 {
		return false
	}
	if data.Reads[0].Path != data.Writes[0].Path {
		return false
	}
	return data.Reads[0].Found
}

func sameVersionAfterLock(v storage.Version, meta backend.Metadata) bool {
	return v.EqualMetaContents(meta)
}

func toLog(id data.TxID, writes []WriteAccess) (storage.TxLog, error) {
	tl := storage.TxLog{
		ID:     id,
		Status: storage.TxCommitStatusOK,
	}
	for _, w := range writes {
		tl.Writes = append(tl.Writes, storage.TxWrite{
			Path:    w.Path,
			Value:   w.Val,
			Deleted: w.Delete,
		})
	}
	return tl, nil
}

func collectionsLocks(vstate *validationState) ([]storage.PathLock, error) {
	locks := make(map[string]storage.LockType)

	for _, info := range vstate.Paths {
		if !info.NotFound && !info.Delete && info.Result != vResultNeedsCLock {
			// Both not found and deleted items require collection locks.
			continue
		}

		// Which lock do we need?
		pr, err := paths.Parse(info.Path)
		if err != nil {
			return nil, err
		}
		if pr.Type != paths.KeyType {
			return nil, fmt.Errorf("expected only keys while locking, got path %q", info.Path)
		}
		if info.Write {
			// No point in checking anything else. We know that the collection
			// needs a write lock as well.
			locks[pr.Prefix] = storage.LockTypeWrite
			continue
		}
		if !info.Read {
			continue
		}

		// The value was read and not found. We need at least a read lock.
		// If the prefix requires a write lock however, keep that one instead.
		if t, ok := locks[pr.Prefix]; ok && t == storage.LockTypeWrite {
			continue
		}
		locks[pr.Prefix] = storage.LockTypeRead
	}

	var res []storage.PathLock
	for p, t := range locks {
		res = append(res, storage.PathLock{
			Path: paths.CollectionInfo(p),
			Type: t,
		})
	}
	return res, nil
}
