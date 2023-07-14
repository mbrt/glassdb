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
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data"
)

type LockType int

func (v LockType) toTag() (string, error) {
	switch v {
	case LockTypeNone:
		return lockTagNone, nil
	case LockTypeRead:
		return lockTagRead, nil
	case LockTypeWrite:
		return lockTagWrite, nil
	case LockTypeCreate:
		return lockTagCreate, nil
	default:
		return "", fmt.Errorf("unknown lock type %q", v)
	}
}

const (
	LockTypeUnknown LockType = iota
	LockTypeNone
	LockTypeRead
	LockTypeWrite
	LockTypeCreate
)

// Tags.
const (
	lockedByTag   = "locked-by"
	lockTypeTag   = "lock-type"
	lastWriterTag = "last-writer"

	lockTagRead   = "r"
	lockTagWrite  = "w"
	lockTagCreate = "c"
	lockTagNone   = "-"
)

func NewLocker(g Global) Locker {
	return Locker{g}
}

type Locker struct {
	global Global
}

func (l Locker) UpdateLock(
	ctx context.Context,
	key string,
	expected backend.Version,
	update LockUpdate,
) error {
	if update.Type == LockTypeNone && update.Value.Deleted {
		if update.PrevType != LockTypeCreate && update.PrevType != LockTypeWrite {
			return fmt.Errorf("cannot delete from unlock type %v", update.PrevType)
		}
		return l.global.DeleteIf(ctx, key, expected)
	}
	isUnlockCreate := update.Type == LockTypeNone && update.PrevType == LockTypeCreate
	if isUnlockCreate && (len(update.Writer) == 0 || update.Value.NotWritten) {
		return l.global.DeleteIf(ctx, key, expected)
	}
	if expected.IsNull() {
		switch update.Type {
		case LockTypeNone:
			// This seems already unlocked. We have no easy way to enforce an
			// unlock if the item is already gone.
			return nil
		case LockTypeRead, LockTypeWrite:
			// We can't lock if there's no object.
			return backend.ErrNotFound
		}
	}

	ltype, err := update.Type.toTag()
	if err != nil {
		return err
	}
	var lockers []string
	for _, lk := range update.Lockers {
		lockers = append(lockers, tidToTag(lk))
	}

	newTags := backend.Tags{
		lockTypeTag: ltype,
		lockedByTag: strings.Join(lockers, ","),
	}
	if update.Type == LockTypeCreate {
		_, err := l.global.WriteIfNotExists(ctx, key, nil, newTags)
		return err
	}
	if len(update.Writer) > 0 && !update.Value.NotWritten {
		newTags[lastWriterTag] = tidToTag(update.Writer)
		_, err = l.global.WriteIf(ctx, key, update.Value.Value, expected, newTags)
		return err
	}
	_, err = l.global.SetTagsIf(ctx, key, expected, newTags)
	return err
}

func (l Locker) UnlockCreateUncommitted(
	ctx context.Context,
	key string,
	expected backend.Version,
) error {
	return l.UpdateLock(ctx, key, expected, LockUpdate{
		Type:     LockTypeNone,
		PrevType: LockTypeCreate,
	})
}

type LockUpdate struct {
	Type     LockType
	PrevType LockType
	Lockers  []data.TxID
	Writer   data.TxID
	Value    TValue
}

type TValue struct {
	Value   []byte
	Deleted bool
	// NotWritten is true when the transaction was committed but the value
	// was not written to (e.g. when only locked in read).
	NotWritten bool
}

type LockRequest struct {
	Type      LockType
	Lockers   []data.TxID
	Unlockers []data.TxID
}

type TxPathState struct {
	Tx     data.TxID
	Status TxCommitStatus
	Value  TValue
}

// LockOps represents what possible next update can be done to a lock.
//
// This includes the update, what transactions need to unlock before this can
// done and what effects the operation will have.
type LockOps struct {
	// Update is the possible next update on the lock. If nothing should be
	// updated, HasUpdate will be false.
	Update    LockUpdate
	HasUpdate bool
	// WaitFor is the list of transactions that need to unlock before any
	// operation can be performed.
	WaitFor []data.TxID
	// What effect these operations will have.
	LockedFor   []data.TxID
	UnlockedFor []data.TxID
}

func ComputeLockUpdate(curr LockInfo, req LockRequest, txs []TxPathState) (LockOps, error) {
	// First compute what to do about unlocks.
	unlockOps, err := computeUnlockUpdate(curr, req.Unlockers, txs)
	if err != nil {
		return LockOps{}, fmt.Errorf("computing unlocks: %w", err)
	}
	if len(req.Lockers) == 0 {
		return handleNoOps(curr, unlockOps), nil
	}
	// If the unlock result is a delete, return that.
	if unlockOps.HasUpdate && unlockOps.Update.Value.Deleted {
		return unlockOps, nil
	}

	// Take the result and feed it into the lock updates.
	if unlockOps.HasUpdate {
		// Update the current info to simulate that the unlocks already happened.
		curr.Type = unlockOps.Update.Type
		curr.LastWriter = unlockOps.Update.Writer
		curr.LockedBy = unlockOps.Update.Lockers
	}
	lockOps, err := computeLockUpdate(curr, req.Type, req.Lockers)
	if err != nil {
		return LockOps{}, fmt.Errorf("computing locks: %w", err)
	}
	if len(lockOps.WaitFor) > 0 && len(lockOps.UnlockedFor) > 0 {
		// Instead of waiting, return the unlock update.
		return handleNoOps(curr, unlockOps), nil
	}
	// Merge lock and unlock updates.
	ops := LockOps{
		Update:      unlockOps.Update, // Start with unlock ops.
		HasUpdate:   unlockOps.HasUpdate || lockOps.HasUpdate,
		WaitFor:     lockOps.WaitFor,
		LockedFor:   lockOps.LockedFor,
		UnlockedFor: unlockOps.UnlockedFor,
	}
	if !lockOps.HasUpdate {
		return handleNoOps(curr, ops), nil
	}
	// Merge lockOps into it.
	ops.Update.Type = lockOps.Update.Type
	ops.Update.Lockers = lockOps.Update.Lockers
	return ops, nil
}

func computeUnlockUpdate(curr LockInfo, unlockers []data.TxID, txs []TxPathState) (LockOps, error) {
	// Check whether we are already unlocked.
	if curr.Type == LockTypeNone {
		return LockOps{UnlockedFor: unlockers}, nil
	}

	unlockersSet := data.TxIDSet(unlockers)
	lockedSet := data.TxIDSet(curr.LockedBy)
	alreadyUnlocked := data.TxIDSetDiff(unlockersSet, lockedSet)

	// Compute what's left after the unlocks.
	update := LockUpdate{
		Type:     curr.Type,
		PrevType: curr.Type,
	}
	unlockedFor := []data.TxID(alreadyUnlocked)

	for _, tx := range curr.LockedBy {
		v, ok := findTxPathState(txs, tx)
		if !ok {
			return LockOps{}, fmt.Errorf("missing state for tx %v", tx)
		}
		if !v.Status.IsFinal() && !unlockersSet.Contains(tx) {
			update.Lockers = append(update.Lockers, tx)
			continue
		}
		// This is unlocked.
		unlockedFor = append(unlockedFor, tx)
		if isWriterType(curr.Type) && v.Status == TxCommitStatusOK {
			// This was written by the transaction.
			update.Value = v.Value
			update.Writer = tx
		}
	}
	if len(update.Lockers) == 0 {
		update.Type = LockTypeNone
	}

	return LockOps{
		Update:      update,
		HasUpdate:   len(unlockedFor) > 0,
		UnlockedFor: unlockedFor,
	}, nil
}

func computeLockUpdate(curr LockInfo, lt LockType, lockers []data.TxID) (LockOps, error) {
	if lt == LockTypeUnknown || lt == LockTypeNone {
		return LockOps{}, fmt.Errorf("cannot lock with type %v", lt)
	}
	if isWriterType(lt) && len(lockers) != 1 {
		return LockOps{}, fmt.Errorf("cannot lock in write with %d lockers", len(lockers))
	}

	// Return early with requests that need no updates.
	// TODO: Do not return, but merge with the next updates.
	if curr.Type == lt {
		alreadyLocked := tidsIntersect(curr.LockedBy, lockers)
		if len(alreadyLocked) > 0 {
			return LockOps{LockedFor: alreadyLocked}, nil
		}
	}
	// LockCreate is special: We don't need to wait for anything, as it will
	// fail if the object is already there.
	if lt == LockTypeCreate {
		return LockOps{
			Update: LockUpdate{
				Type:    LockTypeCreate,
				Lockers: lockers,
			},
			HasUpdate: true,
			LockedFor: lockers,
		}, nil
	}

	// Check whether this is a lock upgrade.
	if lt == LockTypeWrite && curr.Type == LockTypeRead &&
		len(curr.LockedBy) == 1 && curr.LockedBy[0].Equal(lockers[0]) {
		// We know that we can hijack the lock, as we are the only readers.
		return LockOps{
			Update: LockUpdate{
				Type:    LockTypeWrite,
				Lockers: lockers,
			},
			HasUpdate: true,
			LockedFor: lockers,
		}, nil
	}

	// Check whether we need to wait for the current lockers.
	if isWriterType(curr.Type) || (curr.Type == LockTypeRead && isWriterType(lt)) {
		return LockOps{
			WaitFor: curr.LockedBy,
		}, nil
	}

	// We should be able to lock.
	// Make a copy to avoid changing the current lock info.
	lockersCopy := append([]data.TxID(nil), curr.LockedBy...)

	return LockOps{
		Update: LockUpdate{
			Type:     lt,
			PrevType: curr.Type,
			Lockers:  append(lockersCopy, lockers...),
		},
		HasUpdate: true,
		LockedFor: lockers,
	}, nil
}

func handleNoOps(curr LockInfo, ops LockOps) LockOps {
	if len(ops.LockedFor) == 0 && len(ops.UnlockedFor) == 0 {
		// Nothing happened.
		return ops
	}
	if ops.HasUpdate {
		// We are going to ask for an update. No danger.
		return ops
	}
	// We are saying that we are locking or unlocking, _but_ not asking for any
	// change in the backend.
	// This is prone to out-of-order changes breaking our invariants. To force
	// those to fail, we need to make an update setting the same state as
	// before.
	ops.HasUpdate = true
	ops.Update = LockUpdate{
		Type:    curr.Type,
		Lockers: curr.LockedBy,
	}
	return ops
}

func findTxPathState(txs []TxPathState, tid data.TxID) (TxPathState, bool) {
	for _, s := range txs {
		if s.Tx.Equal(tid) {
			return s, true
		}
	}
	return TxPathState{}, false
}

func tidsIntersect(a, b []data.TxID) []data.TxID {
	return []data.TxID(data.TxIDSetIntersect(
		data.TxIDSet(a),
		data.TxIDSet(b),
	))
}

func isWriterType(lt LockType) bool {
	return lt == LockTypeCreate || lt == LockTypeWrite
}

type LockInfo struct {
	Type       LockType
	LockedBy   []data.TxID
	LastWriter data.TxID
}

func (v LockInfo) Valid() error {
	if len(v.LockedBy) == 0 {
		if v.Type != LockTypeNone {
			return errors.New("got zero lockers, but lock type is not none")
		}
		return nil
	}
	if len(v.LockedBy) > 1 && (v.Type == LockTypeCreate || v.Type == LockTypeWrite) {
		return fmt.Errorf("got %d lockers with writer lock", len(v.LockedBy))
	}
	// TODO: Validate whether there are duplicate lockers.
	return nil
}

// TagsLockInfo parses tags managing locks on an object.
func TagsLockInfo(tags backend.Tags) (LockInfo, error) {
	res := LockInfo{
		Type: LockTypeNone,
	}

	// LockType.
	if v, ok := tags[lockTypeTag]; ok {
		switch v {
		case lockTagRead:
			res.Type = LockTypeRead
		case lockTagWrite:
			res.Type = LockTypeWrite
		case lockTagCreate:
			res.Type = LockTypeCreate
		case lockTagNone:
		case "":
		default:
			return res, fmt.Errorf("unknown lock type %q", v)
		}
	}
	// LockedBy.
	if v, ok := tags[lockedByTag]; ok && v != "" {
		for _, lt := range strings.Split(v, ",") {
			d, err := tagToTid(lt)
			if err != nil {
				return res, fmt.Errorf("invalid locked-by tag: %w", err)
			}
			res.LockedBy = append(res.LockedBy, d)
		}
	}
	// LastWriter.
	lw, err := lastWriterFromTags(tags)
	if err != nil {
		return res, nil
	}
	res.LastWriter = lw

	return res, nil
}

func lastWriterFromTags(tags backend.Tags) (data.TxID, error) {
	v, ok := tags[lastWriterTag]
	if !ok {
		return nil, nil
	}
	res, err := tagToTid(v)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func tagToTid(a string) (data.TxID, error) {
	return base64.URLEncoding.DecodeString(a)
}

func tidToTag(t data.TxID) string {
	return base64.URLEncoding.EncodeToString(t)
}
