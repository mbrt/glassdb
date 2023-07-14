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
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
)

const (
	// Make sure we refresh metadata now and then, to avoid getting stuck retrying
	// for something that doesn't make sense anymore.
	metaMaxStaleness = 10 * time.Second
	// Make sure we sometimes poll when we are stuck waiting for transactions to finish.
	waitPollDuration = 5 * time.Second
)

// txState represents how TLocker thinks about a transaction.
type txState int

const (
	txStateUnknown txState = iota
	txStateExisting
)

func NewLocker(
	l storage.Local,
	g storage.Global,
	tl storage.TLogger,
	clock clockwork.Clock,
	tmon *Monitor,
) *Locker {
	res := &Locker{
		local:  l,
		global: g,
		tl:     tl,
		clock:  clock,
		tmon:   tmon,
		tlocks: make(map[string]txLocks),
	}
	res.dedup = concurr.NewDedup(lockerWorker{res})
	return res
}

// Locker implements locking on top of the global storage.
//
// Waits and retries are handled transparently.
type Locker struct {
	local  storage.Local
	global storage.Global
	tl     storage.TLogger
	tmon   *Monitor
	clock  clockwork.Clock

	dedup  *concurr.Dedup
	tlocks map[string]txLocks
	m      sync.Mutex

	nCalls   int32
	nHits    int32
	nRetries int32
}

func (v *Locker) LockRead(ctx context.Context, key string, tid data.TxID) error {
	return v.pushRequest(ctx, key, storage.LockTypeRead, tid)
}

func (v *Locker) LockWrite(ctx context.Context, key string, tid data.TxID) error {
	return v.pushRequest(ctx, key, storage.LockTypeWrite, tid)
}

func (v *Locker) LockCreate(ctx context.Context, key string, tid data.TxID) error {
	return v.pushRequest(ctx, key, storage.LockTypeCreate, tid)
}

func (v *Locker) Unlock(ctx context.Context, key string, tid data.TxID) error {
	return v.pushRequest(ctx, key, storage.LockTypeNone, tid)
}

func (v *Locker) LockType(key string, tid data.TxID) storage.LockType {
	v.m.Lock()
	defer v.m.Unlock()

	locks, ok := v.tlocks[string(tid)]
	if !ok {
		return storage.LockTypeNone
	}
	lt, ok := locks[key]
	if !ok {
		return storage.LockTypeNone
	}
	return lt
}

func (v *Locker) LockedPaths(tid data.TxID) []storage.PathLock {
	v.m.Lock()
	defer v.m.Unlock()

	locks, ok := v.tlocks[string(tid)]
	if !ok {
		return nil
	}
	res := make([]storage.PathLock, 0, len(locks))
	for path, tp := range locks {
		res = append(res, storage.PathLock{
			Path: path,
			Type: tp,
		})
	}
	return res
}

func (v *Locker) StatsAndReset() LockStats {
	return LockStats{
		Calls:   int(atomic.SwapInt32(&v.nCalls, 0)),
		Hits:    int(atomic.SwapInt32(&v.nHits, 0)),
		Retries: int(atomic.SwapInt32(&v.nRetries, 0)),
	}
}

func (v *Locker) pushRequest(ctx context.Context, key string, lt storage.LockType, tid data.TxID) error {
	var err error

	atomic.AddInt32(&v.nCalls, 1)
	txs, nproc := v.needsProcessing(key, tid, lt)
	if !nproc {
		atomic.AddInt32(&v.nHits, 1)
		return nil
	}
	if txs == txStateUnknown {
		// Notify TxMonitor that we're going to start locking from this tx.
		// We'll need refresh logs to keep the locks alive from now on.
		v.tmon.RefreshTx(ctx, tid)
	}

	var (
		lockers   []data.TxID
		unlockers []data.TxID
	)
	if lt == storage.LockTypeNone {
		unlockers = []data.TxID{tid}
	} else {
		lockers = []data.TxID{tid}
	}

	err = v.dedup.Do(ctx, key, lockRequest{
		Type:      lt,
		Lockers:   lockers,
		Unlockers: unlockers,
	})

	lockUpdated := err == nil
	if err != nil && !errors.Is(err, backend.ErrPrecondition) {
		// In case of timeout, we don't know whether the operation was actually
		// completed or not. To be safe, we mark this as unknown.
		lt = storage.LockTypeUnknown
		lockUpdated = true
	}

	if lockUpdated {
		v.updateTxLocks(key, tid, lt)
	}
	return err
}

func (v *Locker) needsProcessing(key string, tid data.TxID, lt storage.LockType) (txState, bool) {
	v.m.Lock()
	defer v.m.Unlock()

	st := txStateUnknown
	txl, ok := v.tlocks[string(tid)]
	if ok {
		st = txStateExisting
	}
	if !ok && lt == storage.LockTypeNone {
		// We don't know the transaction so there's nothing to unlock.
		return st, false
	}
	glt, ok := txl[key]
	if !ok && lt == storage.LockTypeNone {
		return st, false
	}
	return st, glt != lt
}

func (v *Locker) updateTxLocks(key string, tid data.TxID, lt storage.LockType) {
	v.m.Lock()
	defer v.m.Unlock()

	if lt == storage.LockTypeNone {
		// We need to delete the item.
		txl, ok := v.tlocks[string(tid)]
		if !ok {
			return
		}
		delete(txl, key)
		if len(txl) == 0 {
			// Delete transactions with no locked items.
			delete(v.tlocks, string(tid))
		}
		return
	}

	txl, ok := v.tlocks[string(tid)]
	if !ok {
		txl = make(txLocks)
		v.tlocks[string(tid)] = txl
	}
	txl[key] = lt
}

func (v *Locker) doLockOp(ctx context.Context, key string, req storage.LockRequest) (lockOpResult, error) {
	ldata, err := v.fetchLockInfo(ctx, key)
	if err != nil {
		return lockOpResult{}, err
	}

	txs, err := v.fetchLockersState(ctx, key, ldata.Info)
	if err != nil {
		return lockOpResult{}, err
	}
	ops, err := storage.ComputeLockUpdate(ldata.Info, req, txs)
	if err != nil {
		return lockOpResult{}, fmt.Errorf("computing unlock update: %w", err)
	}
	if len(ops.WaitFor) > 0 {
		return lockOpResult{WaitForTx: ops.WaitFor}, nil
	}
	if ops.HasUpdate {
		locker := storage.NewLocker(v.global)
		err := locker.UpdateLock(ctx, key, ldata.Version, ops.Update)
		if err != nil {
			return lockOpResult{}, fmt.Errorf("updating lock: %w", err)
		}
	}
	return lockOpResult{
		LockedFor:   ops.LockedFor,
		UnlockedFor: ops.UnlockedFor,
	}, nil
}

func (v *Locker) fetchLockInfo(ctx context.Context, key string) (lockData, error) {
	reader := NewReader(v.local, v.global, v.tmon)
	meta, err := reader.GetMetadata(ctx, key, metaMaxStaleness)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return lockData{
				Info: storage.LockInfo{
					Type: storage.LockTypeNone,
				},
			}, nil
		}
		return lockData{}, err
	}
	info, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return lockData{}, err
	}
	if err := info.Valid(); err != nil {
		return lockData{}, err
	}
	return lockData{
		Info:    info,
		Version: meta.Version,
	}, nil
}

func (v *Locker) fetchLockersState(ctx context.Context, key string, info storage.LockInfo) ([]storage.TxPathState, error) {
	if info.Type == storage.LockTypeCreate || info.Type == storage.LockTypeWrite {
		tx := info.LockedBy[0]
		tv, err := v.tmon.CommittedValue(ctx, key, tx)
		if err != nil {
			return nil, fmt.Errorf("getting committed value from tx %v", tx)
		}
		return []storage.TxPathState{
			{
				Tx:     tx,
				Status: tv.Status,
				Value:  tv.Value,
			},
		}, nil
	}

	// Avoid expensive work of fetching CommittedValue if it's locked in read.
	// The transaction status is enough.
	var txs []storage.TxPathState
	for _, tx := range info.LockedBy {
		status, err := v.tmon.TxStatus(ctx, tx)
		if err != nil {
			return txs, fmt.Errorf("getting tx status for %v", tx)
		}
		txs = append(txs, storage.TxPathState{
			Tx:     tx,
			Status: status,
		})
	}
	return txs, nil
}

type LockStats struct {
	Calls   int
	Hits    int
	Retries int
}

type lockerWorker struct {
	locker *Locker
}

func (l lockerWorker) Work(ctx context.Context, key string, cntr concurr.DedupContr) error {
	counter := 0
	wctx := &waitCtx{clock: l.locker.clock}

	defer func() {
		if counter > 1 {
			atomic.AddInt32(&l.locker.nRetries, int32(counter-1))
		}
	}()

	for {
		counter++
		req := cntr.Request(key).(lockRequest)

		lockRes, err := l.locker.doLockOp(ctx, key, storage.LockRequest(req))
		if err != nil {
			if errors.Is(err, backend.ErrPrecondition) {
				// The lock info was outdated.
				if req.Type != storage.LockTypeCreate {
					// Force a reload.
					_, _ = l.locker.global.GetMetadata(ctx, key)
					continue
				}
				// There's nothing more we can do here. The object is there
				// and we can't lock it in create.
			}
			return err
		}
		if len(lockRes.WaitForTx) > 0 {
			if err := l.waitForTx(ctx, key, lockRes.WaitForTx, wctx, cntr); err != nil {
				return err
			}
			// Try again after waiting.
			continue
		}
		if !isComplete(storage.LockRequest(req), lockRes) {
			// We weren't able to do everything. Let's do this again.
			continue
		}

		return nil
	}
}

func (l lockerWorker) waitForTx(ctx context.Context, key string, txs []data.TxID, wctx *waitCtx, cntr concurr.DedupContr) error {
	for _, tx := range txs {
		status, err := l.locker.tmon.TxStatus(ctx, tx)
		if err != nil {
			return err
		}
		if status.IsFinal() {
			continue
		}

		if wctx.nextReqCh == nil {
			wctx.nextReqCh = cntr.OnNextDo(key)
		}

		// Wait for any of these events to happen.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.locker.tmon.WaitForTx(ctx, tx):
		case <-wctx.nextReqCh:
		case <-wctx.Timer().Chan():
		}

		break
	}
	return nil
}

type lockRequest storage.LockRequest

func (l lockRequest) Merge(other concurr.Request) (concurr.Request, bool) {
	return mergeRequests(l, other.(lockRequest))
}

func (l lockRequest) CanReorder() bool {
	return l.Type == storage.LockTypeNone
}

func mergeRequests(r1, r2 lockRequest) (lockRequest, bool) {
	if r1.Type == storage.LockTypeNone {
		// To simplify logic, keep r1 as being the one dictating the lock type.
		// Unlocks should stay in r2 (unless both are unlocks).
		r1, r2 = r2, r1
	}

	switch {
	case r1.Type == storage.LockTypeCreate, r2.Type == storage.LockTypeCreate:
		// LockTypeCreate goes alone.
		return lockRequest{}, false
	case r2.Type == storage.LockTypeNone:
	case r1.Type == storage.LockTypeRead && r2.Type == storage.LockTypeRead:
	default:
		// No other merges are possible.
		return lockRequest{}, false
	}

	// These are mergeable.
	return lockRequest{
		Type: r1.Type,
		Lockers: []data.TxID(data.TxIDSetUnion(
			data.TxIDSet(r1.Lockers), data.TxIDSet(r2.Lockers))),
		Unlockers: []data.TxID(data.TxIDSetUnion(
			data.TxIDSet(r1.Unlockers), data.TxIDSet(r2.Unlockers))),
	}, true
}

func isComplete(req storage.LockRequest, res lockOpResult) bool {
	toLock := data.TxIDSet(req.Lockers)
	toUnlock := data.TxIDSet(req.Unlockers)
	locked := data.TxIDSet(res.LockedFor)
	unlocked := data.TxIDSet(res.UnlockedFor)

	return len(data.TxIDSetDiff(toLock, locked)) == 0 &&
		len(data.TxIDSetDiff(toUnlock, unlocked)) == 0
}

type lockData struct {
	Info    storage.LockInfo
	Version backend.Version
}

type lockOpResult struct {
	LockedFor   []data.TxID
	UnlockedFor []data.TxID
	WaitForTx   []data.TxID
}

type waitCtx struct {
	clock     clockwork.Clock
	timer     clockwork.Timer
	nextReqCh <-chan struct{}
}

func (w *waitCtx) Timer() clockwork.Timer {
	if w.timer == nil {
		w.timer = w.clock.NewTimer(waitPollDuration)
		return w.timer
	}
	w.timer.Reset(waitPollDuration)
	return w.timer
}

func (w *waitCtx) Close() {
	if w.timer != nil {
		w.timer.Stop()
	}
}

// txLocks maps a path to its current lock.
type txLocks map[string]storage.LockType
