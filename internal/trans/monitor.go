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
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
)

const (
	pendingTxTimeout  = 15 * time.Second
	maxClockSkew      = 30 * time.Second
	refreshMultiplier = 0.5
	refreshChanCap    = 10 // TODO: Compute a better default capacity.
)

var ErrAlreadyFinalized = errors.New("transaction was already finalized")

type refreshState int

const (
	refreshStateNotStarted refreshState = iota
	refreshStateRunning
	refreshStateStopped
)

func NewMonitor(
	c clockwork.Clock,
	l storage.Local,
	tl storage.TLogger,
	b *concurr.Background,
) *Monitor {
	m := &Monitor{
		clock:      c,
		local:      l,
		tl:         tl,
		background: b,
		localTx:    make(map[string]txStatus),
		waiters:    make(map[string][]waitRequest),
		unknownTx:  make(map[string]txUnknown),
	}
	m.spawnPendingRefresher()
	return m
}

type Monitor struct {
	clock      clockwork.Clock
	local      storage.Local
	tl         storage.TLogger
	background *concurr.Background
	localTx    map[string]txStatus
	waiters    map[string][]waitRequest
	unknownTx  map[string]txUnknown
	refreshCh  chan<- txDeadline
	m          sync.Mutex
}

func (m *Monitor) BeginTx(_ context.Context, tid data.TxID) {
	m.m.Lock()
	m.localTx[string(tid)] = txStatus{
		Status:       storage.TxCommitStatusPending,
		RefreshState: refreshStateNotStarted,
	}
	m.m.Unlock()
}

func (m *Monitor) RefreshTx(ctx context.Context, tid data.TxID) {
	needStart := false

	m.m.Lock()
	st, ok := m.localTx[string(tid)]
	if ok && st.RefreshState == refreshStateNotStarted {
		needStart = true
		st.RefreshState = refreshStateRunning
		m.localTx[string(tid)] = st
	}
	m.m.Unlock()

	if needStart && !m.background.Closed() {
		m.refreshCh <- txDeadline{
			ID:         tid,
			Deadline:   newTxDeadline(m.clock.Now()),
			RefreshCtx: ctx,
		}
	}
}

func (m *Monitor) CommitTx(ctx context.Context, tl storage.TxLog) error {
	m.m.Lock()
	m.stopTxRefresh(tl.ID)
	m.m.Unlock()

	// We can optimize here. If nothing was locked (i.e. RO or single-W tx), we
	// can avoid to write the transaction log.
	if len(tl.Locks) > 0 {
		tl.Status = storage.TxCommitStatusOK
		if err := m.setFinalLog(ctx, tl); err != nil {
			return fmt.Errorf("writing tx log: %w", err)
		}
	} else if len(tl.Writes) > 1 {
		return fmt.Errorf("got %d writes with no locks; this is a bug", len(tl.Writes))
	}

	// Update local storage with the new values.
	version := storage.Version{Writer: tl.ID}
	for _, entry := range tl.Writes {
		if entry.Deleted {
			m.local.MarkDeleted(entry.Path, version)
		} else {
			m.local.Write(entry.Path, entry.Value, version)
		}
	}

	m.m.Lock()
	delete(m.localTx, string(tl.ID))
	m.notifyWaiters(tl.ID, WaitTxResult{Status: storage.TxCommitStatusOK})
	m.m.Unlock()

	return nil
}

func (m *Monitor) AbortTx(ctx context.Context, tid data.TxID) error {
	m.m.Lock()
	m.stopTxRefresh(tid)
	m.m.Unlock()

	err := m.setFinalLog(ctx, storage.TxLog{
		ID:     tid,
		Status: storage.TxCommitStatusAborted,
	})

	m.m.Lock()
	delete(m.localTx, string(tid))
	m.notifyWaiters(tid, WaitTxResult{Status: storage.TxCommitStatusAborted})
	m.m.Unlock()

	return err
}

func (m *Monitor) TxStatus(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	m.m.Lock()
	if s, ok := m.localTx[string(tid)]; ok {
		m.m.Unlock()
		return s.Status, nil
	}
	m.m.Unlock()

	return m.fetchRemoteTxStatus(ctx, tid)
}

// WaitForTx waits asynchronously for the given transaction to complete.
//
// The given context may not be used if other callers are are waiting for the same
// transaction.
func (m *Monitor) WaitForTx(ctx context.Context, tid data.TxID) <-chan WaitTxResult {
	ch := make(chan WaitTxResult, 1)

	m.m.Lock()
	defer m.m.Unlock()

	txs, isLocal := m.localTx[string(tid)]
	s := txs.Status
	if isLocal && s == storage.TxCommitStatusOK || s == storage.TxCommitStatusAborted {
		// The value is already available.
		ch <- WaitTxResult{
			Status: s,
			Err:    nil,
		}
		return ch
	}

	if ws, ok := m.waiters[string(tid)]; ok {
		// There is already a worker waiting for the request.
		// Enqueue ourselves in the waiters.
		m.waiters[string(tid)] = append(ws, waitRequest{
			Ctx: ctx,
			Ch:  ch,
		})
		return ch
	}

	// Nobody is waiting yet.
	if isLocal {
		// No need to spawn a worker, as the transition is local.
		// We can just enqueue ourselves until we get notified through CommitTx
		// or AbortTx.
		m.waiters[string(tid)] = []waitRequest{{Ctx: ctx, Ch: ch}}
		return ch
	}

	// This is a remote transaction. Let's spawn a worker to poll its log.
	// Make sure to initialize the waiters while locked (empty list).
	m.waiters[string(tid)] = nil

	go func(ctx context.Context) {
		// Start with the given context. Update it below.

		for {
			status, err := m.pollTxStatus(ctx, tid)
			if err == nil || ctx.Err() == nil {
				// We got a status or a problem.
				res := WaitTxResult{
					Status: status,
					Err:    err,
				}

				m.m.Lock()
				m.notifyWaiters(tid, res)
				m.m.Unlock()

				ch <- res
				return
			}

			// The context expired. Try and see wether somebody else is
			// interested in this tx.
			m.m.Lock()
			w, ok := m.nextWaiter(tid)
			m.m.Unlock()
			if !ok {
				// Nothing more to do.
				return
			}

			// Use the context from the first valid waiter.
			ctx = w.Ctx
		}
	}(ctx)

	return ch
}

func (m *Monitor) CommittedValue(ctx context.Context, key string, tid data.TxID) (KeyCommitStatus, error) {
	// TODO: How does this play with the "no-log" optimization?

	// If we just committed the value, we should have it in local storage.
	lr, ok := m.local.Read(key, storage.MaxStaleness)
	if ok && tid.Equal(lr.Version.Writer) {
		return KeyCommitStatus{
			Status: storage.TxCommitStatusOK,
			Value: storage.TValue{
				Value:   lr.Value,
				Deleted: lr.Deleted,
			},
		}, nil
	}

	// Otherwise check the status.
	status, err := m.TxStatus(ctx, tid)
	if err != nil {
		return KeyCommitStatus{}, err
	}
	if status != storage.TxCommitStatusOK {
		return KeyCommitStatus{Status: status}, nil
	}

	// It was committed. Fetch the value.
	tl, err := m.tl.Get(ctx, tid)
	if err != nil {
		return KeyCommitStatus{}, fmt.Errorf("getting TID %q: %w", tid, err)
	}
	for _, entry := range tl.Writes {
		if entry.Path == key {
			return KeyCommitStatus{
				Status: storage.TxCommitStatusOK,
				Value: storage.TValue{
					Value:   entry.Value,
					Deleted: entry.Deleted,
				},
			}, nil
		}
	}
	// We didn't find the value. The transaction didn't change it.
	return KeyCommitStatus{
		Status: storage.TxCommitStatusOK,
		Value:  storage.TValue{NotWritten: true},
	}, nil
}

func (m *Monitor) fetchRemoteTxStatus(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	status, err := m.tl.CommitStatus(ctx, tid)
	if err != nil {
		return storage.TxCommitStatusUnknown, err
	}

	switch status.Status {
	case storage.TxCommitStatusUnknown:
		return m.handleUnknownTx(ctx, tid)
	case storage.TxCommitStatusPending:
		if isExpired(status.LastUpdate, m.clock.Now()) {
			return m.tryAbortRemoteTx(ctx, tid, status.Version)
		}
		return storage.TxCommitStatusPending, nil
	}

	return status.Status, nil
}

func (m *Monitor) handleUnknownTx(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	now := m.clock.Now()

	m.m.Lock()
	st, ok := m.unknownTx[string(tid)]
	if !ok {
		m.unknownTx[string(tid)] = txUnknown{FirstCheck: now}
		m.m.Unlock()
		return storage.TxCommitStatusPending, nil
	}
	m.m.Unlock()

	if isExpired(st.FirstCheck, now) {
		st, err := m.tryAbortRemoteTx(ctx, tid, backend.Version{})
		if err == nil {
			// We now have a permanent state. We can remove the tx from the map.
			m.m.Lock()
			delete(m.unknownTx, string(tid))
			m.m.Unlock()
		}
		return st, err
	}
	return storage.TxCommitStatusPending, nil
}

func (m *Monitor) tryAbortRemoteTx(
	ctx context.Context,
	tid data.TxID,
	expected backend.Version,
) (storage.TxCommitStatus, error) {
	var err error
	tlog := storage.TxLog{
		ID:     tid,
		Status: storage.TxCommitStatusAborted,
	}

	if expected.IsNull() {
		_, err = m.tl.Set(ctx, tlog)
	} else {
		_, err = m.tl.SetIf(ctx, tlog, expected)
	}
	if errors.Is(err, backend.ErrPrecondition) {
		// There should be a new log now.
		st, err := m.tl.CommitStatus(ctx, tid)
		return st.Status, err
	}
	if err != nil {
		return storage.TxCommitStatusUnknown, err
	}

	return storage.TxCommitStatusAborted, nil
}

func (m *Monitor) pollTxStatus(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	res := storage.TxCommitStatusUnknown

	err := concurr.RetryWithBackoff(ctx, m.clock, func() error {
		s, err := m.fetchRemoteTxStatus(ctx, tid)
		if err != nil {
			return concurr.Permanent(fmt.Errorf("in get commit status: %w", err))
		}

		res = s
		if s == storage.TxCommitStatusOK || s == storage.TxCommitStatusAborted {
			return nil
		}
		return errors.New("retry polling tx status")
	})

	return res, err
}

func (m *Monitor) nextWaiter(tid data.TxID) (waitRequest, bool) {
	ws, ok := m.waiters[string(tid)]
	if !ok {
		return waitRequest{}, false
	}

	// Find the first non expired waiter.
	i := 0
	for ; i < len(ws) && ws[i].Ctx.Err() != nil; i++ {
	}
	// Trim the expired waiters.
	if i > 0 {
		ws = ws[i:]
		m.waiters[string(tid)] = ws
	}
	if len(ws) == 0 {
		return waitRequest{}, false
	}

	return ws[0], true
}

func (m *Monitor) notifyWaiters(tid data.TxID, res WaitTxResult) {
	ws, ok := m.waiters[string(tid)]
	if !ok {
		return
	}
	for _, w := range ws {
		if w.Ctx.Err() == nil {
			w.Ch <- res
		}
	}
	delete(m.waiters, string(tid))
}

func (m *Monitor) setFinalLog(ctx context.Context, tlog storage.TxLog) error {
	tid := tlog.ID
	if tid == nil {
		return errors.New("missing required tlog ID")
	}

	m.m.Lock()
	stat := m.localTx[string(tid)]
	lastV := stat.LastVersion
	m.m.Unlock()

	// TODO: Add timeout ~= refreshTimeout here.
	err := concurr.RetryWithBackoff(ctx, m.clock, func() error {
		var err error
		if lastV.IsNull() {
			_, err = m.tl.Set(ctx, tlog)
		} else {
			_, err = m.tl.SetIf(ctx, tlog, lastV)
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, backend.ErrPrecondition) {
			// Perhaps there was a race condition on 'setLog'.
			// Fetch the last version one more time and retry.
			st, cserr := m.tl.CommitStatus(ctx, tid)
			if cserr != nil {
				return concurr.Permanent(cserr)
			}
			if st.Status.IsFinal() {
				return concurr.Permanent(
					fmt.Errorf("log status %v: %w", st.Status, ErrAlreadyFinalized))
			}
			lastV = st.Version
			return err
		}
		return concurr.Permanent(err)
	})

	return err
}

func (m *Monitor) spawnPendingRefresher() {
	out, in := concurr.MakeChanInfCap[txDeadline](refreshChanCap)
	m.refreshCh = in

	// Yes, background context is enough. We are not making any network calls with it.
	// Cancellation is completely managed through m.background.
	m.background.Go(context.Background(), func(ctx context.Context) {
	loop:
		for ctx.Err() == nil {
			var txd txDeadline

			select {
			case txd = <-out:
			case <-ctx.Done():
				break loop
			}

			if !m.shouldRefresh(txd.ID) {
				continue
			}

			// Sleep until it's time to refresh.
			now := m.clock.Now()
			if now.Before(txd.Deadline) {
				select {
				case <-ctx.Done():
					break loop
				case <-m.clock.After(txd.Deadline.Sub(now)):
					// Check again if we should refresh, after sleeping.
					if !m.shouldRefresh(txd.ID) {
						continue loop
					}
				}
			}

			// Spawn the refresh as a new background task.
			// This allows parallel refreshes when several have very close deadlines.
			// TODO: Is background spawn from background allowed? RWLock recursive?
			m.background.Go(txd.RefreshCtx, func(ctx context.Context) {
				m.refreshPending(ctx, txd.ID)
			})
		}

		// Close the input and consume the whole output chan.
		// This should free up resources quickly.
		close(in)
		for range out {
		}
	})
}

func (m *Monitor) shouldRefresh(tid data.TxID) bool {
	m.m.Lock()
	defer m.m.Unlock()
	st, ok := m.localTx[string(tid)]
	return ok && st.RefreshState == refreshStateRunning
}

func (m *Monitor) stopTxRefresh(tid data.TxID) bool {
	s, ok := m.localTx[string(tid)]
	if !ok {
		return false
	}
	if s.CancelRefresh != nil {
		s.CancelRefresh()
	}
	s.RefreshState = refreshStateStopped
	s.CancelRefresh = nil
	m.localTx[string(tid)] = s
	return true
}

func (m *Monitor) refreshPending(ctx context.Context, tid data.TxID) {
	var (
		err           error
		lastVersion   backend.Version
		shouldRefresh bool
	)

	m.m.Lock()
	st, ok := m.localTx[string(tid)]
	if ok && st.RefreshState == refreshStateRunning {
		shouldRefresh = true
		ctx, st.CancelRefresh = context.WithCancel(ctx)
		m.localTx[string(tid)] = st
	}
	m.m.Unlock()

	if !shouldRefresh {
		return
	}

	for ctx.Err() == nil {
		startT := m.clock.Now()
		tl := storage.TxLog{
			ID:        tid,
			Timestamp: startT,
			Status:    storage.TxCommitStatusPending,
		}
		if lastVersion.IsNull() {
			lastVersion, err = m.tl.Set(ctx, tl)
		} else {
			lastVersion, err = m.tl.SetIf(ctx, tl, lastVersion)
		}
		if err != nil {
			return
		}

		// Update the cached last version.
		m.m.Lock()
		if st, ok := m.localTx[string(tid)]; ok {
			st.LastVersion = lastVersion
			m.localTx[string(tid)] = st
		}
		m.m.Unlock()

		// Wait for the next refresh.
		select {
		case <-ctx.Done():
			return
		case <-m.clock.After(nextTxTimeout(startT, m.clock.Now())):
		}
	}
}

type KeyCommitStatus struct {
	Status storage.TxCommitStatus
	Value  storage.TValue
}

type WaitTxResult struct {
	Status storage.TxCommitStatus
	Err    error
}

type waitRequest struct {
	Ctx context.Context
	Ch  chan WaitTxResult
}

type txStatus struct {
	Status        storage.TxCommitStatus
	LastVersion   backend.Version
	RefreshState  refreshState
	CancelRefresh context.CancelFunc
}

type txUnknown struct {
	FirstCheck time.Time
}

type txDeadline struct {
	ID         data.TxID
	Deadline   time.Time
	RefreshCtx context.Context
}

func nextTxTimeout(lastRefresh, now time.Time) time.Duration {
	elapsed := now.Sub(lastRefresh)
	return time.Duration(float64(pendingTxTimeout)*refreshMultiplier) - elapsed
}

func newTxDeadline(now time.Time) time.Time {
	return now.Add(nextTxTimeout(now, now))
}

func isExpired(lastRefresh, now time.Time) bool {
	return now.Sub(lastRefresh.Add(maxClockSkew)) > pendingTxTimeout
}
