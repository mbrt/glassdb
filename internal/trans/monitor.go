package trans

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/shard"
	"github.com/mbrt/glassdb/internal/storage"
)

const (
	pendingTxTimeout  = 15 * time.Second
	maxClockSkew      = 30 * time.Second
	refreshMultiplier = 0.5
)

// ErrAlreadyFinalized indicates that a transaction has already been committed or aborted.
var ErrAlreadyFinalized = errors.New("transaction was already finalized")

type refreshState int

const (
	refreshStateNotStarted refreshState = iota
	refreshStateRunning
	refreshStateStopped
)

// NewMonitor returns a Monitor that tracks local and remote transaction state
// using the given transaction logger, background executor, and retrier (used
// when polling remote transaction state and writing the final log).
func NewMonitor(
	l storage.Local,
	tl storage.TLogger,
	b *concurr.Background,
	r concurr.Retrier,
) *Monitor {
	return &Monitor{
		local:      l,
		tl:         tl,
		background: b,
		retrier:    r,
		shards: shard.New(func(int) *monitorShard {
			return &monitorShard{
				localTx:   make(map[string]txStatus),
				waiters:   make(map[string][]waitRequest),
				unknownTx: make(map[string]txUnknown),
			}
		}),
	}
}

// Monitor tracks the lifecycle of transactions, providing commit, abort,
// status queries, and asynchronous wait capabilities.
type Monitor struct {
	local      storage.Local
	tl         storage.TLogger
	background *concurr.Background
	retrier    concurr.Retrier
	shards     shard.Sharded[monitorShard]
}

// monitorShard is one independent partition of the transaction tracking maps,
// keyed by transaction ID. Grouping the three maps under a single lock keeps
// their cross-map updates (e.g. removing a tx and notifying its waiters)
// atomic for a given transaction.
type monitorShard struct {
	mu        sync.Mutex
	localTx   map[string]txStatus
	waiters   map[string][]waitRequest
	unknownTx map[string]txUnknown
}

// shardFor returns the shard responsible for the given transaction ID.
func (m *Monitor) shardFor(tid data.TxID) *monitorShard {
	return m.shards.For(string(tid))
}

// BeginTx registers a new pending transaction with the monitor.
func (m *Monitor) BeginTx(_ context.Context, tid data.TxID) {
	sh := m.shardFor(tid)
	sh.mu.Lock()
	sh.localTx[string(tid)] = txStatus{
		Status:       storage.TxCommitStatusPending,
		RefreshState: refreshStateNotStarted,
	}
	sh.mu.Unlock()
}

// StartRefreshTx starts a background goroutine that periodically refreshes
// the transaction log to prevent the transaction from being considered expired.
func (m *Monitor) StartRefreshTx(ctx context.Context, tid data.TxID) {
	needStart := false

	sh := m.shardFor(tid)
	sh.mu.Lock()
	st, ok := sh.localTx[string(tid)]
	if ok && st.RefreshState == refreshStateNotStarted {
		needStart = true
		st.RefreshState = refreshStateRunning
		sh.localTx[string(tid)] = st
	}
	sh.mu.Unlock()

	if needStart {
		m.background.Go(ctx, func(ctx context.Context) {
			ticker := time.NewTicker(refreshTimeout())
			defer ticker.Stop()
			m.refreshPending(ctx, tid, ticker.C)
		})
	}
}

// CommitTx marks the transaction as committed, writes the final log, updates
// local storage with the committed values, and notifies any waiters.
func (m *Monitor) CommitTx(ctx context.Context, tl storage.TxLog) error {
	m.stopTxRefresh(tl.ID)

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

	sh := m.shardFor(tl.ID)
	sh.mu.Lock()
	delete(sh.localTx, string(tl.ID))
	m.notifyWaiters(sh, tl.ID, WaitTxResult{Status: storage.TxCommitStatusOK})
	sh.mu.Unlock()

	return nil
}

// AbortTx marks the transaction as aborted, writes the final log, and
// notifies any waiters.
func (m *Monitor) AbortTx(ctx context.Context, tid data.TxID) error {
	m.stopTxRefresh(tid)

	err := m.setFinalLog(ctx, storage.TxLog{
		ID:     tid,
		Status: storage.TxCommitStatusAborted,
	})

	sh := m.shardFor(tid)
	sh.mu.Lock()
	delete(sh.localTx, string(tid))
	m.notifyWaiters(sh, tid, WaitTxResult{Status: storage.TxCommitStatusAborted})
	sh.mu.Unlock()

	return err
}

// WoundTx forces the given transaction into the aborted state so that a
// higher-priority transaction can take over its locks under the wound-wait
// rule. It is idempotent and safe on transactions that already finished: a
// committed transaction is left untouched (its locks are released through the
// normal flow), and an already-aborted one is a no-op.
//
// The abort is made durable via a conditional write on the transaction log, so
// it is observed both by the local victim (its commit will fail) and by other
// clients holding the same lock.
func (m *Monitor) WoundTx(ctx context.Context, tid data.TxID) error {
	cs, err := m.tl.CommitStatus(ctx, tid)
	if err != nil {
		return fmt.Errorf("reading status of wound target %v: %w", tid, err)
	}
	if cs.Status.IsFinal() {
		// Already committed or aborted: nothing left to wound.
		m.markLocalAborted(tid, cs.Status)
		return nil
	}

	// Force the transaction to aborted, CAS-ing over its current log version
	// (or creating an aborted log if it has none yet).
	status, err := m.tryAbortRemoteTx(ctx, tid, cs.Version)
	if err != nil {
		return fmt.Errorf("wounding tx %v: %w", tid, err)
	}
	m.markLocalAborted(tid, status)
	return nil
}

// markLocalAborted reflects a durable abort in the in-memory state when the
// wounded transaction is local, so the victim and any waiters unwind promptly.
func (m *Monitor) markLocalAborted(tid data.TxID, status storage.TxCommitStatus) {
	if status != storage.TxCommitStatusAborted {
		return
	}
	m.stopTxRefresh(tid)

	sh := m.shardFor(tid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	delete(sh.localTx, string(tid))
	m.notifyWaiters(sh, tid, WaitTxResult{Status: storage.TxCommitStatusAborted})
}

// TxStatus returns the current commit status of the given transaction,
// checking locally first and then fetching from remote storage if needed.
func (m *Monitor) TxStatus(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	sh := m.shardFor(tid)
	sh.mu.Lock()
	if s, ok := sh.localTx[string(tid)]; ok {
		sh.mu.Unlock()
		return s.Status, nil
	}
	sh.mu.Unlock()

	return m.fetchRemoteTxStatus(ctx, tid)
}

// WaitForTx waits asynchronously for the given transaction to complete.
//
// The given context may not be used if other callers are are waiting for the same
// transaction.
func (m *Monitor) WaitForTx(ctx context.Context, tid data.TxID) <-chan WaitTxResult {
	ch := make(chan WaitTxResult, 1)

	sh := m.shardFor(tid)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	txs, isLocal := sh.localTx[string(tid)]
	s := txs.Status
	if isLocal && s == storage.TxCommitStatusOK || s == storage.TxCommitStatusAborted {
		// The value is already available.
		ch <- WaitTxResult{
			Status: s,
			Err:    nil,
		}
		return ch
	}

	if ws, ok := sh.waiters[string(tid)]; ok {
		// There is already a worker waiting for the request.
		// Enqueue ourselves in the waiters.
		sh.waiters[string(tid)] = append(ws, waitRequest{
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
		sh.waiters[string(tid)] = []waitRequest{{Ctx: ctx, Ch: ch}}
		return ch
	}

	// This is a remote transaction. Let's spawn a worker to poll its log.
	// Make sure to initialize the waiters while locked (empty list).
	sh.waiters[string(tid)] = nil

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

				sh.mu.Lock()
				m.notifyWaiters(sh, tid, res)
				sh.mu.Unlock()

				ch <- res
				return
			}

			// The context expired. Try and see wether somebody else is
			// interested in this tx.
			sh.mu.Lock()
			w, ok := m.nextWaiter(sh, tid)
			sh.mu.Unlock()
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

// CommittedValue returns the committed value for a key written by the given
// transaction, reading from local storage or the transaction log as needed.
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
		if isExpired(status.LastUpdate, time.Now()) {
			return m.tryAbortRemoteTx(ctx, tid, status.Version)
		}
		return storage.TxCommitStatusPending, nil
	}

	return status.Status, nil
}

func (m *Monitor) handleUnknownTx(ctx context.Context, tid data.TxID) (storage.TxCommitStatus, error) {
	now := time.Now()

	sh := m.shardFor(tid)
	sh.mu.Lock()
	st, ok := sh.unknownTx[string(tid)]
	if !ok {
		sh.unknownTx[string(tid)] = txUnknown{FirstCheck: now}
		sh.mu.Unlock()
		return storage.TxCommitStatusPending, nil
	}
	sh.mu.Unlock()

	if isExpired(st.FirstCheck, now) {
		st, err := m.tryAbortRemoteTx(ctx, tid, backend.Version{})
		if err == nil {
			// We now have a permanent state. We can remove the tx from the map.
			sh.mu.Lock()
			delete(sh.unknownTx, string(tid))
			sh.mu.Unlock()
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

	err := m.retrier.Retry(ctx, func() error {
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

// nextWaiter returns the first non-expired waiter for tid. The caller must
// hold sh.mu, and sh must be the shard responsible for tid.
func (m *Monitor) nextWaiter(sh *monitorShard, tid data.TxID) (waitRequest, bool) {
	ws, ok := sh.waiters[string(tid)]
	if !ok {
		return waitRequest{}, false
	}

	// Find the first non expired waiter.
	i := slices.IndexFunc(ws, func(w waitRequest) bool {
		return w.Ctx.Err() == nil
	})
	if i < 0 {
		i = len(ws)
	}
	// Trim the expired waiters.
	if i > 0 {
		ws = ws[i:]
		sh.waiters[string(tid)] = ws
	}
	if len(ws) == 0 {
		return waitRequest{}, false
	}

	return ws[0], true
}

// notifyWaiters sends res to every live waiter for tid and clears them. The
// caller must hold sh.mu, and sh must be the shard responsible for tid.
func (m *Monitor) notifyWaiters(sh *monitorShard, tid data.TxID, res WaitTxResult) {
	ws, ok := sh.waiters[string(tid)]
	if !ok {
		return
	}
	for _, w := range ws {
		if w.Ctx.Err() == nil {
			w.Ch <- res
		}
	}
	delete(sh.waiters, string(tid))
}

func (m *Monitor) setFinalLog(ctx context.Context, tlog storage.TxLog) error {
	tid := tlog.ID
	if tid == nil {
		return errors.New("missing required tlog ID")
	}

	sh := m.shardFor(tid)
	sh.mu.Lock()
	stat := sh.localTx[string(tid)]
	lastV := stat.LastVersion
	sh.mu.Unlock()

	// TODO: Add timeout ~= refreshTimeout here.
	err := m.retrier.Retry(ctx, func() error {
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

func (m *Monitor) shouldRefresh(tid data.TxID) bool {
	sh := m.shardFor(tid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st, ok := sh.localTx[string(tid)]
	return ok && st.RefreshState == refreshStateRunning
}

func (m *Monitor) stopTxRefresh(tid data.TxID) bool {
	sh := m.shardFor(tid)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	s, ok := sh.localTx[string(tid)]
	if !ok || s.RefreshState != refreshStateRunning {
		return false
	}
	s.RefreshState = refreshStateStopped
	sh.localTx[string(tid)] = s
	return true
}

func (m *Monitor) refreshPending(ctx context.Context, tid data.TxID, tickCh <-chan time.Time) {
	var (
		err           error
		lastVersion   backend.Version
		shouldRefresh bool
	)

	sh := m.shardFor(tid)
	sh.mu.Lock()
	st, ok := sh.localTx[string(tid)]
	if ok && st.RefreshState == refreshStateRunning {
		shouldRefresh = true
		sh.localTx[string(tid)] = st
	}
	sh.mu.Unlock()

	if !shouldRefresh {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickCh:
		}

		if !m.shouldRefresh(tid) {
			return
		}

		startT := time.Now()
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
		sh.mu.Lock()
		if st, ok := sh.localTx[string(tid)]; ok {
			st.LastVersion = lastVersion
			sh.localTx[string(tid)] = st
		}
		sh.mu.Unlock()
	}
}

// KeyCommitStatus holds a transaction's commit status for a specific key,
// along with the value that was written.
type KeyCommitStatus struct {
	Status storage.TxCommitStatus
	Value  storage.TValue
}

// WaitTxResult holds the outcome of waiting for a transaction to complete.
type WaitTxResult struct {
	Status storage.TxCommitStatus
	Err    error
}

type waitRequest struct {
	Ctx context.Context
	Ch  chan WaitTxResult
}

type txStatus struct {
	Status       storage.TxCommitStatus
	LastVersion  backend.Version
	RefreshState refreshState
}

type txUnknown struct {
	FirstCheck time.Time
}

func refreshTimeout() time.Duration {
	return time.Duration(float64(pendingTxTimeout) * refreshMultiplier)
}
func isExpired(lastRefresh, now time.Time) bool {
	return now.Sub(lastRefresh.Add(maxClockSkew)) > pendingTxTimeout
}
