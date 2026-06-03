// Package trans implements the transaction processing algorithm with
// serializable isolation.
package trans

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/concurr"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/errors"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trace"
)

// ErrRetry signals that the transaction should be retried from the beginning.
var ErrRetry = errors.New("retry transaction")

// ErrWounded signals that the transaction was aborted by a higher-priority
// transaction under the wound-wait rule. It must be retried from the beginning
// with a fresh ID that preserves its original priority.
var ErrWounded = errors.New("transaction was wounded")

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
	lockLatency        = 90 * time.Millisecond
	maxDeadlockTimeout = 5 * time.Second
	bgCleanupTimeout   = time.Minute
)

var (
	errLockTimeout   = errors.New("lock timeout")
	errNoSingleWrite = errors.New("cannot validate transaction with multiple writes")
	errValidateRetry = errors.New("retry validation")
)

// NewAlgo returns an Algo that coordinates transactions using the given
// storage, locking, monitoring, and garbage collection components.
func NewAlgo(
	g storage.Global,
	local storage.Local,
	locker *Locker,
	mon *Monitor,
	gc *GC,
	bg *concurr.Background,
	log *slog.Logger,
	txIDs data.TxIDSource,
) Algo {
	return Algo{
		global:     g,
		local:      local,
		reader:     NewReader(local, g, mon),
		locker:     locker,
		mon:        mon,
		gc:         gc,
		background: bg,
		log:        log,
		txIDs:      txIDs,
	}
}

// Algo implements the transaction commit protocol, including read validation,
// locking, and write application.
type Algo struct {
	global     storage.Global
	local      storage.Local
	reader     Reader
	locker     *Locker
	mon        *Monitor
	gc         *GC
	background *concurr.Background
	log        *slog.Logger
	txIDs      data.TxIDSource
}

// Begin starts a new transaction with the given data and returns a handle to it.
func (t Algo) Begin(ctx context.Context, d Data) *Handle {
	// CtxWithTxID lets tests pin a specific ID (and thus priority); otherwise
	// the ID comes from the configured source.
	tid := TxIDFromCtx(ctx)
	if tid == nil {
		tid = t.txIDs.New()
	}
	return &Handle{
		id:     tid,
		data:   d,
		status: statusNew,
		log:    t.log.With("tx", txLog(tid)),
	}
}

// Commit validates all reads and applies all writes for the transaction,
// returning ErrRetry if a conflict is detected.
func (t Algo) Commit(ctx context.Context, tx *Handle) error {
	if tx.status == statusNew {
		t.mon.BeginTx(ctx, tx.id)
		tx.status = statusValidating
	}
	vstate := initValidation(tx)

	for {
		// Stop early if a higher-priority transaction wounded us while we were
		// validating: there's no point acquiring more locks.
		if t.wasWounded(ctx, tx) {
			t.updateLocalCache(*vstate)
			return ErrWounded
		}
		tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit round BEGIN")
		err := t.validateRound(ctx, vstate, tx)
		tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit round END", errAttr(err))

		if err == nil {
			// Committed.
			break
		}
		if errors.Is(err, errValidateRetry) {
			// Retry the validation without retrying the transaction.
			continue
		}
		// Other errors.
		// Before retrying, update possible stale values.
		t.updateLocalCache(*vstate)
		return err
	}

	// Validation succeeded.
	if err := t.commitWrites(ctx, tx.data.Writes, tx.id); err != nil {
		if errors.Is(err, ErrAlreadyFinalized) {
			// The log was already finalized as aborted: we were wounded (or
			// reclaimed as expired) between validation and commit. Keep the
			// original error in the chain for callers that inspect it.
			return fmt.Errorf("%w: %w", ErrWounded, err)
		}
		return fmt.Errorf("committing writes for tx %v: %w", tx.id, err)
	}
	// We are now considered committed.
	tx.status = statusCommitted
	t.asyncCleanup(ctx, tx)
	return nil
}

// ValidateReads validates that the reads in a read-only transaction are still
// consistent, returning ErrRetry if any read has been invalidated.
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

// Reset replaces the transaction's data with new data, preserving any acquired
// locks. It panics if the transaction was already committed.
func (t Algo) Reset(tx *Handle, data Data) {
	if tx.status == statusCommitted {
		panic("Cannot reset a committed transaction")
	}
	// Do not reset the locked items. They should persist.
	tx.data = data
}

// Rebegin starts a fresh attempt for a transaction that was wounded, reusing
// the original priority (timestamp) so it is not starved, but with a new ID so
// it gets a distinct transaction log. The locks of the old attempt are not
// carried over; callers should release them separately.
func (t Algo) Rebegin(_ context.Context, old *Handle, d Data) *Handle {
	tid := t.txIDs.Renew(old.id)
	return &Handle{
		id:     tid,
		data:   d,
		status: statusNew,
		log:    t.log.With("tx", txLog(tid)),
	}
}

// wasWounded reports whether the transaction was already aborted by a
// higher-priority transaction. It is best-effort: a status read error is not
// treated as a wound.
func (t Algo) wasWounded(ctx context.Context, tx *Handle) bool {
	status, err := t.mon.TxStatus(ctx, tx.id)
	if err != nil {
		return false
	}
	return status == storage.TxCommitStatusAborted
}

// End aborts a non-committed transaction, releasing its locks and cleaning up
// resources. It is a no-op if the transaction was already committed.
func (t Algo) End(ctx context.Context, tx *Handle) error {
	if tx == nil || tx.status == statusCommitted {
		return nil
	}
	// Release all the locks and abort.
	if err := t.mon.AbortTx(ctx, tx.id); err != nil {
		if errors.Is(err, ErrAlreadyFinalized) {
			// The log was already finalized (typically aborted by a
			// higher-priority transaction under wound-wait, or reclaimed as
			// expired). Aborting is exactly what End wants, so this is a benign
			// no-op: clean up any locks we still hold and report success rather
			// than surfacing it as a transaction error.
			tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit End: already finalized")
			t.asyncCleanup(ctx, tx)
			return nil
		}
		// Failing because of a timeout is not a big problem, as we're going to
		// follow up with an async cleanup.
		tx.log.LogAttrs(ctx, slog.LevelError, "Commit End", errAttr(err))
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
		// Deadlocks are normally prevented by the wound-wait rule during lock
		// acquisition (older transactions wound younger holders). Reaching the
		// timeout therefore means sustained contention rather than a true
		// deadlock; fall back to serial, sorted-order locking as a safety net
		// so we still make progress.
		tx.serialLocking = true
		return errValidateRetry
	}
	return nil
}

func (t Algo) commitSingleRW(ctx context.Context, tx *Handle) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit single RW BEGIN")
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
	for range 2 {
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
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit readonly BEGIN")
	// We can validate that the read is consistent (i.e. not modified by anyone
	// since, nor locked by others) and avoid any locking.
	err := t.fanout(ctx, len(vstate.Paths), func(ctx context.Context, i int) error {
		item := &vstate.Paths[i]
		if item.NotFound {
			return t.validateReadNotFound(ctx, item)
		}
		return t.validateRead(ctx, item)
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

// validateRead validates that the item is still consistent with the read we
// did earlier, without holding any lock on it.
func (t Algo) validateRead(ctx context.Context, item *pathState) error {
	// We need the freshest possible meta, because we don't have a lock.
	meta, err := t.global.GetMetadata(ctx, item.Path)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
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

	// If the item is unlocked or locked in read, the last writer must still
	// be the one we read from.
	if li.Type == storage.LockTypeNone || li.Type == storage.LockTypeRead {
		if !li.LastWriter.Equal(readFromT) {
			item.Result = vResultRetry
			return nil
		}
		item.Result = vResultOK
		return nil
	}

	// Locked in write or create: defer to the more elaborate path, which
	// inspects the locker's commit status to determine the expected writer.
	return t.validateLockedRead(ctx, item, li, readFromT)
}

func (t Algo) validateLockedRead(
	ctx context.Context,
	item *pathState,
	li storage.LockInfo,
	readFromT data.TxID,
) error {
	// The item is still locked in write or create. Two valid cases:
	// - If the transaction is committed, it must be the one we read from.
	// - If not committed, the last writer must be the one we read from.
	if len(li.LockedBy) != 1 {
		return fmt.Errorf("bad lock: %v with %d lockers", li.Type, len(li.LockedBy))
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
		switch {
		case v.Value.NotWritten:
			expectedWriter = li.LastWriter
		case v.Value.Deleted:
			// We expected something but this was deleted. Update the local
			// cache and retry.
			item.Result = vResultRetry
			t.updateLocal(
				WriteAccess{
					Path:   item.Path,
					Val:    v.Value.Value,
					Delete: true,
				},
				locker)
			return nil
		default:
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

	if expectedVal.Status != storage.TxCommitStatusOK {
		// Fetch the expected committed value.
		expectedVal, err = t.mon.CommittedValue(ctx, item.Path, expectedWriter)
		if err != nil || expectedVal.Status != storage.TxCommitStatusOK || expectedVal.Value.NotWritten {
			// We cannot authoritatively resolve expectedWriter's value. This
			// happens when expectedWriter committed through the single-RW fast
			// path, which writes no transaction log, so its log-based status is
			// unknown/aborted even though it did commit. Caching a guessed value
			// here would be corrupting: it would pair value bytes with a writer
			// that did not produce them, and a later read could trust that
			// (writer matches the live last-writer) and overwrite a newer value,
			// losing an update. Instead, invalidate the stale cached value so the
			// retry re-reads the authoritative one straight from storage.
			t.local.MarkValueOutated(item.Path, item.ReadVersion)
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

func (t Algo) validateReadNotFound(ctx context.Context, item *pathState) error {
	// We need the freshest possible meta, because we don't have a lock.
	meta, err := t.global.GetMetadata(ctx, item.Path)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			// All good here, the item is still not found now.
			item.Result = vResultOK
			return nil
		}
		return err
	}

	// Check the current state of the item.
	li, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}

	// If the item is not locked, some transaction committed. We need to retry.
	if li.Type == storage.LockTypeNone {
		item.Result = vResultRetry
		return nil
	}

	// The item is locked. If this is locked in read, it means another
	// transaction before us committed in write.
	if li.Type == storage.LockTypeRead {
		item.Result = vResultRetry
		return nil
	}

	// The item is locked in write. If the current transaction committed, we
	// need to check whether the item was deleted.
	if len(li.LockedBy) != 1 {
		return fmt.Errorf("bad lock: %v with %d lockers", li.Type, len(li.LockedBy))
	}
	locker := li.LockedBy[0]
	status, err := t.mon.TxStatus(ctx, locker)
	if err != nil {
		return err
	}

	var lastWriter data.TxID

	switch status {
	case storage.TxCommitStatusOK:
		lastWriter = locker

	case storage.TxCommitStatusAborted, storage.TxCommitStatusPending:
		lastWriter = li.LastWriter

	default:
		return fmt.Errorf("unknown tx commit status: %v", status)
	}

	// We need to check whether the last writer deleted the item or not.
	v, err := t.mon.CommittedValue(ctx, item.Path, lastWriter)
	if err != nil {
		return err
	}
	if v.Value.Deleted {
		item.Result = vResultOK
		return nil
	}

	// Was written to: retry. Only refresh the local cache when we could
	// authoritatively resolve the value. If lastWriter committed via the
	// single-RW fast path it has no transaction log, so the value is
	// unresolvable here; caching a guessed value would corrupt the entry, so we
	// just retry and let the next read fetch the authoritative value.
	if v.Status == storage.TxCommitStatusOK && !v.Value.NotWritten {
		t.updateLocal(
			WriteAccess{
				Path:   item.Path,
				Val:    v.Value.Value,
				Delete: v.Value.Deleted},
			lastWriter)
	}

	item.Result = vResultRetry
	return nil
}

func (t Algo) checkReadVersionUnlocked(rv ReadVersion, meta backend.Metadata) error {
	// Get info about the state of the lock.
	linfo, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return err
	}

	// Verify that the writer we read from is still the current one. Two
	// acceptable states:
	// - Unlocked and the last-writer tag still matches.
	// - Locked by the writer we read from (committed but not yet flushed).
	sameLastWriter := linfo.LastWriter.Equal(rv.LastWriter) &&
		linfo.Type == storage.LockTypeNone
	lockedByWriter := len(linfo.LockedBy) == 1 &&
		linfo.LockedBy[0].Equal(rv.LastWriter)

	if !sameLastWriter && !lockedByWriter {
		return ErrRetry
	}
	return nil
}

func (t Algo) parallelValidate(ctx context.Context, vstate *validationState, tx *Handle) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit parallel BEGIN")
	// Parallel locking acquires locks out of order, which could deadlock. The
	// wound-wait rule normally breaks any cycle by aborting the younger party,
	// but we still arm a timeout as a backstop: if we can't make progress we
	// resort to serial, sorted-order locking and validation instead.
	ctx, cancel := t.deadlockTimeoutCtx(ctx, *vstate)
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
		err := t.lockPath(ctx, colocks[i].Path, colocks[i].Type, tx)
		if err != nil {
			return fmt.Errorf("locking collection %q: %w", colocks[i].Path, err)
		}
		return nil
	})

	return err
}

// serialValidate is the deadlock safety net used when the wound-wait rule and
// the parallel path fail to make progress under heavy contention. It acquires
// every lock in a globally consistent order (sorted collections, then sorted
// keys), which by itself cannot deadlock.
func (t Algo) serialValidate(ctx context.Context, vstate *validationState, tx *Handle) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Commit serial BEGIN")
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
			if err := t.lockPath(ctx, cl.Path, cl.Type, tx); err != nil {
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
	// Blind-write optimization: a key that is written but never read, with no
	// cached evidence that it exists, is most likely a fresh insert. Probing it
	// with a write-lock first only to discover the key is absent costs a wasted
	// backend metadata read per key (the dominant cost of bulk inserts). Route
	// such keys straight to the create path instead: acquire the collection
	// lock and attempt WriteIfNotExists. If the key turns out to exist after
	// all, lockValidateNotFoundKey falls back to a normal write-lock, so the
	// guess is self-correcting and never unsafe.
	if item.Write && !item.Read && !item.NotFound && !t.localIndicatesPresent(item.Path) {
		item.NotFound = true
		if t.isKeyCollectionLocked(item.Path, storage.LockTypeWrite, tx) {
			return t.lockValidateNotFoundKey(ctx, item, tx)
		}
		item.Result = vResultNeedsCLock
		return nil
	}

	// First thing, lock.
	var err error
	if item.Write {
		err = t.lockWrite(ctx, item.Path, tx)
	} else if item.Read {
		err = t.lockRead(ctx, item.Path, tx)
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

// localIndicatesPresent reports whether the local cache holds positive evidence
// that the key exists (a non-deleted value or non-null metadata). A false
// result means "unknown" - the key may or may not exist - which the blind-write
// optimization treats as a likely fresh insert.
func (t Algo) localIndicatesPresent(path string) bool {
	if lr, ok := t.local.Read(path, storage.MaxStaleness); ok && !lr.Deleted {
		return true
	}
	if lm, ok := t.local.GetMeta(path, storage.MaxStaleness); ok && !lm.M.Version.IsNull() {
		return true
	}
	return false
}

func (t Algo) lockValidateNotFoundKey(ctx context.Context, item *pathState, tx *Handle) error {
	if item.Read && item.Write {
		// The item was read, not found and written to. We need to lock it
		// in write and check that it's still not found afterwards.
		// Lock create will do exactly that.
		err := t.lockCreate(ctx, item.Path, tx)
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
		err = t.lockRead(ctx, item.Path, tx)
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
		err := t.lockCreate(ctx, item.Path, tx)
		if err == nil {
			// All good.
			item.Result = vResultOK
			return nil
		}
		if !errors.Is(err, backend.ErrPrecondition) {
			return err
		}
		// The item was found now, so we should lock it in write normally instead.
		err = t.lockWrite(ctx, item.Path, tx)
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
	tx *Handle,
) error {
	var err error

	switch lt {
	case storage.LockTypeRead:
		err = t.lockRead(ctx, path, tx)
	case storage.LockTypeWrite:
		err = t.lockWrite(ctx, path, tx)
	case storage.LockTypeCreate:
		err = t.lockCreate(ctx, path, tx)
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
	tx *Handle,
) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockRead BEGIN", pathAttr(key))
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.LockRead(ctx, key, tx.id)
	})
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockRead END", pathAttr(key), errAttr(err))
	return err
}

func (t Algo) lockWrite(
	ctx context.Context,
	key string,
	tx *Handle,
) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockWrite BEGIN", pathAttr(key))
	var err error
	trace.WithRegion(ctx, "lock-write", func() {
		err = t.locker.LockWrite(ctx, key, tx.id)
	})
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockWrite END", pathAttr(key), errAttr(err))
	return err
}

func (t Algo) lockCreate(
	ctx context.Context,
	key string,
	tx *Handle,
) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockCreate BEGIN", pathAttr(key))
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.LockCreate(ctx, key, tx.id)
	})
	tx.log.LogAttrs(ctx, slog.LevelDebug, "LockCreate END", pathAttr(key), errAttr(err))
	return err
}

func (t Algo) unlock(
	ctx context.Context,
	key string,
	tx *Handle,
) error {
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Unlock BEGIN", pathAttr(key))
	var err error
	trace.WithRegion(ctx, "lock-create", func() {
		err = t.locker.Unlock(ctx, key, tx.id)
	})
	tx.log.LogAttrs(ctx, slog.LevelDebug, "Unlock END", pathAttr(key), errAttr(err))
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

func (t Algo) unlockAll(ctx context.Context, tx *Handle) error {
	ps := t.locker.LockedPaths(tx.id)
	if len(ps) == 0 {
		return nil
	}
	// Collect the errors here so that we don't stop the fanout even if some
	// unlocks fail.
	errs := make([]error, len(ps))

	// Unlock everything synchronously, but in parallel.
	_ = t.fanout(ctx, len(ps), func(ctx context.Context, i int) error {
		pl := ps[i]
		if err := t.unlock(ctx, pl.Path, tx); err != nil {
			errs[i] = fmt.Errorf("unlocking %q: %w", pl.Path, err)
		}
		return nil
	})

	if err := errors.Combine(errs...); err != nil {
		return fmt.Errorf("unlocking all for tx %q: %v", tx.id, err)
	}
	return nil
}

func (t Algo) asyncCleanup(ctx context.Context, tx *Handle) {
	if t.background == nil {
		// Background tasks are disabled.
		return
	}

	ps := t.locker.LockedPaths(tx.id)

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
		ctx, cancel := context.WithTimeout(ctx, bgCleanupTimeout)
		defer cancel()

		w := f.Spawn(ctx, len(ps), func(ctx context.Context, i int) error {
			pl := ps[i]
			if err := t.unlock(ctx, pl.Path, tx); err != nil {
				errs[i] = fmt.Errorf("unlocking %v: %w", pl, err)
			}
			return nil
		})
		_ = w.Wait()
		if err := errors.Combine(errs...); err != nil {
			// Something went wrong during some unlocking. Skip the next phase.
			// Avoid logging if we are shutting down.
			if ctx.Err() == nil {
				tx.log.LogAttrs(ctx, slog.LevelError, "AsyncCleanup END", errAttr(err))
			}
			return
		}
		t.gc.ScheduleTxCleanup(tx.id)
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
	if num == 1 {
		// Avoid a goroutine spawn and errgroup setup for the common
		// single-element case (e.g. validating one read, or locking the single
		// collection of a transaction). With no siblings there is nothing to run
		// concurrently or to cancel, so running inline is equivalent.
		return fn(ctx, 0)
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

func (t Algo) deadlockTimeoutCtx(
	ctx context.Context,
	vstate validationState,
) (context.Context, context.CancelFunc) {
	if len(vstate.Paths) <= 1 {
		// There's no possibility of deadlocks. This can happen only if there
		// are multiple resources taken out of order. Even if we were to lock
		// the collection of this path, we would do it in a specific order (i.e.
		// collection first, key after).
		return ctx, func() {}
	}
	// With wound-wait preventing true deadlocks, this timeout only guards
	// against sustained contention where the parallel path keeps failing to
	// acquire locks. The balance is between giving up too early (paying for an
	// unnecessary serial restart) and too late (wasting time). We grant more
	// time to transactions locking many keys, since restarting those is
	// expensive.
	timeout := min(4*lockLatency*time.Duration(len(vstate.Paths)), maxDeadlockTimeout)
	return context.WithTimeout(ctx, timeout)
}

// Data holds the reads and writes that make up a transaction.
type Data struct {
	Reads  []ReadAccess
	Writes []WriteAccess
}

// ReadAccess records a single key read within a transaction, including the
// version observed and whether the key existed.
type ReadAccess struct {
	Path    string
	Version ReadVersion
	Found   bool
}

// ReadVersion identifies the version of a value read by a transaction, by
// the transaction ID of the writer that produced the value.
type ReadVersion struct {
	LastWriter data.TxID
}

// ToStorageVersion converts the read version to a storage.Version.
func (r ReadVersion) ToStorageVersion() storage.Version {
	return storage.Version{
		Writer: r.LastWriter,
	}
}

// WriteAccess records a single key write within a transaction.
type WriteAccess struct {
	Path   string
	Val    []byte
	Delete bool
}

// Handle is an opaque reference to an in-progress transaction managed by Algo.
type Handle struct {
	data         Data
	status       statusType
	id           data.TxID
	requireLocks bool
	// serialLocking switches validation to the sorted-order locking safety net
	// after the parallel path hit the deadlock timeout despite wound-wait.
	serialLocking bool
	log           *slog.Logger
}

// ctxKey is a private type for context keys defined in this package, so that
// distinct keys never collide with each other or with keys from other packages.
type ctxKey int

const txIDKey ctxKey = iota

// CtxWithTxID returns a new context carrying the given transaction ID. It lets
// tests pin a transaction's ID (and thus its wound-wait priority); production
// leaves it unset and the ID comes from the Algo's configured TxID source.
func CtxWithTxID(ctx context.Context, id data.TxID) context.Context {
	return context.WithValue(ctx, txIDKey, id)
}

// TxIDFromCtx extracts the transaction ID from the context, or returns nil if
// none is set.
func TxIDFromCtx(ctx context.Context) data.TxID {
	if id, ok := ctx.Value(txIDKey).(data.TxID); ok {
		return id
	}
	return nil
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
	// Iterate in a stable path order so the sequence of backend operations a
	// transaction issues does not depend on Go's randomized map iteration. This
	// keeps the parallel validation path deterministic (the serial path already
	// sorts), which is what makes the serializability fuzzer reproducible.
	sort.Slice(res, func(i, j int) bool {
		return res[i].Path < res[j].Path
	})
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
	// Stable order: the collection-lock acquisition sequence must not depend on
	// map iteration order (see initValidation).
	sort.Slice(res, func(i, j int) bool {
		return res[i].Path < res[j].Path
	})
	return res, nil
}

func errAttr(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.Attr{
		Key:   "err",
		Value: slog.StringValue(err.Error()),
	}
}

func pathAttr(p string) slog.Attr {
	return slog.Attr{
		Key:   "path",
		Value: slog.StringValue(p),
	}
}

type txLog data.TxID

func (t txLog) LogValue() slog.Value {
	return slog.StringValue(data.TxID(t).String())
}
