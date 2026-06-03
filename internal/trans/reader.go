package trans

import (
	"context"
	"time"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/storage"
)

// NewReader returns a Reader that reads from local and global storage, resolving
// uncommitted create-locks through the transaction monitor.
func NewReader(l storage.Local, g storage.Global, m *Monitor) Reader {
	return Reader{
		local:  l,
		global: g,
		tmon:   m,
	}
}

// Reader reads values from local and global storage, handling staleness and
// correctly resolving values for keys that may be locked in create.
type Reader struct {
	local  storage.Local
	global storage.Global
	tmon   *Monitor
}

func (r Reader) Read(
	ctx context.Context,
	key string,
	maxStale time.Duration,
) (ReadValue, error) {
	lr, ok := r.local.Read(key, maxStale)
	if ok && !lr.Outdated {
		if lr.Deleted {
			return ReadValue{}, backend.ErrNotFound
		}
		lres := ReadValue{
			Value:   lr.Value,
			Version: lr.Version,
		}
		return r.handleLockCreate(ctx, key, lres, lr.CreateLockedBy)
	}
	// Otherwise global read.
	gr, err := r.global.Read(ctx, key)
	if err != nil {
		return ReadValue{}, err
	}
	gres := ReadValue{
		Value:   gr.Value,
		Version: gr.Version,
	}
	return r.handleLockCreate(ctx, key, gres, gr.CreateLockedBy)
}

// ReadCached returns the value for key if it can be served entirely from the
// local cache without any backend round-trip - i.e. a present, non-empty,
// non-outdated value that is not create-locked. It reports ok=false for every
// other case (cache miss, outdated, deleted, an empty value that might be a
// create-lock placeholder, or a non-empty create-with-value placeholder that
// may not be committed yet), in which case the caller must fall back to Read.
// The fast-path result is identical to what Read would return for such a value
// (handleLockCreate returns committed, non-create-locked values inline), so it
// is safe to substitute.
func (r Reader) ReadCached(key string) (ReadValue, bool) {
	lr, ok := r.local.Read(key, storage.MaxStaleness)
	if !ok || lr.Outdated || lr.Deleted || len(lr.Value) == 0 || lr.CreateLockedBy != nil {
		return ReadValue{}, false
	}
	return ReadValue{
		Value:   lr.Value,
		Version: lr.Version,
	}, true
}

// GetMetadata returns the object metadata, using the local cache when fresh
// enough and falling back to global storage otherwise.
func (r Reader) GetMetadata(
	ctx context.Context,
	key string,
	maxStale time.Duration,
) (backend.Metadata, error) {
	lm, ok := r.local.GetMeta(key, maxStale)
	if ok && !lm.Outdated {
		return lm.M, nil
	}
	return r.global.GetMetadata(ctx, key)
}

// handleLockCreate resolves a value that may belong to a create-with-value
// placeholder. createLockedBy is the create-lock holder reported by the read
// (nil if the object is not create-locked). The lock state is read atomically
// with the value, so a nil createLockedBy always denotes a committed value:
// the placeholder's value is written together with its create-lock tag and is
// immutable, and finalizing the create only clears that tag, so a value that is
// not create-locked is always a committed one.
func (r Reader) handleLockCreate(ctx context.Context, key string, rv ReadValue, createLockedBy data.TxID) (ReadValue, error) {
	if createLockedBy == nil {
		// Not locked in create: this is a committed value (possibly empty).
		return rv, nil
	}
	// Locked in create: the value may not be committed yet. Check the creator's
	// transaction status to decide whether the value is visible.
	cv, err := r.tmon.CommittedValue(ctx, key, createLockedBy)
	if err != nil || cv.Status != storage.TxCommitStatusOK || cv.Value.NotWritten {
		return ReadValue{}, backend.ErrNotFound
	}
	// Committed. Cache the resolved value for later and return it.
	version := storage.Version{Writer: createLockedBy}
	if cv.Value.Deleted {
		r.local.MarkDeleted(key, version)
		return ReadValue{}, backend.ErrNotFound
	}

	r.local.Write(key, cv.Value.Value, version)

	return ReadValue{
		Value:   cv.Value.Value,
		Version: version,
	}, nil
}

// ReadValue holds the result of reading a key, including the raw value and
// its storage version.
type ReadValue struct {
	Value   []byte
	Version storage.Version
}
