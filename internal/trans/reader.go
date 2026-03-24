package trans

import (
	"context"
	"time"

	"github.com/mbrt/glassdb/backend"
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
		return r.handleLockCreate(ctx, key, lres)
	}
	// Otherwise global read.
	gr, err := r.global.Read(ctx, key)
	if err != nil {
		return ReadValue{}, err
	}
	gres := ReadValue{
		Value: gr.Value,
		Version: storage.Version{
			B: backend.Version{Contents: gr.Version},
		},
	}
	return r.handleLockCreate(ctx, key, gres)
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

func (r Reader) handleLockCreate(ctx context.Context, key string, rv ReadValue) (ReadValue, error) {
	if len(rv.Value) > 0 {
		// We are safe to return this value (it wasn't locked in create).
		return rv, nil
	}
	// Otherwise we might have encountered a key locked in create.
	// In which case, we may have read a value that wasn't committed yet.
	// Let's check for that with an extra metadata read.
	meta, err := r.global.GetMetadata(ctx, key)
	if err != nil {
		return ReadValue{}, err
	}
	info, err := storage.TagsLockInfo(meta.Tags)
	if err != nil {
		return ReadValue{}, err
	}
	if info.Type != storage.LockTypeCreate {
		// No problem. This was really a committed empty value.
		return rv, nil
	}
	// Locked in create. Is the new value available or not?
	if len(info.LockedBy) != 1 {
		// Something wrong with this lock. Return not found.
		return ReadValue{}, backend.ErrNotFound
	}
	lockerID := info.LockedBy[0]

	// We can check whether this is committed or not.
	cv, err := r.tmon.CommittedValue(ctx, key, lockerID)
	if err != nil || cv.Status != storage.TxCommitStatusOK || cv.Value.NotWritten {
		return ReadValue{}, backend.ErrNotFound
	}
	// Committed. Let's save ourselves some time and return this.
	// Also cache the value for later.
	version := storage.Version{Writer: lockerID}
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
