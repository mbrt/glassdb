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
	"errors"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
	pb "github.com/mbrt/glassdb/internal/proto"
)

var ErrKeyNotFound = errors.New("key not found in committed transaction")

type TxCommitStatus int

func (s TxCommitStatus) IsFinal() bool {
	return s == TxCommitStatusOK || s == TxCommitStatusAborted
}

const (
	TxCommitStatusUnknown TxCommitStatus = iota
	TxCommitStatusOK
	TxCommitStatusAborted
	TxCommitStatusPending
)

const (
	commitStatusTag = "commit-status"
	timestampTag    = "timestamp"

	commitStatusOK      = "committed"
	commitStatusAborted = "aborted"
	commitStatusPending = "pending"
)

type TxLog struct {
	ID        data.TxID
	Timestamp time.Time
	Status    TxCommitStatus
	Writes    []TxWrite
	Locks     []PathLock
}

type TxWrite struct {
	Path       string
	Value      []byte
	Deleted    bool
	PrevWriter data.TxID
}

type TxStatus struct {
	Status     TxCommitStatus
	LastUpdate time.Time
	Version    backend.Version
}

type PathLock struct {
	Path string
	Type LockType
}

func NewTLogger(c clockwork.Clock, g Global, l Local, prefix string) TLogger {
	return TLogger{prefix, c, g, l}
}

// TLogger provides tools to deal with transaction logs.
type TLogger struct {
	prefix string
	clock  clockwork.Clock
	global Global
	local  Local
}

func (t TLogger) CommitStatus(ctx context.Context, id data.TxID) (TxStatus, error) {
	tlt, err := t.readTags(ctx, id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return TxStatus{Status: TxCommitStatusUnknown}, nil
		}
		return TxStatus{}, err
	}
	return tlt, nil
}

func (t TLogger) Get(ctx context.Context, id data.TxID) (TxLog, error) {
	tr, err := t.readLog(ctx, id)
	if err != nil {
		return TxLog{}, err
	}

	res := TxLog{
		ID:        id,
		Timestamp: tr.Timestamp.AsTime(),
	}
	switch tr.GetStatus() {
	case pb.TransactionLog_COMMITTED:
		res.Status = TxCommitStatusOK
	case pb.TransactionLog_ABORTED:
		res.Status = TxCommitStatusAborted
	case pb.TransactionLog_PENDING:
		res.Status = TxCommitStatusPending
	default:
		return TxLog{}, fmt.Errorf("unknown commit status %v", tr.GetStatus())
	}

	for _, cw := range tr.GetWrites() {
		for _, w := range cw.GetWrites() {
			res.Writes = append(res.Writes, TxWrite{
				Path:       path.Join(cw.GetPrefix(), w.GetSuffix()),
				Value:      w.GetValue(),
				Deleted:    w.GetDeleted(),
				PrevWriter: w.GetPrevTid(),
			})
		}
		locks := cw.GetLocks()
		if cl := locks.GetCollectionLock(); cl != pb.Lock_NONE {
			res.Locks = append(res.Locks, PathLock{
				Path: paths.CollectionInfo(cw.GetPrefix()),
				Type: parseLockType(cl),
			})
		}
		for _, l := range locks.GetLocks() {
			res.Locks = append(res.Locks, PathLock{
				Path: path.Join(cw.GetPrefix(), l.GetSuffix()),
				Type: parseLockType(l.GetLockType()),
			})
		}
	}

	return res, nil
}

func (t TLogger) Set(ctx context.Context, l TxLog) (backend.Version, error) {
	if l.Timestamp.IsZero() {
		l.Timestamp = t.clock.Now()
	}
	buf, err := marshalLog(l)
	if err != nil {
		return backend.Version{}, err
	}
	tags := logTags(l)
	m, err := t.global.WriteIfNotExists(ctx, paths.FromTransaction(t.prefix, l.ID), buf, tags)
	return m.Version, err
}

func (t TLogger) SetIf(ctx context.Context, l TxLog, expected backend.Version) (backend.Version, error) {
	if l.Timestamp.IsZero() {
		l.Timestamp = t.clock.Now()
	}
	buf, err := marshalLog(l)
	if err != nil {
		return backend.Version{}, err
	}
	tags := logTags(l)
	m, err := t.global.WriteIf(ctx, paths.FromTransaction(t.prefix, l.ID), buf, expected, tags)
	return m.Version, err
}

func (t TLogger) Delete(ctx context.Context, id data.TxID) error {
	err := t.global.Delete(ctx, paths.FromTransaction(t.prefix, id))
	if errors.Is(err, backend.ErrNotFound) {
		// Ignore if the log is already gone
		return nil
	}
	return err
}

func (t TLogger) readLog(ctx context.Context, id data.TxID) (*pb.TransactionLog, error) {
	// We can optimize for non-pending transactions by relying on the local cache.
	// It's enough to do a weak read with a very long max staleness.
	p := paths.FromTransaction(t.prefix, id)
	lr, ok := t.local.Read(p, MaxStaleness)
	if ok {
		log, err := t.parseLog(lr.Value)
		if err != nil {
			return nil, err
		}
		if log.Status != pb.TransactionLog_PENDING {
			return log, nil
		}
		// The transaction is pending, so we can't trust the locally cached
		// value. This could have been overwritten in the meantime.
		// Do a global read instead.
	}
	// We can't use the local read (either we don't have it, or it may be stale).
	gr, err := t.global.Read(ctx, p)
	if err != nil {
		return nil, err
	}
	return t.parseLog(gr.Value)
}

func (t TLogger) parseLog(buf []byte) (*pb.TransactionLog, error) {
	tr := &pb.TransactionLog{}
	if err := proto.Unmarshal(buf, tr); err != nil {
		return nil, fmt.Errorf("unmarshalling transaction log: %w", err)
	}
	return tr, nil
}

func (t TLogger) readTags(ctx context.Context, id data.TxID) (TxStatus, error) {
	// We can optimize for non-pending transactions by relying on the local cache.
	// It's enough to do a weak read with a very long max staleness.
	p := paths.FromTransaction(t.prefix, id)
	lm, ok := t.local.GetMeta(p, MaxStaleness)
	if ok {
		ts, err := parseLogTags(lm.M.Tags)
		if err != nil {
			return ts, err
		}
		ts.Version = lm.M.Version
		if ts.Status != TxCommitStatusPending {
			return ts, err
		}
		// The transaction is pending, so we can't trust the locally cached
		// value. This could have been overwritten in the meantime.
		// Do a global read instead.
	}
	// We don't have it cached. Read globally.
	gm, err := t.global.GetMetadata(ctx, p)
	if err != nil {
		return TxStatus{}, err
	}
	ts, err := parseLogTags(gm.Tags)
	ts.Version = gm.Version
	return ts, err
}

func marshalLog(l TxLog) ([]byte, error) {
	if l.ID == nil {
		return nil, errors.New("empty transaction ID")
	}

	collWrites := make(map[string]*pb.CollectionWrites)

	for _, e := range l.Writes {
		pr, err := paths.Parse(e.Path)
		if err != nil {
			return nil, err
		}
		if pr.Type != paths.KeyType {
			return nil, fmt.Errorf("expected 'key' path, got path %q", e.Path)
		}
		write := &pb.Write{
			Suffix:  path.Join(string(pr.Type), pr.Suffix),
			PrevTid: e.PrevWriter,
		}
		if e.Deleted {
			write.ValDelete = &pb.Write_Deleted{Deleted: true}
		} else {
			write.ValDelete = &pb.Write_Value{Value: e.Value}
		}
		collP := collWrites[pr.Prefix]
		if collP == nil {
			collP = &pb.CollectionWrites{
				Prefix: pr.Prefix,
				Locks:  &pb.CollectionLocks{},
			}
			collWrites[pr.Prefix] = collP
		}
		collP.Writes = append(collP.Writes, write)
	}

	for _, e := range l.Locks {
		var lt pb.Lock_LockType
		switch e.Type {
		case LockTypeNone:
			lt = pb.Lock_NONE
		case LockTypeRead:
			lt = pb.Lock_READ
		case LockTypeWrite:
			lt = pb.Lock_WRITE
		case LockTypeCreate:
			lt = pb.Lock_CREATE
		default:
			lt = pb.Lock_UNKNOWN
		}

		pr, err := paths.Parse(e.Path)
		if err != nil {
			return nil, err
		}

		var clocks *pb.CollectionLocks
		if collP := collWrites[pr.Prefix]; collP == nil {
			clocks = &pb.CollectionLocks{}
			collP = &pb.CollectionWrites{
				Prefix: pr.Prefix,
				Locks:  clocks,
			}
			collWrites[pr.Prefix] = collP
		} else {
			clocks = collP.GetLocks()
		}

		if pr.Type == paths.CollectionInfoType {
			clocks.CollectionLock = lt
		} else {
			lock := &pb.Lock{
				Suffix:   path.Join(string(pr.Type), pr.Suffix),
				LockType: lt,
			}
			clocks.Locks = append(clocks.Locks, lock)
		}
	}

	tr := &pb.TransactionLog{
		Timestamp: timestamppb.New(l.Timestamp),
	}
	switch l.Status {
	case TxCommitStatusOK:
		tr.Status = pb.TransactionLog_COMMITTED
	case TxCommitStatusAborted:
		tr.Status = pb.TransactionLog_ABORTED
	case TxCommitStatusPending:
		tr.Status = pb.TransactionLog_PENDING
	default:
		return nil, fmt.Errorf("unsupported commit status %v", l.Status)
	}

	for _, cws := range collWrites {
		tr.Writes = append(tr.Writes, cws)
	}

	buf, err := proto.Marshal(tr)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction: %w", err)
	}
	return buf, nil
}

func logTags(l TxLog) backend.Tags {
	var status string
	switch l.Status {
	case TxCommitStatusOK:
		status = commitStatusOK
	case TxCommitStatusPending:
		status = commitStatusPending
	default:
		status = commitStatusAborted
	}
	ts := l.Timestamp.UnixMilli()
	tsstr := strconv.FormatInt(ts, 10)

	return backend.Tags{
		commitStatusTag: status,
		timestampTag:    tsstr,
	}
}

func parseLogTags(t backend.Tags) (TxStatus, error) {
	st, ok := t[commitStatusTag]
	if !ok {
		return TxStatus{}, errors.New("commit-status tag not found in tx log")
	}
	var status TxCommitStatus
	switch st {
	case commitStatusOK:
		status = TxCommitStatusOK
	case commitStatusAborted:
		status = TxCommitStatusAborted
	case commitStatusPending:
		status = TxCommitStatusPending
	default:
		return TxStatus{}, fmt.Errorf("unknown commit-status tag %q", st)
	}
	ts, ok := t[timestampTag]
	if !ok {
		return TxStatus{}, errors.New("timestamp tag not found in tx log")
	}
	unixMilli, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return TxStatus{}, fmt.Errorf("parsing timestamp tag %q: %v", ts, err)
	}

	return TxStatus{
		Status:     status,
		LastUpdate: time.Unix(unixMilli/1000, 0),
	}, nil
}

func parseLockType(t pb.Lock_LockType) LockType {
	switch t {
	case pb.Lock_NONE:
		return LockTypeNone
	case pb.Lock_READ:
		return LockTypeRead
	case pb.Lock_WRITE:
		return LockTypeWrite
	case pb.Lock_CREATE:
		return LockTypeCreate
	default:
		return LockTypeUnknown
	}
}
