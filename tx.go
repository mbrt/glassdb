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

package glassdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data/paths"
	"github.com/mbrt/glassdb/internal/storage"
	"github.com/mbrt/glassdb/internal/trans"
	"github.com/sourcegraph/conc"
)

var ErrAborted = errors.New("aborted transaction")

func newTx(
	ctx context.Context,
	g storage.Global,
	l storage.Local,
	tm *trans.Monitor,
) *Tx {
	return &Tx{
		ctx:    ctx,
		global: g,
		local:  l,
		reader: trans.NewReader(l, g, tm),
		staged: make(map[string]tvalue),
		reads:  make(map[string]readInfo),
	}
}

type Tx struct {
	ctx     context.Context
	global  storage.Global
	local   storage.Local
	reader  trans.Reader
	staged  map[string]tvalue
	reads   map[string]readInfo
	aborted bool
	retried bool
}

func (t *Tx) Read(c Collection, key []byte) ([]byte, error) {
	p := paths.FromKey(c.prefix, key)
	// Return the last read, if present.
	tval, ok := t.staged[p]
	if ok {
		return tval.val, nil
	}
	if info, ok := t.reads[p]; ok && !info.found {
		// Be consistent with values not found the first time.
		// Do not try to fetch again, to avoid phantom reads.
		return nil, backend.ErrNotFound
	}

	// First time we read this in the transaction.
	rv, err := t.reader.Read(t.ctx, p, storage.MaxStaleness)
	if errors.Is(err, backend.ErrNotFound) {
		// Cache the fact that the value wasn't present.
		// Avoids phantom reads.
		t.reads[p] = readInfo{found: false}
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("reading from storage: %w", err)
	}
	// Cache locally and mark the read.
	t.staged[p] = tvalue{val: rv.Value}
	t.reads[p] = readInfo{
		version: trans.ReadVersion{
			Version:    rv.Version.B.Contents,
			LastWriter: rv.Version.Writer,
		},
		found: true,
	}
	return rv.Value, nil
}

func (t *Tx) ReadMulti(ks []FQKey) []ReadResult {
	if len(ks) == 0 {
		return nil
	}

	res := make([]ReadResult, len(ks))
	infos := make([]readInfoEx, len(ks))
	wg := conc.WaitGroup{}

	for i, key := range ks {
		i := i // https://golang.org/doc/faq#closures_and_goroutines

		p := paths.FromKey(key.Collection.prefix, key.Key)
		infos[i].path = p

		// Return the last read, if present.
		tval, ok := t.staged[p]
		if ok {
			res[i] = ReadResult{Value: tval.val}
			infos[i].cached = true
			continue
		}
		if info, ok := t.reads[p]; ok && !info.found {
			// Be consistent with values not found the first time.
			// Do not try to fetch again, to avoid phantom reads.
			res[i] = ReadResult{Err: backend.ErrNotFound}
			infos[i].cached = true
			continue
		}

		// Fetch in parallel.
		wg.Go(func() {
			rv, err := t.reader.Read(t.ctx, p, storage.MaxStaleness)
			if err != nil && !errors.Is(err, backend.ErrNotFound) {
				err = fmt.Errorf("reading from storage: %w", err)
			}
			res[i] = ReadResult{Value: rv.Value, Err: err}
			if err != nil {
				return
			}
			infos[i].version = trans.ReadVersion{
				Version:    rv.Version.B.Contents,
				LastWriter: rv.Version.Writer,
			}
			infos[i].found = true
		})
	}

	wg.Wait()

	// Update the local staged with what was just fetched.
	// We need to do it serially to avoid race conditions or locking.
	for i, info := range infos {
		if info.cached {
			continue
		}
		if !info.found {
			t.reads[info.path] = readInfo{found: false}
			continue
		}
		t.staged[info.path] = tvalue{val: res[i].Value}
		t.reads[info.path] = readInfo{
			version: info.version,
			found:   true,
		}
	}

	return res
}

func (t *Tx) Write(c Collection, key, value []byte) error {
	if key == nil {
		return errors.New("invalid nil key")
	}
	p := paths.FromKey(c.prefix, key)
	t.staged[p] = tvalue{val: value, modified: true}
	return nil
}

func (t *Tx) Delete(c Collection, key []byte) error {
	if key == nil {
		return errors.New("invalid nil key")
	}
	p := paths.FromKey(c.prefix, key)
	t.staged[p] = tvalue{deleted: true}
	return nil
}

func (t *Tx) Abort() error {
	t.aborted = true
	return ErrAborted
}

func (t *Tx) reset() {
	t.staged = make(map[string]tvalue)
	t.reads = make(map[string]readInfo)
	t.retried = true
}

func (t *Tx) collectAccesses() trans.Data {
	var reads []trans.ReadAccess
	var writes []trans.WriteAccess

	// Collect writes.
	for k, v := range t.staged {
		if !v.modified && !v.deleted {
			continue
		}
		writes = append(writes, trans.WriteAccess{
			Path:   k,
			Val:    v.val,
			Delete: v.deleted,
		})
	}
	// Collect reads.
	for k, v := range t.reads {
		reads = append(reads, trans.ReadAccess{
			Path:    k,
			Version: v.version,
			Found:   v.found,
		})
	}

	return trans.Data{
		Reads:  reads,
		Writes: writes,
	}
}

// FQKey is a fully qualified key: collection + key name.
type FQKey struct {
	Collection Collection
	Key        []byte
}

type ReadResult struct {
	Value []byte
	Err   error
}

type tvalue struct {
	val      []byte
	modified bool
	deleted  bool
}

type readInfo struct {
	version trans.ReadVersion
	found   bool
}

type readInfoEx struct {
	path    string
	version trans.ReadVersion
	found   bool
	cached  bool
}
