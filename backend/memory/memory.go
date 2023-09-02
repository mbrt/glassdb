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

package memory

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/stringset"
)

func New() *Backend {
	return &Backend{
		objects: make(map[string]object),
		nextGen: 1,
	}
}

type Backend struct {
	objects map[string]object
	nextGen int64
	m       sync.Mutex
}

func (b *Backend) ReadIfModified(
	ctx context.Context,
	path string,
	version int64,
) (backend.ReadReply, error) {
	if err := ctx.Err(); err != nil {
		return backend.ReadReply{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.ReadReply{}, backend.ErrNotFound
	}
	if obj.Version.Contents == version {
		return backend.ReadReply{}, backend.ErrPrecondition
	}
	return backend.ReadReply{
		Contents: obj.Data,
		Version:  obj.Version,
	}, nil
}

func (b *Backend) Read(ctx context.Context, path string) (backend.ReadReply, error) {
	if err := ctx.Err(); err != nil {
		return backend.ReadReply{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.ReadReply{}, backend.ErrNotFound
	}
	return backend.ReadReply{
		Contents: obj.Data,
		Version:  obj.Version,
	}, nil
}

func (b *Backend) GetMetadata(
	ctx context.Context,
	path string,
) (backend.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return backend.Metadata{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.Metadata{}, backend.ErrNotFound
	}
	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.Version,
	}, nil
}

func (b *Backend) SetTagsIf(
	ctx context.Context,
	path string,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return backend.Metadata{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.Metadata{}, backend.ErrNotFound
	}
	if obj.Version != expected {
		return backend.Metadata{}, backend.ErrPrecondition
	}
	b.updateTags(&obj, t)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.Version,
	}, nil
}

func (b *Backend) Write(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return backend.Metadata{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj := b.objects[path]
	b.updateTags(&obj, t)
	b.updateData(&obj, value)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.Version,
	}, nil
}

func (b *Backend) WriteIf(
	ctx context.Context,
	path string,
	value []byte,
	expected backend.Version,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return backend.Metadata{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.Metadata{}, backend.ErrNotFound
	}
	if obj.Version != expected {
		return backend.Metadata{}, backend.ErrPrecondition
	}
	b.updateTags(&obj, t)
	b.updateData(&obj, value)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.Version,
	}, nil
}

func (b *Backend) WriteIfNotExists(
	ctx context.Context,
	path string,
	value []byte,
	t backend.Tags,
) (backend.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return backend.Metadata{}, err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if ok {
		return backend.Metadata{}, backend.ErrPrecondition
	}
	b.updateTags(&obj, t)
	b.updateData(&obj, value)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.Version,
	}, nil
}

func (b *Backend) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.m.Lock()
	defer b.m.Unlock()

	_, ok := b.objects[path]
	if !ok {
		return backend.ErrNotFound
	}
	delete(b.objects, path)
	return nil
}

func (b *Backend) DeleteIf(
	ctx context.Context,
	path string,
	expected backend.Version,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.m.Lock()
	defer b.m.Unlock()

	obj, ok := b.objects[path]
	if !ok {
		return backend.ErrNotFound
	}
	if obj.Version != expected {
		return backend.ErrPrecondition
	}
	delete(b.objects, path)
	return nil
}

func (b *Backend) List(ctx context.Context, dirPath string) (backend.ListIter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	b.m.Lock()
	defer b.m.Unlock()

	ps := stringset.New()
	for k := range b.objects {
		if !strings.HasPrefix(k, dirPath) {
			continue
		}
		// Drop subdirs from the path.
		i := len(dirPath)
		for ; i < len(k) && k[i] != '/'; i++ {
		}
		ps.Add(k[:i])
	}

	paths := ps.ToSlice()
	sort.Strings(paths)
	return &listIter{
		paths: paths,
	}, nil
}

func (b *Backend) updateTags(obj *object, t backend.Tags) {
	if len(t) == 0 {
		return
	}
	if obj.Tags == nil {
		obj.Tags = make(backend.Tags)
	}
	for k, v := range t {
		obj.Tags[k] = v
	}
	obj.Version.Meta++
}

func (b *Backend) updateData(obj *object, d []byte) {
	obj.Data = d
	obj.Version.Contents = b.nextGeneration()
}

func (b *Backend) nextGeneration() int64 {
	// Note that this is not locked.
	res := b.nextGen
	b.nextGen++
	return res
}

type listIter struct {
	paths []string
	curr  int
}

func (l *listIter) Next() (path string, ok bool) {
	if l.curr >= len(l.paths) {
		return "", false
	}
	res := l.paths[l.curr]
	l.curr++
	return res, true
}

func (l *listIter) Err() error {
	return nil
}

type object struct {
	Data    []byte
	Tags    backend.Tags
	Version backend.Version
}

func copyTags(t backend.Tags) backend.Tags {
	if len(t) == 0 {
		return nil
	}
	res := make(backend.Tags)
	for k, v := range t {
		res[k] = v
	}
	return res
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*Backend)(nil)
