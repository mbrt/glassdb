// Package memory implements an in-memory backend for testing and development.
package memory

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/stringset"
)

// formatToken encodes a generation and metageneration into an opaque
// backend.Version token. The format intentionally matches the GCS backend,
// so tests that exercise both stay consistent.
func formatToken(gen, metagen int64) string {
	return strconv.FormatInt(gen, 10) + "/" + strconv.FormatInt(metagen, 10)
}

func parseToken(token string) (gen, metagen int64, err error) {
	before, after, ok := strings.Cut(token, "/")
	if !ok {
		return 0, 0, fmt.Errorf("invalid token %q", token)
	}
	gen, err = strconv.ParseInt(before, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid token %q: %w", token, err)
	}
	metagen, err = strconv.ParseInt(after, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid token %q: %w", token, err)
	}
	return gen, metagen, nil
}

// New creates a new in-memory Backend.
func New() *Backend {
	return &Backend{
		objects: make(map[string]object),
		nextGen: 1,
	}
}

// Backend is an in-memory implementation of backend.Backend.
type Backend struct {
	objects map[string]object
	nextGen int64
	m       sync.Mutex
}

// ReadIfModified implements backend.Backend.
func (b *Backend) ReadIfModified(
	ctx context.Context,
	path string,
	expectedWriter backend.WriterID,
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
	if obj.Tags[backend.LastWriterTag] == backend.EncodeWriterTag(expectedWriter) {
		return backend.ReadReply{}, backend.ErrPrecondition
	}
	return backend.ReadReply{
		Contents: obj.Data,
		Version:  obj.version(),
		Tags:     copyTags(obj.Tags),
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
		Version:  obj.version(),
		Tags:     copyTags(obj.Tags),
	}, nil
}

// GetMetadata implements backend.Backend.
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
		Version: obj.version(),
	}, nil
}

// SetTagsIf implements backend.Backend.
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
	if obj.version() != expected {
		return backend.Metadata{}, backend.ErrPrecondition
	}
	b.updateTags(&obj, t)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.version(),
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
		Version: obj.version(),
	}, nil
}

// WriteIf implements backend.Backend.
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
	if obj.version() != expected {
		return backend.Metadata{}, backend.ErrPrecondition
	}
	b.updateTags(&obj, t)
	b.updateData(&obj, value)
	b.objects[path] = obj

	return backend.Metadata{
		Tags:    copyTags(obj.Tags),
		Version: obj.version(),
	}, nil
}

// WriteIfNotExists implements backend.Backend.
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
		Version: obj.version(),
	}, nil
}

// Delete implements backend.Backend.
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

// DeleteIf implements backend.Backend.
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
	if obj.version() != expected {
		return backend.ErrPrecondition
	}
	delete(b.objects, path)
	return nil
}

// List implements backend.Backend.
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
		rest := k[len(dirPath):]
		if idx := strings.IndexByte(rest, '/'); idx >= 0 {
			ps.Add(k[:len(dirPath)+idx])
		} else {
			ps.Add(k)
		}
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
	maps.Copy(obj.Tags, t)
	obj.Metagen++
}

func (b *Backend) updateData(obj *object, d []byte) {
	obj.Data = d
	obj.Gen = b.nextGeneration()
	obj.Metagen = 1
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
	Gen     int64
	Metagen int64
}

// version returns the opaque backend.Version token for this object.
func (o object) version() backend.Version {
	return backend.Version{Token: formatToken(o.Gen, o.Metagen)}
}

func copyTags(t backend.Tags) backend.Tags {
	if len(t) == 0 {
		return nil
	}
	res := make(backend.Tags)
	maps.Copy(res, t)
	return res
}

// Ensure that Backend interface is implemented correctly.
var _ backend.Backend = (*Backend)(nil)
