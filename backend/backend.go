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

package backend

import (
	"context"
	"errors"
)

var (
	ErrNotFound     = errors.New("object not found")
	ErrPrecondition = errors.New("precondition failed")
	ErrEOF          = errors.New("end of file")
)

type Backend interface {
	ReadIfModified(ctx context.Context, path string, version int64) (ReadReply, error)
	Read(ctx context.Context, path string) (ReadReply, error)
	GetMetadata(ctx context.Context, path string) (Metadata, error)
	SetTagsIf(ctx context.Context, path string, expected Version, t Tags) (Metadata, error)
	Write(ctx context.Context, path string, value []byte, t Tags) (Metadata, error)
	WriteIf(ctx context.Context, path string, value []byte, expected Version, t Tags) (Metadata, error)
	WriteIfNotExists(ctx context.Context, path string, value []byte, t Tags) (Metadata, error)
	Delete(ctx context.Context, path string) error
	DeleteIf(ctx context.Context, path string, expected Version) error

	// List returns an iterator over the objects in the bucket within the given
	// directory (path separator is '/'). Objects will be iterated over
	// lexicographically by name.
	//
	// Note: The returned iterator is not safe for concurrent operations without
	// explicit synchronization.
	List(ctx context.Context, dirPath string) (ListIter, error)
}

type ListIter interface {
	Next() (path string, ok bool)
	Err() error
}

type ReadReply struct {
	Contents []byte
	// TODO: Consider only exposing contents version.
	Version Version
}

type Tags map[string]string

type Metadata struct {
	Tags    Tags
	Version Version
}

type Version struct {
	Contents int64
	Meta     int64
}

func (v Version) IsNull() bool {
	return v.Contents == 0
}
