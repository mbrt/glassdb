// Package backend defines the interfaces for object storage backends used by
// glassdb.
package backend

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when an object does not exist in the backend.
	ErrNotFound = errors.New("object not found")
	// ErrPrecondition is returned when a conditional operation's precondition fails.
	ErrPrecondition = errors.New("precondition failed")
	// ErrEOF is returned when there are no more items to iterate over.
	ErrEOF = errors.New("end of file")
)

// Backend defines the interface for object storage operations with
// conditional reads, writes, and listing capabilities.
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

// ListIter iterates over objects returned by a list operation.
type ListIter interface {
	Next() (path string, ok bool)
	Err() error
}

// ReadReply holds the contents and version of a read object.
type ReadReply struct {
	Contents []byte
	// TODO: Consider only exposing contents version.
	Version Version
}

// Tags represents key-value metadata pairs associated with an object.
type Tags map[string]string

// Metadata holds the tags and version information of an object.
type Metadata struct {
	Tags    Tags
	Version Version
}

// Version represents the content and metadata generation numbers of an object.
type Version struct {
	Contents int64
	Meta     int64
}

// IsNull reports whether the version is unset (zero value).
func (v Version) IsNull() bool {
	return v.Contents == 0
}
