// Package backend defines the interfaces for object storage backends used by
// glassdb.
package backend

import (
	"context"
	"encoding/base64"
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

// WriterID is the opaque identifier of the transaction that last wrote an
// object. It mirrors internal/data.TxID, redeclared here to keep the backend
// package independent of internal packages.
type WriterID []byte

// Equal reports whether two writer IDs refer to the same transaction. Two
// empty (nil) writers compare equal.
func (w WriterID) Equal(other WriterID) bool {
	if len(w) != len(other) {
		return false
	}
	for i := range w {
		if w[i] != other[i] {
			return false
		}
	}
	return true
}

// LastWriterTag is the name of the tag that records the transaction ID of the
// most recent writer of an object. Backends that implement writer-based
// ReadIfModified read this tag to compare against the expected writer.
const LastWriterTag = "last-writer"

// EncodeWriterTag encodes a WriterID into the string form used in object
// tags. Returns the empty string when the writer is nil.
func EncodeWriterTag(w WriterID) string {
	if len(w) == 0 {
		return ""
	}
	return base64.URLEncoding.EncodeToString(w)
}

// Backend defines the interface for object storage operations with
// conditional reads, writes, and listing capabilities.
type Backend interface {
	// ReadIfModified returns the object's contents, version, and tags if the
	// last-writer recorded in the tags differs from expectedWriter. Otherwise
	// returns ErrPrecondition.
	ReadIfModified(ctx context.Context, path string, expectedWriter WriterID) (ReadReply, error)
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

// ReadReply holds the contents, version, and tags of a read object.
type ReadReply struct {
	Contents []byte
	Version  Version
	Tags     Tags
}

// Tags represents key-value metadata pairs associated with an object.
type Tags map[string]string

// Metadata holds the tags and version information of an object.
type Metadata struct {
	Tags    Tags
	Version Version
}

// Version is an opaque CAS token identifying a particular generation of an
// object. The exact format is backend-specific; consumers should treat it as
// an opaque string and only use it for equality (via IsNull) or by passing it
// back unchanged to conditional operations.
type Version struct {
	Token string
}

// IsNull reports whether the version is unset (zero value).
func (v Version) IsNull() bool {
	return v.Token == ""
}
