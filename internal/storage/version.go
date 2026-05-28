package storage

import (
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/internal/data"
)

// Version represents a storage object's version, combining a backend version
// with an optional local transaction writer.
type Version struct {
	B      backend.Version
	Writer data.TxID
}

// IsNull reports whether the version has no backend version and no writer.
func (v Version) IsNull() bool {
	return v.B.IsNull() && v.Writer == nil
}

// IsLocal reports whether the version was written by a local transaction
// that has not yet been committed to the backend.
func (v Version) IsLocal() bool {
	return v.Writer != nil
}

// EqualContents reports whether v and other refer to the same value, by
// comparing the writer transaction IDs. Two versions with no writer (i.e.
// never written by GlassDB) compare equal.
func (v Version) EqualContents(other Version) bool {
	return v.Writer.Equal(other.Writer)
}

// EqualMetaContents reports whether v matches the version described by the
// given backend metadata. It compares the last-writer transaction ID found in
// the metadata tags against v's writer. Two versions with no writer compare
// equal (covers never-written or externally-created keys).
func (v Version) EqualMetaContents(m backend.Metadata) bool {
	writer, _ := lastWriterFromTags(m.Tags)
	return v.Writer.Equal(writer)
}

// VersionFromMeta constructs a Version from backend metadata, extracting the
// last writer from the metadata tags.
func VersionFromMeta(m backend.Metadata) Version {
	writer, _ := lastWriterFromTags(m.Tags)
	return Version{
		B:      m.Version,
		Writer: writer,
	}
}
