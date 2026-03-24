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

// EqualContents reports whether v and other refer to the same version content,
// comparing backend contents or writer IDs as appropriate.
func (v Version) EqualContents(other Version) bool {
	if !v.B.IsNull() {
		return v.B.Contents == other.B.Contents
	}
	return v.Writer.Equal(other.Writer)
}

// EqualMetaContents reports whether v matches the version described by the
// given backend metadata.
func (v Version) EqualMetaContents(m backend.Metadata) bool {
	if !v.B.IsNull() {
		return v.B.Contents == m.Version.Contents
	}
	writer, _ := lastWriterFromTags(m.Tags)
	return writer != nil && v.Writer.Equal(writer)
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
