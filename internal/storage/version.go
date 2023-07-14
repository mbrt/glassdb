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

type Version struct {
	B      backend.Version
	Writer data.TxID
}

func (v Version) IsNull() bool {
	return v.B.IsNull() && v.Writer == nil
}

func (v Version) IsLocal() bool {
	return v.Writer != nil
}

func (v Version) EqualContents(other Version) bool {
	if !v.B.IsNull() {
		return v.B.Contents == other.B.Contents
	}
	return v.Writer.Equal(other.Writer)
}

func (v Version) EqualMetaContents(m backend.Metadata) bool {
	if !v.B.IsNull() {
		return v.B.Contents == m.Version.Contents
	}
	writer, _ := lastWriterFromTags(m.Tags)
	return writer != nil && v.Writer.Equal(writer)
}

func VersionFromMeta(m backend.Metadata) Version {
	writer, _ := lastWriterFromTags(m.Tags)
	return Version{
		B:      m.Version,
		Writer: writer,
	}
}
