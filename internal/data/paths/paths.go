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

package paths

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/mbrt/glassdb/internal/data"
)

type Type string

const (
	UnknownType        Type = ""
	KeyType            Type = "_k"
	CollectionType     Type = "_c"
	TransactionType    Type = "_t"
	CollectionInfoType Type = "_i"
)

const encodeAlphabet = "0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

// This base64 encoding preserves the lexicographical ordering.
var encoding = base64.NewEncoding(encodeAlphabet).WithPadding(base64.NoPadding)

var bufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func FromKey(prefix string, key []byte) string {
	return prefixEncode(prefix, KeyType, key)
}

func ToKey(suffix string) ([]byte, error) {
	return decode(KeyType, suffix)
}

func KeysPrefix(prefix string) string {
	return typedPrefix(prefix, KeyType)
}

func FromCollection(prefix string, name []byte) string {
	return prefixEncode(prefix, CollectionType, name)
}

func ToCollection(suffix string) ([]byte, error) {
	return decode(CollectionType, suffix)
}

func CollectionInfo(prefix string) string {
	return path.Join(prefix, "_i")
}

func IsCollectionInfo(p string) bool {
	return strings.HasSuffix(p, "/_i")
}

func CollectionsPrefix(prefix string) string {
	return typedPrefix(prefix, CollectionType)
}

func FromTransaction(prefix string, id data.TxID) string {
	return prefixEncode(prefix, TransactionType, id)
}

func ToTransaction(suffix string) (data.TxID, error) {
	return decode(TransactionType, suffix)
}

func Parse(p string) (ParseResult, error) {
	// Collection info is a special case.
	// Example path: foo/bar/_i -> prefix: foo/bar, suffix: ""
	if IsCollectionInfo(p) {
		return ParseResult{
			Prefix: p[:len(p)-3],
			Type:   CollectionInfoType,
		}, nil
	}

	// The path is composed by three parts:
	// prefix, type, suffix; separated by /.
	prefixIdx, typeIdx := pathPartsIndexes(p)
	if prefixIdx < 0 || typeIdx < 0 {
		return ParseResult{}, fmt.Errorf("expected path with >=3 parts, got %q", p)
	}
	prefix := p[:prefixIdx]
	typStr := p[prefixIdx+1 : typeIdx]
	suffix := p[typeIdx+1:]

	typ := UnknownType
	switch Type(typStr) {
	case KeyType:
		typ = KeyType
	case CollectionType:
		typ = CollectionType
	case TransactionType:
		typ = TransactionType
	}

	return ParseResult{
		Prefix: prefix,
		Suffix: suffix,
		Type:   typ,
	}, nil
}

type ParseResult struct {
	Prefix string
	Suffix string
	Type   Type
}

func pathPartsIndexes(p string) (prefixIdx int, typeIdx int) {
	typeIdx = strings.LastIndexByte(p, '/')
	if typeIdx <= 0 {
		return -1, -1
	}
	prefixIdx = strings.LastIndexByte(p[:typeIdx-1], '/')
	if typeIdx <= 0 {
		return -1, typeIdx
	}
	return prefixIdx, typeIdx
}

func typedPrefix(prefix string, t Type) string {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.WriteString(prefix)
	buf.WriteByte('/')
	buf.WriteString(string(t))
	buf.WriteByte('/')
	res := buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return res
}

func prefixEncode(prefix string, category Type, a []byte) string {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.WriteString(prefix)
	buf.WriteByte('/')
	encode(buf, category, a)
	res := buf.String()
	buf.Reset()
	bufPool.Put(buf)
	return res
}

func encode(buf *bytes.Buffer, category Type, a []byte) {
	buf.WriteString(string(category))
	buf.WriteByte('/')
	buf.WriteString(encoding.EncodeToString(a))
}

func decode(category Type, suffix string) ([]byte, error) {
	trimmed := strings.TrimPrefix(suffix, string(category)+"/")
	if trimmed == suffix {
		return nil, fmt.Errorf(`got path %q, expected prefix %q`, suffix, category)
	}
	return encoding.DecodeString(trimmed)
}
