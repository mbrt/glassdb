// Package paths encodes and decodes storage paths for database objects.
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

// Type represents the category of a storage path (e.g. key, collection, transaction).
type Type string

// Path type constants for each category of storage object.
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

// FromKey encodes a key into a storage path under the given prefix.
func FromKey(prefix string, key []byte) string {
	return prefixEncode(prefix, KeyType, key)
}

// ToKey decodes a key from its storage path suffix.
func ToKey(suffix string) ([]byte, error) {
	return decode(KeyType, suffix)
}

// KeysPrefix returns the storage path prefix for listing all keys under the given prefix.
func KeysPrefix(prefix string) string {
	return typedPrefix(prefix, KeyType)
}

// FromCollection encodes a collection name into a storage path under the given prefix.
func FromCollection(prefix string, name []byte) string {
	return prefixEncode(prefix, CollectionType, name)
}

// ToCollection decodes a collection name from its storage path suffix.
func ToCollection(suffix string) ([]byte, error) {
	return decode(CollectionType, suffix)
}

// CollectionInfo returns the storage path for the collection info object under the given prefix.
func CollectionInfo(prefix string) string {
	return path.Join(prefix, "_i")
}

// IsCollectionInfo reports whether the given path refers to a collection info object.
func IsCollectionInfo(p string) bool {
	return strings.HasSuffix(p, "/_i")
}

// CollectionsPrefix returns the storage path prefix for listing all collections under the given prefix.
func CollectionsPrefix(prefix string) string {
	return typedPrefix(prefix, CollectionType)
}

// FromTransaction encodes a transaction ID into a storage path under the given prefix.
func FromTransaction(prefix string, id data.TxID) string {
	return prefixEncode(prefix, TransactionType, id)
}

// ToTransaction decodes a transaction ID from its storage path suffix.
func ToTransaction(suffix string) (data.TxID, error) {
	return decode(TransactionType, suffix)
}

// Parse splits a storage path into its prefix, type, and suffix components.
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

// ParseResult holds the components of a parsed storage path.
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
