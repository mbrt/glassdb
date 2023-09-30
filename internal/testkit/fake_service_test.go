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

package testkit

import (
	"context"
	"flag"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

const testBucketName = "glassdb-test"

var debugBackend = flag.Bool("debug-backend", false, "debug backend requests")

func testClient(t *testing.T) *storage.Client {
	ctx := context.Background()
	client := NewGCSClient(ctx, t, *debugBackend)
	if err := client.Bucket(testBucketName).Create(ctx, "test-prj", nil); err != nil {
		t.Fatalf("creating test-bucket: %v", err)
	}
	return client
}

func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)
	bucket := client.Bucket(testBucketName)
	obj := bucket.Object("test-obj")

	var (
		gen     int64
		metagen int64
	)
	meta := map[string]string{"k1": "v1"}

	// Write plus metadata.
	{
		w := obj.NewWriter(ctx)
		w.Metadata = meta
		w.ContentType = "application/octet-stream"
		_, err := w.Write([]byte("value"))
		assert.NoError(t, err)
		err = w.Close()
		assert.NoError(t, err)
		assert.Equal(t, meta, w.Attrs().Metadata)
		gen = w.Attrs().Generation
		metagen = w.Attrs().Metageneration
	}

	// Read contents.
	{
		r, err := obj.NewReader(ctx)
		assert.NoError(t, err)
		buf, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "value", string(buf))
		assert.NoError(t, r.Close())

		// Check metadata in reader.
		assert.Equal(t, gen, r.Attrs.Generation)

		// Get metadata directly.
		attrs, err := obj.Attrs(ctx)
		assert.NoError(t, err)
		assert.Equal(t, meta, attrs.Metadata)
	}

	// Change metadata only.
	{
		attrs, err := obj.Update(ctx, storage.ObjectAttrsToUpdate{
			Metadata: map[string]string{"k2": "v2"},
		})
		assert.NoError(t, err)
		meta = map[string]string{"k1": "v1", "k2": "v2"}
		assert.Equal(t, meta, attrs.Metadata)
		assert.Equal(t, attrs.Generation, gen)
		assert.NotEqual(t, metagen, attrs.Metageneration)

		// Get metadata.
		attrs, err = obj.Attrs(ctx)
		assert.NoError(t, err)
		assert.Equal(t, gen, attrs.Generation)
		assert.NotEqual(t, metagen, attrs.Metageneration)

		// Update the new values.
		gen = attrs.Generation
		metagen = attrs.Metageneration
	}

	// Conditions on write.
	{
		w := obj.If(storage.Conditions{
			GenerationMatch:     gen,
			MetagenerationMatch: 300,
		}).NewWriter(ctx)
		w.Metadata = meta
		w.ContentType = "application/octet-stream"
		_, err := w.Write([]byte("value-2"))
		assert.NoError(t, err)
		err = w.Close()
		var aerr *googleapi.Error
		assert.ErrorAs(t, err, &aerr)
		assert.Equal(t, http.StatusPreconditionFailed, aerr.Code)
	}

	// Conditions on write OK.
	{
		w := obj.If(storage.Conditions{
			GenerationMatch:     gen,
			MetagenerationMatch: metagen,
		}).NewWriter(ctx)
		w.Metadata = meta
		w.ContentType = "application/octet-stream"
		_, err := w.Write([]byte("value-2"))
		assert.NoError(t, err)
		err = w.Close()
		assert.NoError(t, err)
		assert.Equal(t, meta, w.Attrs().Metadata)
	}
}

func TestList(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)
	bucket := client.Bucket(testBucketName)

	testObjs := []string{
		"root",
		"a/1",
		"a/2",
		"a/3",
		"a/40",
		"a/b/1",
		"a/b/2",
		"c/1",
	}
	// Write the test objects.
	for _, name := range testObjs {
		w := bucket.Object(name).NewWriter(ctx)
		w.Metadata = map[string]string{"k": name}
		w.ContentType = "application/octet-stream"
		_, err := w.Write([]byte(name))
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}

	// List tests.
	tests := []struct {
		name        string
		prefix      string
		delimiter   string
		startOffset string
		endOffset   string
		expected    []string
	}{
		{
			name: "full",
			expected: []string{
				"a/1",
				"a/2",
				"a/3",
				"a/40",
				"a/b/1",
				"a/b/2",
				"c/1",
				"root",
			},
		},
		{
			name:      "root objs",
			delimiter: "/",
			expected: []string{
				"root",
				"a/",
				"c/",
			},
		},
		{
			name:      "subdir",
			prefix:    "a/",
			delimiter: "/",
			expected: []string{
				"a/1",
				"a/2",
				"a/3",
				"a/40",
				"a/b/",
			},
		},
		{
			name:        "offsets",
			prefix:      "a/",
			delimiter:   "/",
			startOffset: "a/2",
			endOffset:   "a/4",
			expected: []string{
				"a/2",
				"a/3",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iter := bucket.Objects(ctx, &storage.Query{
				Delimiter:   tc.delimiter,
				Prefix:      tc.prefix,
				StartOffset: tc.startOffset,
				EndOffset:   tc.endOffset,
			})
			var res []string
			for attrs, err := iter.Next(); err == nil; attrs, err = iter.Next() {
				if attrs.Prefix != "" {
					res = append(res, attrs.Prefix)
					continue
				}
				// Check that metadata makes sense.
				v, ok := attrs.Metadata["k"]
				if !ok || !strings.HasPrefix(v, tc.prefix) {
					t.Errorf(`metadata["k"] = %q, expected prefix %q`, v, tc.prefix)
				}
				assert.Equal(t, testBucketName, attrs.Bucket)
				res = append(res, attrs.Name)
			}

			assert.Equal(t, tc.expected, res)
		})
	}
}
