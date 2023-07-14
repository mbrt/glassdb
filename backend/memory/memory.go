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

package memory

import (
	"context"
	"testing"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/internal/testkit"
)

func New(ctx context.Context, t testing.TB, debug bool) backend.Backend {
	// TODO: Do a proper memory backend implementation.
	// TODO: Tests should test both GCS and memory backends.
	client := testkit.NewGCSClient(ctx, t, debug)
	bucket := client.Bucket("test")
	if err := bucket.Create(ctx, "", nil); err != nil {
		t.Fatalf("creating 'test' bucket: %v", err)
	}
	return gcs.New(bucket)
}
