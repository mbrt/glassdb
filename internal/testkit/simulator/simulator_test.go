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

package simulator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mbrt/glassdb"
)

func TestBase(t *testing.T) {
	// The first sleeps more than the second (10 vs 5 ms).
	sim := New(t, []byte{10, 5})
	ch := make(chan int, 2)
	sim.Run("one", nil, func(ctx context.Context, d *glassdb.DB) error {
		ch <- 0
		return nil
	})
	sim.Run("two", nil, func(ctx context.Context, d *glassdb.DB) error {
		ch <- 1
		return nil
	})
	err := sim.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 0}, []int{<-ch, <-ch})
}

func TestDB(t *testing.T) {
	sim := New(t, []byte{1, 2, 3, 4})
	db := sim.DBInstance()
	sim.Run("one", db, func(ctx context.Context, db *glassdb.DB) error {
		coll := db.Collection([]byte("foo"))
		if err := coll.Create(ctx); err != nil {
			return err
		}
		if err := coll.Write(ctx, []byte("key"), []byte("val")); err != nil {
			return err
		}
		rv, err := coll.ReadStrong(ctx, []byte("key"))
		if err != nil {
			return err
		}
		if string(rv) != "val" {
			return fmt.Errorf("unexpected read: %q", string(rv))
		}
		return nil
	})
	err := sim.Wait()
	assert.NoError(t, err)
}
