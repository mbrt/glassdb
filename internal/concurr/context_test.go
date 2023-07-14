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

package concurr

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestContextTimeout(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ctx, cancel := ContextWithTimeout(context.Background(), clock, 10*time.Second)
	defer cancel()

	go func() {
		for {
			clock.BlockUntil(1)
			clock.Advance(time.Second)
			time.Sleep(time.Millisecond)
		}
	}()

	// The context wasn't canceled yet.
	clock.Sleep(time.Second)
	assert.NoError(t, ctx.Err())

	// The context was definitely already canceled.
	clock.Sleep(10 * time.Second)
	time.Sleep(time.Millisecond) // Wait for things to wake up.
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
}

type testKey string

func TestContextCancel(t *testing.T) {
	parent := context.WithValue(context.Background(), testKey("testKey"), "foo")
	parent, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	ctx := ContextWithNewCancel(parent, done)
	assert.NoError(t, parent.Err())

	// Values of the parent are preserved.
	gotVal := ctx.Value(testKey("testKey")).(string)
	assert.Equal(t, "foo", gotVal)

	// Cancelling the parent doesn't affect the child.
	cancel()
	assert.ErrorIs(t, parent.Err(), context.Canceled)
	assert.NoError(t, ctx.Err())
	select {
	case <-ctx.Done():
		assert.True(t, false, "should be unreachable")
	default:
		// All good here.
	}

	// Closing the 'done' channel causes cancellation.
	close(done)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	select {
	case <-ctx.Done():
	default:
		assert.True(t, false, "should be unreachable")
	}
}
