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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackgroundRun(t *testing.T) {
	ctx := context.Background()
	b := NewBackground()
	ran := false
	ok := b.Go(ctx, func(context.Context) {
		ran = true
	})
	assert.True(t, ok)
	b.Close()
	assert.True(t, ran)
	assert.False(t, b.Go(ctx, func(context.Context) {}))
}

func TestBackgroundConcurrent(t *testing.T) {
	ctx := context.Background()
	b := NewBackground()
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	count := int32(0)
	ok1 := b.Go(ctx, func(context.Context) {
		<-ch1
		ch2 <- struct{}{}
		// We can reach this only if the two routines run in parallel.
		atomic.AddInt32(&count, 1)
	})
	ok2 := b.Go(ctx, func(context.Context) {
		ch1 <- struct{}{}
		<-ch2
		atomic.AddInt32(&count, 1)
	})
	b.Close()

	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, 2, int(count))
}

func TestBackgroundCancel(t *testing.T) {
	ctx := context.Background()
	b := NewBackground()
	ch := make(chan struct{})
	done := false

	b.Go(ctx, func(ctx context.Context) {
		close(ch) // Signal that we started running.
		// Wait for the context to be canceled.
		<-ctx.Done()
		time.Sleep(5 * time.Millisecond)
		done = true
	})

	// Wait for the task to start.
	<-ch

	b.Close()
	// Make sure the context in the task is canceled, and
	// the task can complete successfully.
	assert.True(t, done)

	// New tasks can't start.
	ok := b.Go(ctx, func(context.Context) {})
	assert.False(t, ok)
}

func TestNestedBackground(t *testing.T) {
	ctx := context.Background()
	b := NewBackground()
	done := make(chan struct{})

	b.Go(ctx, func(context.Context) {
		b.Go(ctx, func(context.Context) {
			close(done)
		})
	})

	<-done
	b.Close()
}
