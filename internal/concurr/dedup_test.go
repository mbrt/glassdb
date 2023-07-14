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

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestSingleCall(t *testing.T) {
	tw := &testWorker{}
	d := NewDedup(tw)
	err := d.Do(context.Background(), "key", testRequest{})
	assert.NoError(t, err)
	assert.Equal(t, 1, tw.counter)
}

func TestContextExpired(t *testing.T) {
	tw := &testWorker{}
	d := NewDedup(tw)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Do(ctx, "key", testRequest{})
	assert.ErrorIs(t, err, ctx.Err())
	assert.Equal(t, 1, tw.counter)
}

func TestMergeDo(t *testing.T) {
	tw := &testMergeWorker{}
	ctx := context.Background()
	d := NewDedup(tw)
	wg := errgroup.Group{}
	wg.Go(func() error {
		return d.Do(ctx, "key", testRequest{counter: 1})
	})
	err := d.Do(ctx, "key", testRequest{counter: 1})
	assert.NoError(t, err)
	err = wg.Wait()
	assert.NoError(t, err)
}

type testWorker struct {
	counter int
}

func (t *testWorker) Work(ctx context.Context, key string, cntr DedupContr) error {
	_ = cntr.Request(key)
	t.counter++
	return ctx.Err()
}

type testMergeWorker struct {
	res int
}

func (t *testMergeWorker) Work(ctx context.Context, key string, cntr DedupContr) error {
	select {
	case <-cntr.OnNextDo(key):
	case <-ctx.Done():
		return ctx.Err()
	}

	r := cntr.Request(key).(testRequest)
	t.res = r.counter
	return nil
}

type testRequest struct {
	counter int
}

func (r testRequest) CanReorder() bool { return false }

func (r testRequest) Merge(other Request) (Request, bool) {
	or, ok := other.(testRequest)
	if !ok {
		return nil, false
	}
	return testRequest{r.counter + or.counter}, true
}
