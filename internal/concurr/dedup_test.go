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
	tw := &testMergeWorker{waitRequests: 1}
	ctx := context.Background()
	d := NewDedup(tw)
	wg := errgroup.Group{}
	wg.Go(func() error {
		return d.Do(ctx, "key", mergeableRequest(1))
	})
	err := d.Do(ctx, "key", mergeableRequest(1))
	assert.NoError(t, err)
	err = wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{2}, tw.res)
}

func TestSequentialDo(t *testing.T) {
	tw := &testMergeWorker{waitRequests: 1}
	ctx := context.Background()
	d := NewDedup(tw)
	wg := errgroup.Group{}
	wg.Go(func() error {
		return d.Do(ctx, "key", unmergeableRequest(1))
	})
	err := d.Do(ctx, "key", mergeableRequest(1))
	assert.NoError(t, err)
	err = wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 1}, tw.res)
}

func TestReorderMerge(t *testing.T) {
	tw := &testMergeWorker{waitRequests: 2}
	ctx := context.Background()
	d := NewDedup(tw)
	wg := errgroup.Group{}
	wg.Go(func() error {
		return d.Do(ctx, "key", unmergeableRequest(2))
	})
	wg.Go(func() error {
		return d.Do(ctx, "key", reorderableRequest(3))
	})
	err := d.Do(ctx, "key", mergeableRequest(5))
	assert.NoError(t, err)
	err = wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, []int{8, 2}, tw.res)
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
	waitRequests int
	res          []int
}

func (t *testMergeWorker) Work(ctx context.Context, key string, cntr DedupContr) error {
	for t.waitRequests > 0 {
		select {
		case <-cntr.OnNextDo(key):
		case <-ctx.Done():
			return ctx.Err()
		}
		t.waitRequests--
	}

	r := cntr.Request(key).(testRequest)
	t.res = append(t.res, r.counter)
	return nil
}

func mergeableRequest(counter int) testRequest {
	return testRequest{counter: counter, canMerge: true}
}

func unmergeableRequest(counter int) testRequest {
	return testRequest{counter: counter}
}

func reorderableRequest(counter int) testRequest {
	return testRequest{counter: counter, canMerge: true, canReorder: true}
}

type testRequest struct {
	counter    int
	canMerge   bool
	canReorder bool
}

func (r testRequest) CanReorder() bool { return r.canReorder }

func (r testRequest) Merge(other Request) (Request, bool) {
	or, ok := other.(testRequest)
	if !ok {
		return nil, false
	}
	if !r.canMerge || !or.canMerge {
		return nil, false
	}
	return mergeableRequest(r.counter + or.counter), true
}
