package concurr

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

type testKey string

func TestContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		synctest.Wait()
		assert.ErrorIs(t, parent.Err(), context.Canceled)
		assert.NoError(t, ctx.Err())
		select {
		case <-ctx.Done():
			assert.True(t, false, "should be unreachable")
		default:
		}

		// Closing the 'done' channel causes cancellation.
		close(done)
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		select {
		case <-ctx.Done():
		default:
			assert.True(t, false, "should be unreachable")
		}
	})
}
