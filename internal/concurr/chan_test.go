package concurr

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestChanInfCap(t *testing.T) {
	tests := []struct {
		name     string
		waitSend bool
		waitRecv bool
	}{
		{
			name: "no wait",
		},
		{
			name:     "buffer at send",
			waitRecv: true,
		},
		{
			name:     "buffer at recv",
			waitSend: true,
		},
	}

	wait := func(ok bool) {
		if !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, in := MakeChanInfCap[int](1)
			var rec []int

			g := errgroup.Group{}
			g.Go(func() error {
				for v := range out {
					wait(tc.waitRecv)
					rec = append(rec, v)
				}
				return nil
			})

			for i := range 100 {
				wait(tc.waitSend)
				in <- i
			}
			close(in)
			_ = g.Wait()

			assert.Len(t, rec, 100)
		})
	}

}
