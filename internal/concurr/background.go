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
	"sync"

	"github.com/sourcegraph/conc"
)

func NewBackground() *Background {
	return &Background{
		done: make(chan struct{}),
	}
}

type Background struct {
	wg     conc.WaitGroup
	done   chan struct{}
	closed bool
	m      sync.Mutex
}

func (b *Background) Close() {
	close(b.done)
	b.m.Lock()
	b.closed = true
	b.m.Unlock()
	b.wg.Wait()
}

func (b *Background) Closed() bool {
	b.m.Lock()
	defer b.m.Unlock()
	return b.closed
}

func (b *Background) Go(ctx context.Context, fn func(context.Context)) bool {
	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return false
	}
	b.wg.Go(func() {
		fn(ContextWithNewCancel(ctx, b.done))
	})
	return true
}
