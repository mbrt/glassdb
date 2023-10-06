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
	"time"

	"github.com/jonboulle/clockwork"
)

func ContextWithTimeout(
	parent context.Context,
	clock clockwork.Clock,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	clock.AfterFunc(timeout, func() {
		cancel()
	})
	return ctx, cancel
}

// ContextWithNewCancel replaces the parent cancellation (if any) with the
// given one, while preserving the embedded values.
func ContextWithNewCancel(parent context.Context, done <-chan struct{}) context.Context {
	return detachedCtx{
		parent: parent,
		done:   done,
	}
}

type detachedCtx struct {
	parent context.Context
	done   <-chan struct{}
}

func (v detachedCtx) Deadline() (time.Time, bool)       { return time.Time{}, false }
func (v detachedCtx) Done() <-chan struct{}             { return v.done }
func (v detachedCtx) Value(key interface{}) interface{} { return v.parent.Value(key) }

func (v detachedCtx) Err() error {
	select {
	case <-v.done:
		return context.Canceled
	default:
		return nil
	}
}
