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

//go:build !tracing

package trace

import "context"

// NewTask returns a no-op tracing task when the tracing build tag is disabled.
func NewTask(ctx context.Context, _ string) (context.Context, Task) {
	return ctx, Task{}
}

// Task is a no-op implementation of a tracing task.
type Task struct{}

// End is a no-op that satisfies the tracing task interface.
func (Task) End() {}

// WithRegion calls fn directly without tracing when the tracing build tag is disabled.
func WithRegion(_ context.Context, _ string, fn func()) {
	fn()
}

// StartRegion returns a no-op Region when the tracing build tag is disabled.
func StartRegion(context.Context, string) Region {
	return Region{}
}

// Region is a no-op implementation of a tracing region.
type Region struct{}

// End is a no-op that satisfies the tracing region interface.
func (Region) End() {}
