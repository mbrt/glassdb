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

//go:build tracing

package trace

import (
	"context"
	"runtime/trace"
)

func NewTask(ctx context.Context, n string) (context.Context, *trace.Task) {
	return trace.NewTask(ctx, n)
}

func WithRegion(ctx context.Context, n string, fn func()) {
	trace.WithRegion(ctx, n, fn)
}

func StartRegion(ctx context.Context, n string) *trace.Region {
	return trace.StartRegion(ctx, n)
}
