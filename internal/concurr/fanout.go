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

	"golang.org/x/sync/errgroup"
)

func NewFanout(maxConcurrent int) Fanout {
	return Fanout{
		limit: maxConcurrent,
	}
}

type Fanout struct {
	limit int
}

func (o Fanout) Spawn(ctx context.Context, num int, f func(context.Context, int) error) Result {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(o.limit)

	for i := 0; i < num; i++ {
		i := i // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return f(ctx, i)
		})
	}

	return g
}

type Result interface {
	Wait() error
}
