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
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jonboulle/clockwork"
)

// TODO: Make these configurable. Compute them based on backend latency.
const (
	initialInterval = 200 * time.Millisecond
	maxInterval     = 5 * time.Second
)

func Permanent(err error) error {
	return backoff.Permanent(err)
}

func IsPermanent(err error) bool {
	var perr *backoff.PermanentError
	return errors.As(err, &perr)
}

type Retrier struct {
	backoff.BackOff
	clock clockwork.Clock
}

func RetryOptions(initial, max time.Duration, c clockwork.Clock) Retrier {
	b := backoff.NewExponentialBackOff()
	b.Clock = c
	b.InitialInterval = initial
	b.MaxInterval = max
	b.MaxElapsedTime = 0
	return Retrier{b, c}
}

func (r Retrier) Retry(ctx context.Context, fn func() error) error {
	return backoff.RetryNotifyWithTimer(fn,
		backoff.WithContext(r.BackOff, ctx), nil, &timer{r.clock, nil})
}

func RetryWithBackoff(ctx context.Context, c clockwork.Clock, f func() error) error {
	r := RetryOptions(initialInterval, maxInterval, c)
	return backoff.RetryNotifyWithTimer(f,
		backoff.WithContext(r.BackOff, ctx), nil, &timer{c, nil})
}

type timer struct {
	clockwork.Clock
	timer clockwork.Timer
}

func (t *timer) C() <-chan time.Time {
	return t.timer.Chan()
}

// Start starts the timer to fire after the given duration
func (t *timer) Start(duration time.Duration) {
	if t.timer == nil {
		t.timer = t.Clock.NewTimer(duration)
	} else {
		t.timer.Reset(duration)
	}
}

// Stop is called when the timer is not used anymore and resources may be freed.
func (t timer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}
