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

package bench

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	defaultDuration = 10 * time.Second
	minSamples      = 10
)

// Bench tracks timing samples over a configurable duration for benchmarking.
type Bench struct {
	startTime        time.Time
	totDuration      time.Duration
	expectedDuration time.Duration
	samples          []time.Duration
	m                sync.Mutex
}

// SetDuration sets the expected total duration for the benchmark run.
func (b *Bench) SetDuration(d time.Duration) {
	b.expectedDuration = d
}

// Start begins the benchmark timer.
func (b *Bench) Start() {
	b.startTime = time.Now()
	if b.expectedDuration == 0 {
		b.expectedDuration = defaultDuration
	}
}

// End records the total elapsed time since Start was called.
func (b *Bench) End() {
	b.totDuration = time.Since(b.startTime)
}

// IsTestFinished reports whether the benchmark has run long enough and collected enough samples.
func (b *Bench) IsTestFinished() bool {
	return time.Since(b.startTime) >= b.expectedDuration && len(b.samples) >= minSamples
}

// Measure times the execution of fn and records the duration as a sample.
func (b *Bench) Measure(fn func() error) error {
	start := time.Now()
	if err := fn(); err != nil {
		return err
	}
	d := time.Since(start)

	b.m.Lock()
	b.samples = append(b.samples, d)
	b.m.Unlock()
	return nil
}

// Results returns a snapshot of the collected benchmark results.
func (b *Bench) Results() Results {
	b.m.Lock()
	res := Results{
		Samples:     make([]time.Duration, len(b.samples)),
		TotDuration: b.totDuration,
	}
	copy(res.Samples, b.samples)
	b.m.Unlock()
	return res
}

// Results holds the collected timing samples and total duration of a benchmark run.
type Results struct {
	Samples     []time.Duration
	TotDuration time.Duration
}

// Avg returns the arithmetic mean of all collected samples.
func (r Results) Avg() time.Duration {
	sum := time.Duration(0)
	for _, t := range r.Samples {
		sum += t
	}
	return time.Duration(float64(sum) / float64(len(r.Samples)))
}

// Percentile returns the sample value at the given percentile (0.0 to 1.0).
func (r Results) Percentile(pctile float64) time.Duration {
	if len(r.Samples) == 0 || pctile < 0 || pctile > 1 {
		panic("invalid parameters")
	}
	xs := make([]float64, len(r.Samples))
	for i := 0; i < len(r.Samples); i++ {
		xs[i] = float64(r.Samples[i])
	}
	return time.Duration(percentile(xs, pctile))
}

func percentile(sx []float64, pctile float64) float64 {
	// Interpolation method R8 from Hyndman and Fan (1996).
	sort.Float64Slice(sx).Sort()
	N := float64(len(sx))
	// n := pctile * (N + 1) // R6
	n := 1/3.0 + pctile*(N+1/3.0) // R8
	kf, frac := math.Modf(n)
	k := int(kf)
	if k <= 0 {
		return sx[0]
	} else if k >= len(sx) {
		return sx[len(sx)-1]
	}
	return sx[k-1] + frac*(sx[k]-sx[k-1])
}
