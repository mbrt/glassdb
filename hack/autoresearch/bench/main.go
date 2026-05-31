// Command bench is the autoresearch scoring harness for glassdb.
//
// It runs a fixed suite of single-client workloads against the in-memory
// backend and reports a single primary score plus secondary axes. The primary
// score is a weighted count of backend operations per transaction (lower is
// better) and is deterministic for these single-client workloads, so it is
// comparable across machines and runs. The secondary axes (memory, CPU /
// runtime, lock contention) are softer, noisier signals used as tie-breakers.
//
// This file is part of the autoresearch fixed infrastructure: it defines the
// metric and must NOT be modified by autoresearch experiments.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"syscall"
	"time"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/memory"
)

const logPath = "hack/autoresearch/log.md"

// weights converts backend operation counts into a single cost. The values are
// the mean object-storage latencies (in milliseconds) reported in the README:
// object read ~57ms, object write ~70ms, metadata ~31ms. List is treated as a
// metadata-class operation.
type weights struct {
	ObjRead   float64 `json:"objRead"`
	ObjWrite  float64 `json:"objWrite"`
	MetaRead  float64 `json:"metaRead"`
	MetaWrite float64 `json:"metaWrite"`
	ObjList   float64 `json:"objList"`
}

var defaultWeights = weights{
	ObjRead:   57,
	ObjWrite:  70,
	MetaRead:  31,
	MetaWrite: 31,
	ObjList:   31,
}

func (w weights) cost(s glassdb.Stats) float64 {
	return w.ObjRead*float64(s.ObjReads) +
		w.ObjWrite*float64(s.ObjWrites) +
		w.MetaRead*float64(s.MetaReads) +
		w.MetaWrite*float64(s.MetaWrites) +
		w.ObjList*float64(s.ObjLists)
}

// workloadResult holds the measured metrics for a single workload.
type workloadResult struct {
	Name             string  `json:"name"`
	Txn              int     `json:"txn"`
	ObjReads         int     `json:"objReads"`
	ObjWrites        int     `json:"objWrites"`
	MetaReads        int     `json:"metaReads"`
	MetaWrites       int     `json:"metaWrites"`
	ObjLists         int     `json:"objLists"`
	Retries          int     `json:"retries"`
	CostPerTx        float64 `json:"costPerTx"`
	AllocBytesPerTx  float64 `json:"allocBytesPerTx"`
	AllocsPerTx      float64 `json:"allocsPerTx"`
	NsPerTx          float64 `json:"nsPerTx"`
	CPUNsPerTx       float64 `json:"cpuNsPerTx"`
	MutexWaitNsPerTx float64 `json:"mutexWaitNsPerTx"`
}

// secondary aggregates the secondary axes across all workloads.
type secondary struct {
	AllocBytesPerTx  float64 `json:"allocBytesPerTx"`
	AllocsPerTx      float64 `json:"allocsPerTx"`
	NsPerTx          float64 `json:"nsPerTx"`
	CPUNsPerTx       float64 `json:"cpuNsPerTx"`
	MutexWaitNsPerTx float64 `json:"mutexWaitNsPerTx"`
}

// suiteResult is the outcome of running the full workload suite once.
type suiteResult struct {
	Score     float64          `json:"score"`
	Secondary secondary        `json:"secondary"`
	Weights   weights          `json:"weights"`
	Workloads []workloadResult `json:"workloads"`
	Scores    []float64        `json:"scores,omitempty"`
}

func main() {
	jsonOut := flag.Bool("json", false, "emit machine-readable JSON")
	count := flag.Int("count", 1, "run the suite this many times and report the median")
	record := flag.Bool("record", false, "append a score record line to "+logPath)
	flag.Parse()

	if *count < 1 {
		*count = 1
	}

	runs := make([]suiteResult, *count)
	scores := make([]float64, *count)
	for i := range runs {
		res, err := runSuite()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		runs[i] = res
		scores[i] = res.Score
	}

	final := medianRun(runs)
	final.Scores = scores

	if *record {
		if err := appendRecord(final); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not record: %v\n", err)
		}
	}

	if *jsonOut {
		emitJSON(final)
		return
	}
	emitText(final)
}

func emitJSON(res suiteResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
}

func emitText(res suiteResult) {
	fmt.Printf("primary score (lower is better): %.2f\n", res.Score)
	if len(res.Scores) > 1 {
		fmt.Printf("per-run scores: %v\n", res.Scores)
	}
	fmt.Println()
	fmt.Printf("%-14s %6s %10s %10s %10s %10s\n",
		"workload", "txn", "cost/tx", "allocB/tx", "ns/tx", "muNs/tx")
	for _, w := range res.Workloads {
		fmt.Printf("%-14s %6d %10.1f %10.0f %10.0f %10.0f\n",
			w.Name, w.Txn, w.CostPerTx, w.AllocBytesPerTx, w.NsPerTx, w.MutexWaitNsPerTx)
	}
	fmt.Println()
	fmt.Printf("secondary (geomean over workloads):\n")
	fmt.Printf("  alloc bytes/tx: %.0f\n", res.Secondary.AllocBytesPerTx)
	fmt.Printf("  allocs/tx:      %.1f\n", res.Secondary.AllocsPerTx)
	fmt.Printf("  ns/tx:          %.0f\n", res.Secondary.NsPerTx)
	fmt.Printf("  cpu ns/tx:      %.0f\n", res.Secondary.CPUNsPerTx)
	fmt.Printf("  mutex wait ns/tx: %.0f\n", res.Secondary.MutexWaitNsPerTx)
}

// runSuite runs every workload once and aggregates the results.
func runSuite() (suiteResult, error) {
	wls := workloads()
	results := make([]workloadResult, 0, len(wls))
	for _, w := range wls {
		r, err := measure(w)
		if err != nil {
			return suiteResult{}, fmt.Errorf("workload %s: %w", w.name, err)
		}
		results = append(results, r)
	}

	costs := make([]float64, len(results))
	allocB := make([]float64, len(results))
	allocs := make([]float64, len(results))
	ns := make([]float64, len(results))
	cpu := make([]float64, len(results))
	mu := make([]float64, len(results))
	for i, r := range results {
		costs[i] = r.CostPerTx
		allocB[i] = r.AllocBytesPerTx
		allocs[i] = r.AllocsPerTx
		ns[i] = r.NsPerTx
		cpu[i] = r.CPUNsPerTx
		mu[i] = r.MutexWaitNsPerTx
	}

	return suiteResult{
		Score:   geomean(costs),
		Weights: defaultWeights,
		Secondary: secondary{
			AllocBytesPerTx:  geomean(allocB),
			AllocsPerTx:      geomean(allocs),
			NsPerTx:          geomean(ns),
			CPUNsPerTx:       geomean(cpu),
			MutexWaitNsPerTx: geomean(mu),
		},
		Workloads: results,
	}, nil
}

// measure runs a workload's setup (unmeasured), then its body while sampling
// backend operations, allocations, wall-clock, CPU time and mutex contention.
func measure(w workload) (workloadResult, error) {
	ctx := context.Background()
	db := openDB(ctx, memory.New())
	defer func() { _ = db.Close(ctx) }()

	st, err := w.setup(ctx, db)
	if err != nil {
		return workloadResult{}, fmt.Errorf("setup: %w", err)
	}

	startStats := db.Stats()
	var memStart, memEnd runtime.MemStats
	runtime.ReadMemStats(&memStart)
	muStart := mutexWaitNs()
	cpuStart := cpuNs()
	wallStart := time.Now()

	if err := w.body(ctx, db, st); err != nil {
		return workloadResult{}, fmt.Errorf("body: %w", err)
	}

	wall := time.Since(wallStart)
	cpuDelta := cpuNs() - cpuStart
	muDelta := mutexWaitNs() - muStart
	runtime.ReadMemStats(&memEnd)
	delta := db.Stats().Sub(startStats)

	txn := delta.TxN
	if txn <= 0 {
		return workloadResult{}, fmt.Errorf("no transactions recorded")
	}
	n := float64(txn)

	return workloadResult{
		Name:             w.name,
		Txn:              txn,
		ObjReads:         delta.ObjReads,
		ObjWrites:        delta.ObjWrites,
		MetaReads:        delta.MetaReads,
		MetaWrites:       delta.MetaWrites,
		ObjLists:         delta.ObjLists,
		Retries:          delta.TxRetries,
		CostPerTx:        defaultWeights.cost(delta) / n,
		AllocBytesPerTx:  float64(memEnd.TotalAlloc-memStart.TotalAlloc) / n,
		AllocsPerTx:      float64(memEnd.Mallocs-memStart.Mallocs) / n,
		NsPerTx:          float64(wall.Nanoseconds()) / n,
		CPUNsPerTx:       float64(cpuDelta) / n,
		MutexWaitNsPerTx: float64(muDelta) / n,
	}, nil
}

func openDB(ctx context.Context, b backend.Backend) *glassdb.DB {
	opts := glassdb.DefaultOptions()
	opts.Logger = slog.New(nilHandler{})
	db, err := glassdb.OpenWith(ctx, "autoresearch", b, opts)
	if err != nil {
		panic(fmt.Sprintf("opening db: %v", err))
	}
	return db
}

// geomean returns the geometric mean of the values, so that workloads with very
// different magnitudes contribute proportionally. A small floor avoids log(0).
func geomean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	const floor = 1.0
	sum := 0.0
	for _, x := range xs {
		if x < floor {
			x = floor
		}
		sum += math.Log(x)
	}
	return math.Exp(sum / float64(len(xs)))
}

// medianRun returns the run whose score is the median across runs.
func medianRun(runs []suiteResult) suiteResult {
	idx := make([]int, len(runs))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool {
		return runs[idx[a]].Score < runs[idx[b]].Score
	})
	return runs[idx[len(idx)/2]]
}

var mutexSample = []metrics.Sample{{Name: "/sync/mutex/wait/total:seconds"}}

// mutexWaitNs reports cumulative time goroutines have spent blocked on mutexes.
func mutexWaitNs() int64 {
	metrics.Read(mutexSample)
	v := mutexSample[0].Value
	if v.Kind() != metrics.KindFloat64 {
		return 0
	}
	return int64(v.Float64() * 1e9)
}

// cpuNs reports cumulative user + system CPU time consumed by this process.
func cpuNs() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return int64(user + sys)
}

func appendRecord(res suiteResult) error {
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = fmt.Fprintf(f,
		"- score-record %s primary=%.2f allocBytesPerTx=%.0f allocsPerTx=%.1f nsPerTx=%.0f cpuNsPerTx=%.0f mutexWaitNsPerTx=%.0f\n",
		time.Now().UTC().Format(time.RFC3339),
		res.Score,
		res.Secondary.AllocBytesPerTx,
		res.Secondary.AllocsPerTx,
		res.Secondary.NsPerTx,
		res.Secondary.CPUNsPerTx,
		res.Secondary.MutexWaitNsPerTx,
	)
	return err
}

// nilHandler is a slog handler that discards everything, keeping benchmark
// output clean.
type nilHandler struct{}

func (nilHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (nilHandler) Handle(context.Context, slog.Record) error { return nil }
func (h nilHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h nilHandler) WithGroup(string) slog.Handler           { return h }

func readInt(b []byte) int64 {
	res, _ := binary.Varint(b)
	return res
}

func writeInt(num int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, num)
	return buf[:n]
}
