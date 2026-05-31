// Rtbench measures database transaction performance under various concurrency
// scenarios.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/backend/s3"
	"github.com/mbrt/glassdb/internal/testkit/bench"
)

const (
	readWrite9010Cname        = "read-write-9010"
	readWrite9010Duration     = time.Minute
	readWrite9010MaxDBs       = 50
	readWrite9010NumConcurrTx = 10
	readWrite9010NumKeys      = 50000
)

const deadlockNumWriters = 5

var (
	backendType      = flag.String("backend", "memory", "select backend type [memory|gcs|s3]")
	memoryDelays     = flag.String("delays", "gcs", "delay profile for the memory backend [gcs|s3]")
	enableThrottling = flag.Bool("enable-throttling", true, "enable throttling with memory backend")
	testName         = flag.String("test-name", "simple", "which test to run [simple|rw9010|deadlock]")
	samplesOut       = flag.String("samples-out", "samples.csv", "output file with raw samples data")
	statsOut         = flag.String("stats-out", "stats.csv", "output file with db stats")
	throughputOut    = flag.String("throughput-out", "throughput.csv", "output file with per-db throughput data")
	clientStatsOut   = flag.String("client-stats-out", "client-stats.csv", "output file with per-step client resource metrics (CPU, HTTP, connections)")
	deadlockOut      = flag.String("deadlock-out", "deadlock.csv", "output file with deadlock latency samples")
	maxDBs           = flag.Int("max-dbs", readWrite9010MaxDBs, "max concurrent DBs for the rw9010 test")
	numKeys          = flag.Int("num-keys", readWrite9010NumKeys, "number of keys for the rw9010 test")
	duration         = flag.Duration("duration", readWrite9010Duration, "duration of each rw9010 step")
)

// initBackend builds the configured backend. For the s3 backend it also returns
// an httpMetrics handle that the rw9010 loop snapshots per step; it is nil for
// other backends, which have no instrumented HTTP client.
func initBackend() (backend.Backend, *httpMetrics, error) {
	ctx := context.Background()

	switch *backendType {
	case "memory":
		backend := memory.New()
		delays, err := memoryDelayProfile()
		if err != nil {
			return nil, nil, err
		}
		if !*enableThrottling {
			// Effectively disable throttling.
			delays.SameObjWritePs = 100000
		}
		return middleware.NewDelayBackend(backend, delays), nil, nil
	case "gcs":
		b, err := initGCS(ctx)
		return b, nil, err
	case "s3":
		return initS3(ctx)
	}

	return nil, nil, fmt.Errorf("unknown backend type %q", *backendType)
}

// memoryDelayProfile selects which simulated-latency profile the in-memory
// backend should emulate. This lets the memory backend stand in for either GCS
// or S3 ("fake backend") without touching a real bucket.
func memoryDelayProfile() (middleware.DelayOptions, error) {
	switch *memoryDelays {
	case "gcs":
		return middleware.GCSDelays, nil
	case "s3":
		return middleware.S3Delays, nil
	}
	return middleware.DelayOptions{}, fmt.Errorf("unknown delay profile %q", *memoryDelays)
}

func env(k string) (string, error) {
	if v := os.Getenv(k); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("environment variable $%v is required", k)
}

func initGCS(ctx context.Context) (backend.Backend, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %v", err)
	}
	bucket, err := env("BUCKET")
	if err != nil {
		return nil, err
	}
	return gcs.New(client.Bucket(bucket)), nil
}

func initS3(ctx context.Context) (backend.Backend, *httpMetrics, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("loading AWS config: %v", err)
	}
	bucket, err := env("BUCKET")
	if err != nil {
		return nil, nil, err
	}
	metrics := &httpMetrics{}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.HTTPClient = newInstrumentedHTTPClient(metrics)
	})
	return s3.New(client, bucket), metrics, nil
}

func initDB(b backend.Backend) *glassdb.DB {
	db, err := glassdb.Open(context.Background(), "bench", b)
	if err != nil {
		panic(err)
	}
	return db
}

type benchmarker struct {
	bench.Bench
	numKeys          int
	numWorkers       int
	numKeysPerWorker int
	stats            glassdb.Stats
}

func (b *benchmarker) Start(db *glassdb.DB, numKeys, numWorkers, numKeysPerWorker int) {
	b.numKeys = numKeys
	b.numWorkers = numWorkers
	b.numKeysPerWorker = numKeysPerWorker
	b.stats = db.Stats()
	b.Bench.Start()
}

func (b *benchmarker) End(db *glassdb.DB) {
	b.Bench.End()
	st := db.Stats()
	b.stats = st.Sub(b.stats)
}

func (b *benchmarker) Results() string {
	res := b.Bench.Results()
	txs := float64(len(res.Samples)) / float64(res.TotDuration) * float64(time.Second)
	return strings.Join([]string{
		formatMs(res.TotDuration),
		formatInt(b.numKeys),
		formatInt(b.numWorkers),
		formatInt(b.numKeysPerWorker),
		formatInt(b.stats.TxN),
		formatInt(b.stats.TxRetries),
		formatFloat(txs),
		formatMs(res.Avg()),
		formatMs(res.Percentile(0.25)),
		formatMs(res.Percentile(0.5)),
		formatMs(res.Percentile(0.9)),
		formatMs(res.Percentile(0.95)),
	}, ",")
}

func formatMs(d time.Duration) string {
	return formatFloat(float64(d) / float64(time.Millisecond))
}

func formatFloat(f float64) string {
	if f > 1000 {
		return fmt.Sprintf("%.2f", f)
	}
	return fmt.Sprintf("%.4f", f)
}

func formatInt(d int) string {
	return fmt.Sprintf("%d", d)
}

func rand1K() []byte {
	b := make([]byte, 1024)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("Cannot read from random device: %v", err))
	}
	return b
}

func independentSingleRMW(b *benchmarker, db *glassdb.DB, nwriters int) error {
	ctx := context.Background()
	eg := errgroup.Group{}

	b.Start(db, nwriters, nwriters, 1)
	defer b.End(db)

	for i := range nwriters {

		eg.Go(func() error {
			coll := db.Collection(fmt.Appendf(nil, "c%d", i))
			if err := coll.Create(ctx); err != nil {
				return err
			}

			for !b.IsTestFinished() {
				err := b.Measure(func() error {
					return db.Tx(ctx, func(tx *glassdb.Tx) error {
						v, err := tx.Read(coll, []byte("key"))
						if err != nil && !errors.Is(err, backend.ErrNotFound) {
							return err
						}
						return tx.Write(coll, []byte("key"), v)
					})
				})
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func independentMultiRMW(b *benchmarker, db *glassdb.DB, nwriters, numkeys int) error {
	ctx := context.Background()
	eg := errgroup.Group{}

	b.Start(db, numkeys*nwriters, nwriters, numkeys)
	defer b.End(db)

	for i := range nwriters {

		eg.Go(func() error {
			coll := db.Collection(fmt.Appendf(nil, "c%d", i))
			if err := coll.Create(ctx); err != nil {
				return err
			}

			keys := make([]glassdb.FQKey, numkeys)
			for j := range numkeys {
				keys[j] = glassdb.FQKey{
					Collection: coll,
					Key:        fmt.Appendf(nil, "key%d", j),
				}
			}

			for !b.IsTestFinished() {
				err := b.Measure(func() error {
					return db.Tx(ctx, func(tx *glassdb.Tx) error {
						res := tx.ReadMulti(keys)
						for i, k := range keys {
							if err := res[i].Err; err != nil && !errors.Is(err, backend.ErrNotFound) {
								return err
							}
							if err := tx.Write(k.Collection, k.Key, res[i].Value); err != nil {
								return err
							}
						}
						return nil
					})
				})
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func overlappingMultiRMW(b *benchmarker, db *glassdb.DB, nWriters, nKeysPerWriter, nOverlap int) error {
	ctx := context.Background()
	eg := errgroup.Group{}

	// Initialize all keys beforehand.
	coll := db.Collection([]byte("omrmw"))
	_ = coll.Create(ctx)

	nKeys := nWriters*nKeysPerWriter - nOverlap
	allKeys := make([]glassdb.FQKey, nKeys)
	for i := range allKeys {
		allKeys[i] = glassdb.FQKey{
			Collection: coll,
			Key:        fmt.Appendf(nil, "key%d", i),
		}
	}
	err := db.Tx(ctx, func(tx *glassdb.Tx) error {
		for i := range allKeys {
			if err := tx.Write(coll, allKeys[i].Key, rand1K()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Start the benchmark afterwards.
	b.Start(db, nKeys, nWriters, nKeysPerWriter)
	defer b.End(db)

	for i := range nWriters {
		// Keys are structured this way:
		// The overlapping keys at the beginning, then the unique
		// (per worker) keys.
		keys := make([]glassdb.FQKey, nKeysPerWriter)
		nUnique := nKeysPerWriter - nOverlap
		copy(keys[:nOverlap], allKeys[:nOverlap])
		copy(keys[nOverlap:], allKeys[nOverlap+i*nUnique:])

		eg.Go(func() error {
			for !b.IsTestFinished() {
				err := b.Measure(func() error {
					return db.Tx(ctx, func(tx *glassdb.Tx) error {
						res := tx.ReadMulti(keys)
						for i, k := range keys {
							if err := res[i].Err; err != nil {
								return err
							}
							if err := tx.Write(k.Collection, k.Key, res[i].Value); err != nil {
								return err
							}
						}
						return nil
					})
				})
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func runReadWrite9010() error {
	b, metrics, err := initBackend()
	if err != nil {
		return err
	}

	// Initialize all the keys once and for all the tests.
	log.Printf("Initialize keys")
	keys, err := initKeys(b, *numKeys)
	if err != nil {
		return err
	}
	log.Printf("End of keys initialization")

	fSamples, err := createCSV(*samplesOut, "num-db,db,tx-type,ops,latency\n")
	if err != nil {
		return err
	}
	fStats, err := createCSV(*statsOut,
		"num-db,db,num-tx,num-retries,obj-write,obj-read,meta-write,meta-read\n")
	if err != nil {
		return err
	}
	fThroughput, err := createCSV(*throughputOut,
		"num-db,db,tx-type,count,duration-ms,tx-per-sec\n")
	if err != nil {
		return err
	}
	fClient, err := createCSV(*clientStatsOut,
		"num-db,wall-ms,num-cpu,cpu-user-ms,cpu-sys-ms,cpu-util-pct,"+
			"http-requests,http-throttle,http-5xx,http-2xx,new-conns,max-goroutines\n")
	if err != nil {
		return err
	}

	// Deterministic random source. Helps with reproducibility.
	rnd := mrand.New(mrand.NewSource(42)) // #nosec

	for i := 0; i <= *maxDBs; i += 5 {
		numdb := max(1, i)
		log.Printf("Testing %d dbs", numdb)
		out := rw9010Outputs{
			samples:    fSamples,
			stats:      fStats,
			throughput: fThroughput,
			client:     fClient,
		}
		if err := runReadWrite9010Step(rnd, b, metrics, keys, numdb, out); err != nil {
			return err
		}
	}

	return nil
}

// createCSV creates (truncating) the file at path and writes the header row.
func createCSV(path, header string) (*os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.WriteString(header); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// rw9010Outputs groups the per-step CSV destinations for the rw9010 run.
type rw9010Outputs struct {
	samples    io.Writer
	stats      io.Writer
	throughput io.Writer
	client     io.Writer
}

// runReadWrite9010Step runs one concurrency step and dumps its rows. It
// snapshots client-side resource usage (CPU, HTTP traffic, connection churn)
// around the step so each row is attributed to this exact concurrency level.
func runReadWrite9010Step(
	rnd *mrand.Rand,
	b backend.Backend,
	metrics *httpMetrics,
	keys [][]byte,
	numdb int,
	out rw9010Outputs,
) error {
	var httpBefore httpSnapshot
	if metrics != nil {
		httpBefore = metrics.snapshot()
	}
	userBefore, sysBefore := processCPUTime()
	sampler := startGoroutineSampler()
	wallStart := time.Now()

	res, err := readWrite9010AllDBs(rnd, b, keys, numdb)
	if err != nil {
		return err
	}

	wall := time.Since(wallStart)
	userAfter, sysAfter := processCPUTime()
	peakGoroutines := sampler.stopAndPeak()
	var httpDelta httpSnapshot
	if metrics != nil {
		httpDelta = metrics.snapshot().sub(httpBefore)
	}

	if err := dumpSamples(out.samples, res, numdb); err != nil {
		return err
	}
	if err := dumpStats(out.stats, res, numdb); err != nil {
		return err
	}
	if err := dumpThroughput(out.throughput, res, numdb); err != nil {
		return err
	}
	return dumpClientStats(
		out.client, numdb, wall, userAfter-userBefore, sysAfter-sysBefore,
		httpDelta, peakGoroutines,
	)
}

// dumpClientStats writes one row of per-step client-side resource usage and
// logs a human-readable summary. CPU utilization is a percentage of all cores
// (100% means every core was fully busy for the whole step), which is the key
// signal for whether the throughput plateau is a client CPU bottleneck. The
// HTTP throttle count distinguishes that from backend (S3) throttling.
func dumpClientStats(
	out io.Writer,
	numdb int,
	wall, cpuUser, cpuSys time.Duration,
	http httpSnapshot,
	peakGoroutines int,
) error {
	numCPU := runtime.NumCPU()
	cpuTotal := cpuUser + cpuSys
	var utilPct, reqPerSec float64
	if wall > 0 {
		utilPct = 100 * cpuTotal.Seconds() / (wall.Seconds() * float64(numCPU))
		reqPerSec = float64(http.Requests) / wall.Seconds()
	}

	log.Printf(
		"clientmetrics num-db=%d cpu-util=%.0f%% (user=%s sys=%s wall=%s cores=%d) "+
			"http-req=%d (%.0f/s) throttle=%d 5xx=%d new-conns=%d peak-goroutines=%d",
		numdb, utilPct,
		cpuUser.Round(time.Millisecond), cpuSys.Round(time.Millisecond),
		wall.Round(time.Millisecond), numCPU,
		http.Requests, reqPerSec, http.Throttle, http.ServerErr, http.NewConns,
		peakGoroutines,
	)

	_, err := fmt.Fprintf(out, "%d,%.2f,%d,%.2f,%.2f,%.2f,%d,%d,%d,%d,%d,%d\n",
		numdb,
		float64(wall)/float64(time.Millisecond),
		numCPU,
		float64(cpuUser)/float64(time.Millisecond),
		float64(cpuSys)/float64(time.Millisecond),
		utilPct,
		http.Requests, http.Throttle, http.ServerErr, http.Success, http.NewConns,
		peakGoroutines,
	)
	return err
}

func dumpSamples(out io.Writer, results []dbResults, numdb int) error {
	for i, res := range results {
		for _, sample := range res.Bench.Read.Samples {
			err := dumpSampleRow(out, numdb, i, "strong-read", 2, sample)
			if err != nil {
				return err
			}
		}
		for _, sample := range res.Bench.WeakRead.Samples {
			err := dumpSampleRow(out, numdb, i, "weak-read", 1, sample)
			if err != nil {
				return err
			}
		}
		for _, sample := range res.Bench.Write.Samples {
			err := dumpSampleRow(out, numdb, i, "write", 2, sample)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func dumpSampleRow(out io.Writer, numdb, db int, tp string, ops int, lat time.Duration) error {
	latMs := float64(lat) / float64(time.Millisecond)
	_, err := fmt.Fprintf(out, "%d,%d,%s,%d,%.2f\n", numdb, db, tp, ops, latMs)
	return err
}

func dumpStats(out io.Writer, results []dbResults, numdb int) error {
	for i, res := range results {
		_, err := fmt.Fprintf(out, "%d,%d,%d,%d,%d,%d,%d,%d\n",
			numdb, i,
			res.Stats.TxN, res.Stats.TxRetries,
			res.Stats.ObjWrites,
			res.Stats.ObjReads,
			res.Stats.MetaWrites,
			res.Stats.MetaReads,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// dumpThroughput writes the per-DB, per-transaction-type throughput. The
// transaction count divided by the measured run duration gives the throughput
// each individual DB sustained; the plotting script aggregates these into the
// total system throughput.
func dumpThroughput(out io.Writer, results []dbResults, numdb int) error {
	for i, res := range results {
		byType := []struct {
			name string
			r    bench.Results
		}{
			{"strong-read", res.Bench.Read},
			{"weak-read", res.Bench.WeakRead},
			{"write", res.Bench.Write},
		}
		for _, t := range byType {
			count := len(t.r.Samples)
			durMs := float64(t.r.TotDuration) / float64(time.Millisecond)
			var tps float64
			if t.r.TotDuration > 0 {
				tps = float64(count) / float64(t.r.TotDuration) * float64(time.Second)
			}
			_, err := fmt.Fprintf(out, "%d,%d,%s,%d,%.2f,%.4f\n",
				numdb, i, t.name, count, durMs, tps)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readWrite9010AllDBs(rnd *mrand.Rand, b backend.Backend, keys [][]byte, numdb int) ([]dbResults, error) {
	resCh := make(chan dbResults, numdb)
	eg := errgroup.Group{}

	// Derive one seed per DB up front, from the single shared source, so each
	// DB (and below, each worker) gets its own *mrand.Rand. math/rand.Rand is
	// not safe for concurrent use, and sharing one across all DBs/workers
	// races and panics at high concurrency.
	seeds := make([]int64, numdb)
	for i := range seeds {
		seeds[i] = rnd.Int63()
	}

	for i := range numdb {
		eg.Go(func() error {
			db := initDB(b)
			defer db.Close(context.Background())

			res, err := readWrite9010(mrand.NewSource(seeds[i]), db, keys)
			if err != nil {
				return err
			}
			resCh <- dbResults{
				Stats: db.Stats(),
				Bench: res,
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(resCh)

	var res []dbResults
	for r := range resCh {
		res = append(res, r)
	}
	return res, nil
}

type dbResults struct {
	Stats glassdb.Stats
	Bench benchResults
}

type benchResults struct {
	Write    bench.Results
	Read     bench.Results
	WeakRead bench.Results
}

type txType int

const (
	txTypeWrite txType = iota
	txTypeReadStrong
	txTypeReadWeak
)

func makeTxSeries(numW, numStrongR, numWeakR int) []txType {
	res := make([]txType, numW+numStrongR+numWeakR)
	i := 0
	for ; i < numW; i++ {
		res[i] = txTypeWrite
	}
	for ; i < numW+numStrongR; i++ {
		res[i] = txTypeReadStrong
	}
	for ; i < numW+numStrongR+numWeakR; i++ {
		res[i] = txTypeReadWeak
	}
	return res
}

func readWrite9010(src mrand.Source, db *glassdb.DB, keys [][]byte) (benchResults, error) {
	eg := errgroup.Group{}

	wbench := bench.Bench{}
	wbench.SetDuration(*duration)
	srbench := bench.Bench{}
	srbench.SetDuration(*duration)
	wrbench := bench.Bench{}
	wrbench.SetDuration(*duration)

	wbench.Start()
	srbench.Start()
	wrbench.Start()

	// Each parallel worker gets its own rand seeded from the DB's source, so no
	// *mrand.Rand is shared across goroutines (see readWrite9010AllDBs).
	dbRnd := mrand.New(src)
	wseeds := make([]int64, readWrite9010NumConcurrTx)
	for i := range wseeds {
		wseeds[i] = dbRnd.Int63()
	}

	// Run the sequence of transactions 10 times in parallel.
	// Each sequence has 10 operations.
	for w := range readWrite9010NumConcurrTx {
		eg.Go(func() error {
			rnd := mrand.New(mrand.NewSource(wseeds[w]))
			// 10 transactions in series.
			// - 1 writer.
			// - 6 strong readers.
			// - 3 weak readers.
			txSeries := makeTxSeries(1, 6, 3)

			for !wbench.IsTestFinished() {
				// Every iteration has a different sequence of transactions.
				rnd.Shuffle(len(txSeries), func(i, j int) {
					txSeries[i], txSeries[j] = txSeries[j], txSeries[i]
				})
				var err error

				for _, ttype := range txSeries {
					switch ttype {
					case txTypeWrite:
						err = wbench.Measure(func() error {
							return writeTx(rnd, db, keys)
						})
					case txTypeReadStrong:
						err = srbench.Measure(func() error {
							return readTx(rnd, db, keys)
						})
					case txTypeReadWeak:
						err = wrbench.Measure(func() error {
							return weakReadTx(rnd, db, keys)
						})
					default:
						return fmt.Errorf("unknown txType: %v", ttype)
					}
				}
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	err := eg.Wait()
	wbench.End()
	srbench.End()
	wrbench.End()

	return benchResults{
		Write:    wbench.Results(),
		Read:     srbench.Results(),
		WeakRead: wrbench.Results(),
	}, err
}

func writeTx(rnd *mrand.Rand, db *glassdb.DB, keys [][]byte) error {
	ctx := context.Background()
	coll := db.Collection([]byte(readWrite9010Cname))

	return db.Tx(ctx, func(tx *glassdb.Tx) error {
		ks := []glassdb.FQKey{
			{Collection: coll, Key: keys[rnd.Intn(len(keys))]},
			{Collection: coll, Key: keys[rnd.Intn(len(keys))]},
		}
		res := tx.ReadMulti(ks)
		for _, r := range res {
			if r.Err != nil {
				return r.Err
			}
		}
		_ = tx.Write(coll, ks[0].Key, res[1].Value)
		_ = tx.Write(coll, ks[1].Key, res[0].Value)
		return nil
	})
}

func readTx(rnd *mrand.Rand, db *glassdb.DB, keys [][]byte) error {
	ctx := context.Background()
	coll := db.Collection([]byte(readWrite9010Cname))

	return db.Tx(ctx, func(tx *glassdb.Tx) error {
		ks := []glassdb.FQKey{
			{Collection: coll, Key: keys[rnd.Intn(len(keys))]},
			{Collection: coll, Key: keys[rnd.Intn(len(keys))]},
		}
		res := tx.ReadMulti(ks)
		for _, r := range res {
			if r.Err != nil {
				return r.Err
			}
		}
		return nil
	})
}

func weakReadTx(rnd *mrand.Rand, db *glassdb.DB, keys [][]byte) error {
	ctx := context.Background()
	coll := db.Collection([]byte(readWrite9010Cname))
	k := keys[rnd.Intn(len(keys))]
	_, err := coll.ReadWeak(ctx, k, 10*time.Second)
	return err
}

func initKeys(b backend.Backend, num int) ([][]byte, error) {
	if num%100 != 0 {
		return nil, fmt.Errorf("num must be multiple of 100")
	}

	ctx := context.Background()
	db := initDB(b)
	defer db.Close(ctx)

	coll := db.Collection([]byte(readWrite9010Cname))
	if err := coll.Create(ctx); err != nil {
		return nil, err
	}
	keys := make([][]byte, num)

	// Initialize in batches of 100 keys.
	for i := 0; i < num; i += 100 {
		log.Printf("keys %d - %d", i, i+100)
		err := db.Tx(ctx, func(tx *glassdb.Tx) error {
			for j := range 100 {
				k := fmt.Appendf(nil, "key%d", i+j)
				keys[i+j] = k
				if err := tx.Write(coll, k, rand1K()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Give the db some time to flush the keys.
	time.Sleep(time.Second)
	return keys, nil
}

func runTest(name string, b backend.Backend, fn func(*benchmarker, *glassdb.DB) error) error {
	ctx := context.Background()
	db := initDB(b)
	defer func() {
		db.Close(ctx)
		runtime.GC()
	}()

	ben := benchmarker{}
	if err := fn(&ben, db); err != nil {
		return err
	}
	fmt.Printf("%s,%s\n", name, ben.Results())
	return nil
}

func runSimple() error {
	back, _, err := initBackend()
	if err != nil {
		return err
	}

	fmt.Println(strings.Join([]string{
		"test name",
		"tot time",
		"num keys",
		"num workers",
		"num keys/worker",
		"num tx",
		"num retries",
		"tx/sec",
		"avg tx time",
		"25% tx time",
		"50% tx time",
		"90% tx time",
		"95% tx time",
	}, ","))

	for i := 0; i <= 100; i += 5 {
		numw := max(i, 1)
		name := fmt.Sprintf("IndepSingleRMW (%dw)", numw)
		err := runTest(name, back, func(b *benchmarker, d *glassdb.DB) error {
			return independentSingleRMW(b, d, numw)
		})
		if err != nil {
			return err
		}
	}
	for i := 0; i <= 50; i += 10 {
		numk := max(i, 2)
		for j := 0; j <= 100; j += 5 {
			numw := max(j, 1)
			name := fmt.Sprintf("IndepMultiRMW (%dw%dk)", numw, numk)
			err := runTest(name, back, func(b *benchmarker, d *glassdb.DB) error {
				return independentMultiRMW(b, d, numw, numk)
			})
			if err != nil {
				return err
			}
		}
	}
	for numw := 2; numw <= 5; numw++ {
		for i := 0; i <= 6; i += 2 {
			numkpw := max(i, 1)
			for j := 0; j <= numkpw; j += 2 {
				nover := max(j, 1)
				name := fmt.Sprintf("OverlapMultiRMW (%dw%dkpw%dko)", numw, numkpw, nover)
				err := runTest(name, back, func(b *benchmarker, d *glassdb.DB) error {
					return overlappingMultiRMW(b, d, numw, numkpw, nover)
				})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// runDeadlock reproduces the deadlock-latency scenario: deadlockNumWriters
// workers contend on the same set of keys (1 to 6) with up to 100% overlap.
// It dumps the raw per-transaction latency samples so the plotting script can
// compute percentiles and show the tail behavior under contention.
func runDeadlock() error {
	back, _, err := initBackend()
	if err != nil {
		return err
	}

	f, err := os.Create(*deadlockOut)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString("num-keys,overlap,overlap-pct,latency-ms\n"); err != nil {
		return err
	}

	for k := 1; k <= 6; k++ {
		for overlap := 1; overlap <= k; overlap++ {
			ctx := context.Background()
			db := initDB(back)

			ben := benchmarker{}
			ben.SetDuration(*duration)
			err := overlappingMultiRMW(&ben, db, deadlockNumWriters, k, overlap)
			db.Close(ctx)
			runtime.GC()
			if err != nil {
				return err
			}

			res := ben.Bench.Results()
			overlapPct := 100 * overlap / k
			for _, s := range res.Samples {
				latMs := float64(s) / float64(time.Millisecond)
				if _, err := fmt.Fprintf(f, "%d,%d,%d,%.2f\n", k, overlap, overlapPct, latMs); err != nil {
					return err
				}
			}
			log.Printf("deadlock: keys=%d overlap=%d (%d%%) samples=%d p50=%v p90=%v",
				k, overlap, overlapPct, len(res.Samples),
				res.Percentile(0.5), res.Percentile(0.9))
		}
	}

	return nil
}

func do() error {
	switch *testName {
	case "simple":
		return runSimple()
	case "rw9010":
		return runReadWrite9010()
	case "deadlock":
		return runDeadlock()
	}
	return fmt.Errorf("unknown test name %q", *testName)
}

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
