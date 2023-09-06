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
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/internal/testkit/bench"
)

const (
	readWrite9010Cname        = "read-write-9010"
	readWrite9010Duration     = time.Minute
	readWrite9010MaxDBs       = 50
	readWrite9010NumConcurrTx = 10
	readWrite9010NumKeys      = 50000
)

var (
	backendType      = flag.String("backend", "memory", "select backend type [memory|gcs]")
	enableThrottling = flag.Bool("enable-throttling", true, "enable throttling with memory backend")
	testName         = flag.String("test-name", "simple", "which test to run [simple|rw9010]")
	samplesOut       = flag.String("samples-out", "samples.csv", "output file with raw samples data")
	statsOut         = flag.String("stats-out", "stats.csv", "output file with db stats")
)

func initBackend() (backend.Backend, error) {
	ctx := context.Background()

	switch *backendType {
	case "memory":
		backend := memory.New()
		delays := middleware.GCSDelays
		if !*enableThrottling {
			// Effectively disable throttling.
			delays.SameObjWritePs = 100000
		}
		return middleware.NewDelayBackend(backend, clockwork.NewRealClock(), delays), nil
	case "gcs":
		return initGCS(ctx)
	}

	return nil, fmt.Errorf("unknown backend type %q", *backendType)
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

	for i := 0; i < nwriters; i++ {
		i := i

		eg.Go(func() error {
			coll := db.Collection([]byte(fmt.Sprintf("c%d", i)))
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

	for i := 0; i < nwriters; i++ {
		i := i

		eg.Go(func() error {
			coll := db.Collection([]byte(fmt.Sprintf("c%d", i)))
			if err := coll.Create(ctx); err != nil {
				return err
			}

			keys := make([]glassdb.FQKey, numkeys)
			for j := 0; j < numkeys; j++ {
				keys[j] = glassdb.FQKey{
					Collection: coll,
					Key:        []byte(fmt.Sprintf("key%d", j)),
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
	for i := 0; i < len(allKeys); i++ {
		allKeys[i] = glassdb.FQKey{
			Collection: coll,
			Key:        []byte(fmt.Sprintf("key%d", i)),
		}
	}
	err := db.Tx(ctx, func(tx *glassdb.Tx) error {
		for i := 0; i < len(allKeys); i++ {
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

	for i := 0; i < nWriters; i++ {
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
	b, err := initBackend()
	if err != nil {
		return err
	}

	// Initialize all the keys once and for all the tests.
	log.Printf("Initialize keys")
	keys, err := initKeys(b, readWrite9010NumKeys)
	if err != nil {
		return err
	}
	log.Printf("End of keys initialization")

	fSamples, err := os.Create(*samplesOut)
	if err != nil {
		return err
	}
	_, err = fSamples.WriteString("num-db,db,tx-type,ops,latency\n")
	if err != nil {
		return err
	}

	fStats, err := os.Create(*statsOut)
	if err != nil {
		return err
	}
	_, err = fStats.WriteString("num-db,db,num-tx,num-retries,obj-write,obj-read,meta-write,meta-read\n")
	if err != nil {
		return err
	}

	// Deterministic random source. Helps with reproducibility.
	rnd := mrand.New(mrand.NewSource(42)) // #nosec

	for i := 0; i <= readWrite9010MaxDBs; i += 5 {
		numdb := max(1, i)
		log.Printf("Testing %d dbs", numdb)
		res, err := readWrite9010AllDBs(rnd, b, keys, numdb)
		if err != nil {
			return err
		}
		if err := dumpSamples(fSamples, res, numdb); err != nil {
			return err
		}
		if err := dumpStats(fStats, res, numdb); err != nil {
			return err
		}
	}

	return nil
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
			res.Stats.ObjWriteN,
			res.Stats.ObjReadN,
			res.Stats.MetaWriteN,
			res.Stats.MetaReadN,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func readWrite9010AllDBs(rnd *mrand.Rand, b backend.Backend, keys [][]byte, numdb int) ([]dbResults, error) {
	resCh := make(chan dbResults, numdb)
	eg := errgroup.Group{}

	for i := 0; i < numdb; i++ {
		eg.Go(func() error {
			db := initDB(b)
			defer db.Close(context.Background())

			res, err := readWrite9010(rnd, db, keys)
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

func readWrite9010(rnd *mrand.Rand, db *glassdb.DB, keys [][]byte) (benchResults, error) {
	eg := errgroup.Group{}

	wbench := bench.Bench{}
	wbench.SetDuration(readWrite9010Duration)
	srbench := bench.Bench{}
	srbench.SetDuration(readWrite9010Duration)
	wrbench := bench.Bench{}
	wrbench.SetDuration(readWrite9010Duration)

	wbench.Start()
	srbench.Start()
	wrbench.Start()

	// Run the sequence of transactions 10 times in parallel.
	// Each sequence has 10 operations.
	for i := 0; i < readWrite9010NumConcurrTx; i++ {
		eg.Go(func() error {
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
			for j := 0; j < 100; j++ {
				k := []byte(fmt.Sprintf("key%d", i+j))
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
	back, err := initBackend()
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

func do() error {
	switch *testName {
	case "simple":
		return runSimple()
	case "rw9010":
		return runReadWrite9010()
	}
	return nil
}

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
