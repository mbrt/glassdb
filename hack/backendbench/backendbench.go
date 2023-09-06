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
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jonboulle/clockwork"

	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/backend/memory"
	"github.com/mbrt/glassdb/backend/middleware"
	"github.com/mbrt/glassdb/internal/testkit/bench"
)

const (
	testRoot     = "backend-bench"
	testDuration = 20 * time.Second
)

var backendType = flag.String("backend", "memory", "select backend type [memory|gcs]")

func initBackend() (backend.Backend, error) {
	ctx := context.Background()

	switch *backendType {
	case "memory":
		backend := memory.New()
		clock := clockwork.NewRealClock()
		return middleware.NewDelayBackend(backend, clock, middleware.GCSDelays), nil
	case "gcs":
		return initGCSBackend(ctx)
	}

	return nil, fmt.Errorf("unknown backend type %q", *backendType)
}

func initGCSBackend(ctx context.Context) (backend.Backend, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	bucket, err := env("BUCKET")
	if err != nil {
		return nil, err
	}
	return gcs.New(client.Bucket(bucket)), nil
}

func env(k string) (string, error) {
	if v := os.Getenv(k); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("environment variable $%v is required", k)
}

func runBench(name string, b backend.Backend, fn func(backend.Backend, *bench.Bench) error) error {
	var ben bench.Bench
	ben.SetDuration(testDuration)
	ben.Start()
	if err := fn(b, &ben); err != nil {
		return err
	}
	ben.End()
	res := ben.Results()

	var ts time.Duration
	for _, x := range res.Samples {
		fmt.Printf("%s,%s,%s\n", name, formatMs(ts), formatMs(x))
		ts += x
	}

	log.Printf("%s: 50pc: %v, 90pc: %v, 95pc: %v", name,
		res.Percentile(0.5), res.Percentile(0.9), res.Percentile(0.95))

	return nil
}

func benchWriteSame(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)
	p := path.Join(testRoot, "write-same")
	count := 0

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			_, err := b.Write(ctx, p, data, backend.Tags{
				"key": fmt.Sprintf("val%d", count),
			})
			return err
		})
		if err != nil {
			return err
		}
		count++
	}

	return nil
}

func benchWriteFailPre(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)
	p := path.Join(testRoot, "write-same")
	meta, err := b.Write(ctx, p, data, backend.Tags{"key": "val"})
	if err != nil {
		return err
	}
	count := 0

	expected := meta.Version
	expected.Meta++

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			_, _ = b.WriteIf(ctx, p, data, expected, backend.Tags{
				"key": fmt.Sprintf("val%d", count),
			})
			return nil
		})
		if err != nil {
			return err
		}
		count++
	}

	return nil
}

func benchRead(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)

	p := path.Join(testRoot, "read")
	_, err := b.Write(ctx, p, data, backend.Tags{"key": "val"})
	if err != nil {
		return err
	}

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			_, err := b.Read(ctx, p)
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func benchReadUnchanged(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)

	p := path.Join(testRoot, "read")
	meta, err := b.Write(ctx, p, data, backend.Tags{"key": "val"})
	if err != nil {
		return err
	}

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			_, err := b.ReadIfModified(ctx, p, meta.Version.Contents)
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func benchSetMetaSame(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)
	p := path.Join(testRoot, "set-meta")

	meta, err := b.Write(ctx, p, data, backend.Tags{"key": "val"})
	if err != nil {
		return err
	}

	count := 0

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			var err error
			meta, err = b.SetTagsIf(ctx, p, meta.Version, backend.Tags{
				"key": fmt.Sprintf("val%d", count),
			})
			return err
		})
		if err != nil {
			return err
		}
		count++
	}

	return nil
}

func benchGetMeta(b backend.Backend, ben *bench.Bench) error {
	ctx := context.Background()
	data := randomData(1024)

	p := path.Join(testRoot, "get-meta")
	_, err := b.Write(ctx, p, data, backend.Tags{"key": "val"})
	if err != nil {
		return err
	}

	for !ben.IsTestFinished() {
		err := ben.Measure(func() error {
			_, err := b.GetMetadata(ctx, p)
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func randomData(size int) []byte {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("Cannot read from random device: %v", err))
	}
	return b
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

func do() error {
	b, err := initBackend()
	if err != nil {
		return err
	}
	tests := []struct {
		name string
		fn   func(backend.Backend, *bench.Bench) error
	}{
		{
			name: "WriteSame",
			fn:   benchWriteSame,
		},
		{
			name: "WriteFailPre",
			fn:   benchWriteFailPre,
		},
		{
			name: "Read",
			fn:   benchRead,
		},
		{
			name: "ReadUnchanged",
			fn:   benchReadUnchanged,
		},
		{
			name: "SetMetaSame",
			fn:   benchSetMetaSame,
		},
		{
			name: "GetMeta",
			fn:   benchGetMeta,
		},
	}

	for _, test := range tests {
		if err := runBench(test.name, b, test.fn); err != nil {
			return err
		}
	}
	return nil

}

func main() {
	if err := do(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
