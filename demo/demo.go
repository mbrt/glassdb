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
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
)

func env(k string) (string, error) {
	if v := os.Getenv(k); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("environment variable $%v is required", k)
}

func initStorage(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	return client, nil
}

func initBackend(client *storage.Client) (backend.Backend, error) {
	bucket, err := env("BUCKET")
	if err != nil {
		return nil, err
	}
	return gcs.New(client.Bucket(bucket)), nil
}

func do() error {
	ctx := context.Background()
	client, err := initStorage(ctx)
	if err != nil {
		return err
	}
	b, err := initBackend(client)
	if err != nil {
		return err
	}
	db, err := glassdb.Open(ctx, "example", b)
	if err != nil {
		return fmt.Errorf("opening db: %w", err)
	}
	defer db.Close(ctx)

	key := []byte("key1")
	val := []byte("value1")

	coll := db.Collection([]byte("demo-coll"))
	if err := coll.Create(ctx); err != nil {
		return err
	}
	if err := coll.Write(ctx, key, val); err != nil {
		return err
	}
	buf, err := coll.ReadStrong(ctx, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(buf, val) {
		return fmt.Errorf("read(%q) = %q, expected %q", string(key), string(buf), string(val))
	}
	return nil
}

func main() {
	if err := do(); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
