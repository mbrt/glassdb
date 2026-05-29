// Demo demonstrates basic glassdb functionality against a GCS backend.
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
	"github.com/mbrt/glassdb/backend/s3"
)

func env(k string) (string, error) {
	if v := os.Getenv(k); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("environment variable $%v is required", k)
}

// initBackend selects the storage backend based on the BACKEND environment
// variable ("gcs" by default, or "s3"). Both read the bucket from $BUCKET.
func initBackend(ctx context.Context) (backend.Backend, error) {
	bucket, err := env("BUCKET")
	if err != nil {
		return nil, err
	}
	switch os.Getenv("BACKEND") {
	case "", "gcs":
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating GCS client: %w", err)
		}
		return gcs.New(client.Bucket(bucket)), nil
	case "s3":
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading AWS config: %w", err)
		}
		return s3.New(awss3.NewFromConfig(cfg), bucket), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", os.Getenv("BACKEND"))
	}
}

func do() error {
	ctx := context.Background()
	b, err := initBackend(ctx)
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
