package glassdb

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/mbrt/glassdb/backend"
)

const (
	dbVersion  = "v0"
	dbMetaPath = "glassdb"

	dbVersionTag = "version"
)

func checkOrCreateDBMeta(ctx context.Context, b backend.Backend, name string) error {
	err := checkDBVersion(ctx, b, name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, backend.ErrNotFound) {
		return err
	}
	err = setDBMetadata(ctx, b, name, dbVersion)
	if err == nil {
		return nil
	}
	if !errors.Is(err, backend.ErrPrecondition) {
		return fmt.Errorf("creating db metadata: %w", err)
	}
	// We raced against another instance creating the metadata.
	// Check it again.
	return checkDBVersion(ctx, b, name)
}

func checkDBVersion(ctx context.Context, b backend.Backend, name string) error {
	p := path.Join(name, dbMetaPath)
	meta, err := b.GetMetadata(ctx, p)
	if err != nil {
		return err
	}
	if v := meta.Tags[dbVersionTag]; v != dbVersion {
		return fmt.Errorf("got db version %q, expected %q", v, dbVersion)
	}
	return nil
}

func setDBMetadata(ctx context.Context, b backend.Backend, name, version string) error {
	p := path.Join(name, dbMetaPath)
	_, err := b.WriteIfNotExists(ctx, p, nil, backend.Tags{
		dbVersionTag: version,
	})
	return err
}
