package glassdb

import (
	"context"
	"testing"

	"github.com/mbrt/glassdb/backend/memory"
	"github.com/stretchr/testify/assert"
)

func TestCreateDBMeta(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	err := setDBMetadata(ctx, b, "test", dbVersion)
	assert.NoError(t, err)

	err = checkDBVersion(ctx, b, "test")
	assert.NoError(t, err)
}

func TestCheckWrongDBMeta(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	err := setDBMetadata(ctx, b, "test", "v300")
	assert.NoError(t, err)

	err = checkDBVersion(ctx, b, "test")
	assert.ErrorContains(t, err, "db version")
}
