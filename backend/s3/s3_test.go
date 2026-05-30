package s3_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mbrt/glassdb/backend"
	s3backend "github.com/mbrt/glassdb/backend/s3"
	"github.com/mbrt/glassdb/internal/testkit"
)

func newBackend(t *testing.T) s3backend.Backend {
	t.Helper()
	ctx := context.Background()
	client := testkit.NewS3Client(ctx, t, false)
	return s3backend.New(client, testkit.S3TestBucket)
}

func TestReadStripsNonce(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	tests := []struct {
		name  string
		value []byte
	}{
		{"non-empty", []byte("hello world")},
		{"empty", nil},
		{"binary", []byte{0x00, 0x01, 0x02, 0xff}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := b.Write(ctx, tc.name, tc.value, backend.Tags{"key": "val"})
			require.NoError(t, err)
			assert.False(t, meta.Version.IsNull())

			r, err := b.Read(ctx, tc.name)
			require.NoError(t, err)
			// bytes.Equal treats nil and empty slices as equal, which is the
			// only difference for the empty case (io.ReadAll yields []byte{}).
			assert.True(t, bytes.Equal(tc.value, r.Contents),
				"got %v, want %v", r.Contents, tc.value)
			assert.Equal(t, "val", r.Tags["key"])
		})
	}
}

func TestWriteProducesFreshVersionEachTime(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	// Re-uploading identical bytes must still change the version, because the
	// nonce forces a fresh ETag. Without it, CAS would break.
	m1, err := b.Write(ctx, "k", []byte("same"), nil)
	require.NoError(t, err)
	m2, err := b.Write(ctx, "k", []byte("same"), nil)
	require.NoError(t, err)
	assert.NotEqual(t, m1.Version, m2.Version)
}

func TestSetTagsIfMergesAndCAS(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	writer := backend.WriterID("tx-1")
	m0, err := b.Write(ctx, "k", []byte("value"), backend.Tags{
		backend.LastWriterTag: backend.EncodeWriterTag(writer),
		"lock-type":           "-",
	})
	require.NoError(t, err)

	// SetTagsIf with the current version succeeds, changes the token, and
	// preserves the last-writer tag while overlaying the new lock tags.
	m1, err := b.SetTagsIf(ctx, "k", m0.Version, backend.Tags{
		"lock-type": "w",
		"locked-by": "tx2",
	})
	require.NoError(t, err)
	assert.NotEqual(t, m0.Version, m1.Version)
	assert.Equal(t, backend.EncodeWriterTag(writer), m1.Tags[backend.LastWriterTag])
	assert.Equal(t, "w", m1.Tags["lock-type"])
	assert.Equal(t, "tx2", m1.Tags["locked-by"])

	// The underlying value is untouched by a tag update.
	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), r.Contents)

	// Using the now-stale version fails the precondition.
	_, err = b.SetTagsIf(ctx, "k", m0.Version, backend.Tags{"lock-type": "r"})
	assert.ErrorIs(t, err, backend.ErrPrecondition)
}

func TestSetTagsIfNotFound(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	_, err := b.SetTagsIf(ctx, "missing", backend.Version{Token: `"x"`}, backend.Tags{"lock-type": "r"})
	assert.ErrorIs(t, err, backend.ErrNotFound)
}

func TestWriteIfNotExists(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	_, err := b.WriteIfNotExists(ctx, "k", []byte("a"), nil)
	require.NoError(t, err)

	_, err = b.WriteIfNotExists(ctx, "k", []byte("b"), nil)
	assert.ErrorIs(t, err, backend.ErrPrecondition)

	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), r.Contents)
}

func TestWriteIfCAS(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	m0, err := b.Write(ctx, "k", []byte("a"), nil)
	require.NoError(t, err)

	// Stale version fails.
	_, err = b.WriteIf(ctx, "k", []byte("b"), backend.Version{Token: `"stale"`}, nil)
	assert.ErrorIs(t, err, backend.ErrPrecondition)

	// Current version succeeds.
	m1, err := b.WriteIf(ctx, "k", []byte("b"), m0.Version, nil)
	require.NoError(t, err)
	assert.NotEqual(t, m0.Version, m1.Version)

	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), r.Contents)
}

func TestWriteIfNullVersionFailsPrecondition(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	m0, err := b.Write(ctx, "k", []byte("a"), nil)
	require.NoError(t, err)

	// A null expected version has an empty token, which can never match a
	// stored ETag. It must fail the precondition rather than overwrite the
	// object unconditionally.
	_, err = b.WriteIf(ctx, "k", []byte("b"), backend.Version{}, nil)
	assert.ErrorIs(t, err, backend.ErrPrecondition)

	// The original value and version are left untouched.
	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), r.Contents)
	assert.Equal(t, m0.Version, r.Version)

	// SetTagsIf with a null version on an existing object behaves the same.
	_, err = b.SetTagsIf(ctx, "k", backend.Version{}, backend.Tags{"lock-type": "r"})
	assert.ErrorIs(t, err, backend.ErrPrecondition)
}

func TestReadIfModified(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	writer := backend.WriterID("w1")
	_, err := b.Write(ctx, "k", []byte("x"), backend.Tags{
		backend.LastWriterTag: backend.EncodeWriterTag(writer),
	})
	require.NoError(t, err)

	// Same writer: unchanged.
	_, err = b.ReadIfModified(ctx, "k", writer)
	assert.ErrorIs(t, err, backend.ErrPrecondition)

	// Different writer: returns the value.
	r, err := b.ReadIfModified(ctx, "k", backend.WriterID("other"))
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), r.Contents)
}

func TestDeleteIf(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	m0, err := b.Write(ctx, "k", []byte("x"), nil)
	require.NoError(t, err)

	// Wrong token: precondition failure, object stays.
	err = b.DeleteIf(ctx, "k", backend.Version{Token: `"wrong"`})
	assert.ErrorIs(t, err, backend.ErrPrecondition)
	_, err = b.Read(ctx, "k")
	require.NoError(t, err)

	// Correct token: deleted.
	err = b.DeleteIf(ctx, "k", m0.Version)
	require.NoError(t, err)
	_, err = b.Read(ctx, "k")
	assert.ErrorIs(t, err, backend.ErrNotFound)
}

func TestReadAndMetadataNotFound(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	_, err := b.Read(ctx, "missing")
	assert.ErrorIs(t, err, backend.ErrNotFound)

	_, err = b.GetMetadata(ctx, "missing")
	assert.ErrorIs(t, err, backend.ErrNotFound)
}

func TestList(t *testing.T) {
	ctx := context.Background()
	b := newBackend(t)

	objs := []string{"d/a/1", "d/a/2", "d/a/b/1", "d/c/1", "d/root"}
	for _, name := range objs {
		_, err := b.Write(ctx, name, []byte(name), nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		dir      string
		expected []string
	}{
		{"top", "d", []string{"d/a/", "d/c/", "d/root"}},
		{"subdir", "d/a", []string{"d/a/1", "d/a/2", "d/a/b/"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iter, err := b.List(ctx, tc.dir)
			require.NoError(t, err)
			var got []string
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				got = append(got, p)
			}
			require.NoError(t, iter.Err())
			assert.Equal(t, tc.expected, got)
		})
	}
}

// flakyBackend builds a Backend whose HTTP transport returns 503 SlowDown for
// the first n requests matching match, then delegates. It returns the
// transport so tests can assert how many injected failures were consumed.
func flakyBackend(
	ctx context.Context,
	t *testing.T,
	n int,
	match func(*http.Request) bool,
	opts ...s3backend.Option,
) (s3backend.Backend, *testkit.SlowDownTransport) {
	t.Helper()
	var flaky *testkit.SlowDownTransport
	client := testkit.NewS3ClientWithTransport(ctx, t, func(inner http.RoundTripper) http.RoundTripper {
		flaky = testkit.NewSlowDownTransport(inner, n, match)
		return flaky
	})
	return s3backend.New(client, testkit.S3TestBucket, opts...), flaky
}

func methodIs(m string) func(*http.Request) bool {
	return func(r *http.Request) bool { return r.Method == m }
}

// fastRetryer retries the same errors as the default (incl. 503 SlowDown) but
// with negligible backoff, keeping tests quick and deterministic.
func fastRetryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 5
		o.MaxBackoff = time.Millisecond
	})
}

func TestWriteRetriesThroughSlowDown(t *testing.T) {
	ctx := context.Background()
	b, flaky := flakyBackend(ctx, t, 2, methodIs(http.MethodPut),
		s3backend.WithRetryer(fastRetryer))

	// The first two PUTs are throttled, but the retryer rides it out.
	_, err := b.Write(ctx, "k", []byte("v"), nil)
	require.NoError(t, err)
	assert.Equal(t, 0, flaky.Remaining())

	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), r.Contents)
}

func TestReadRetriesThroughSlowDown(t *testing.T) {
	ctx := context.Background()
	b, flaky := flakyBackend(ctx, t, 2, methodIs(http.MethodGet),
		s3backend.WithRetryer(fastRetryer))

	// The write is a PUT, so it is not throttled here.
	_, err := b.Write(ctx, "k", []byte("v"), nil)
	require.NoError(t, err)

	// The first two GETs are throttled; the retryer rides it out.
	r, err := b.Read(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), r.Contents)
	assert.Equal(t, 0, flaky.Remaining())
}

func TestDefaultRetryerRidesOutSlowDown(t *testing.T) {
	ctx := context.Background()
	// No WithRetryer: exercises the adaptive default that New installs. A single
	// injected failure is enough to confirm the default retries (and keeps the
	// adaptive backoff cost low).
	b, flaky := flakyBackend(ctx, t, 1, methodIs(http.MethodPut))

	_, err := b.Write(ctx, "k", []byte("v"), nil)
	require.NoError(t, err)
	assert.Equal(t, 0, flaky.Remaining())
}

func TestNopRetryerSurfacesSlowDown(t *testing.T) {
	ctx := context.Background()
	b, _ := flakyBackend(ctx, t, 1, methodIs(http.MethodPut),
		s3backend.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }))

	// With retries disabled, a single throttle surfaces to the caller.
	_, err := b.Write(ctx, "k", []byte("v"), nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "SlowDown")
}
