package testkit

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
)

// NewLogger creates a new logger that writes to the testing.TB.
func NewLogger(tb testing.TB, opts *slog.HandlerOptions) *slog.Logger {
	w := &testLogWriter{
		t:   tb,
		buf: make([]byte, 0, 1024),
	}
	tb.Cleanup(w.Close)
	return slog.New(slog.NewJSONHandler(w, opts))
}

// maxBufferedRecords bounds the memory used by NewFailureLogger. When exceeded,
// the oldest records are dropped (the count is reported on flush).
const maxBufferedRecords = 20000

// NewFailureLogger returns a logger that buffers records in memory and only
// writes them to tb (via tb.Log) if the test fails. Formatting is deferred
// until failure, so passing tests pay only for cloning records.
func NewFailureLogger(tb testing.TB, opts *slog.HandlerOptions) *slog.Logger {
	r := &failureRecorder{tb: tb, opts: opts}
	tb.Cleanup(r.flush)
	return slog.New(&bufferHandler{rec: r, derive: identityHandler})
}

func identityHandler(h slog.Handler) slog.Handler { return h }

type bufferedEntry struct {
	rec    slog.Record
	derive func(slog.Handler) slog.Handler
}

type failureRecorder struct {
	tb   testing.TB
	opts *slog.HandlerOptions

	mu      sync.Mutex
	entries []bufferedEntry
	dropped int
}

func (r *failureRecorder) add(e bufferedEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) >= maxBufferedRecords {
		r.entries = r.entries[1:]
		r.dropped++
	}
	r.entries = append(r.entries, e)
}

func (r *failureRecorder) flush() {
	if !r.tb.Failed() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	w := &testLogWriter{t: r.tb, buf: make([]byte, 0, 1024)}
	defer w.Close()
	if r.dropped > 0 {
		r.tb.Logf("(%d earlier debug records dropped)", r.dropped)
	}
	base := slog.NewJSONHandler(w, r.opts)
	for _, e := range r.entries {
		_ = e.derive(base).Handle(context.Background(), e.rec)
	}
}

// bufferHandler is an slog.Handler that defers formatting by cloning records
// into a failureRecorder. WithAttrs/WithGroup accumulate into the derive
// closure so the original attr/group nesting is faithfully reproduced when the
// buffered records are replayed at flush time.
type bufferHandler struct {
	rec    *failureRecorder
	derive func(slog.Handler) slog.Handler
}

func (h *bufferHandler) Enabled(_ context.Context, l slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.rec.opts != nil && h.rec.opts.Level != nil {
		minLevel = h.rec.opts.Level.Level()
	}
	return l >= minLevel
}

func (h *bufferHandler) Handle(_ context.Context, r slog.Record) error {
	h.rec.add(bufferedEntry{rec: r.Clone(), derive: h.derive})
	return nil
}

func (h *bufferHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	parent := h.derive
	return &bufferHandler{
		rec:    h.rec,
		derive: func(b slog.Handler) slog.Handler { return parent(b).WithAttrs(attrs) },
	}
}

func (h *bufferHandler) WithGroup(name string) slog.Handler {
	parent := h.derive
	return &bufferHandler{
		rec:    h.rec,
		derive: func(b slog.Handler) slog.Handler { return parent(b).WithGroup(name) },
	}
}

type testLogWriter struct {
	t   testing.TB
	buf []byte
}

func (w *testLogWriter) Write(p []byte) (n int, err error) {
	i := bytes.IndexByte(p, '\n')
	if i == -1 {
		w.buf = append(w.buf, p...)
		return len(p), nil
	}
	w.buf = append(w.buf, p[:i+1]...)
	w.sync(len(w.buf))
	w.buf = append(w.buf, p[i+1:]...)
	return len(p), nil
}

func (w *testLogWriter) Close() {
	w.sync(len(w.buf))
}

func (w *testLogWriter) sync(n int) {
	if n == 0 {
		return
	}
	w.t.Log(string(w.buf[:n]))
	w.buf = w.buf[n:]
}
