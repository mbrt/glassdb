package testkit

import (
	"bytes"
	"log/slog"
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
