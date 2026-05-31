package main

import (
	"net/http"
	"net/http/httptrace"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// httpMetrics accumulates HTTP-level activity of the shared S3 client. Counting
// happens at the round-trip layer, so retries (each a fresh attempt) and
// throttling responses are visible here even when the SDK retryer absorbs them.
// This is what lets us tell a client-side ceiling (CPU, connections) apart from
// backend throttling.
type httpMetrics struct {
	requests  atomic.Int64 // total HTTP attempts, including retries
	throttle  atomic.Int64 // 503 SlowDown / 429 throttling responses
	serverErr atomic.Int64 // other 5xx responses
	success   atomic.Int64 // 2xx responses
	newConns  atomic.Int64 // connections opened (non-reused), i.e. TLS handshakes
}

// httpSnapshot is a point-in-time copy of httpMetrics, used to compute per-step
// deltas.
type httpSnapshot struct {
	Requests, Throttle, ServerErr, Success, NewConns int64
}

func (m *httpMetrics) snapshot() httpSnapshot {
	return httpSnapshot{
		Requests:  m.requests.Load(),
		Throttle:  m.throttle.Load(),
		ServerErr: m.serverErr.Load(),
		Success:   m.success.Load(),
		NewConns:  m.newConns.Load(),
	}
}

func (s httpSnapshot) sub(o httpSnapshot) httpSnapshot {
	return httpSnapshot{
		Requests:  s.Requests - o.Requests,
		Throttle:  s.Throttle - o.Throttle,
		ServerErr: s.ServerErr - o.ServerErr,
		Success:   s.Success - o.Success,
		NewConns:  s.NewConns - o.NewConns,
	}
}

// countingHTTPClient wraps an aws.HTTPClient and records every request, its
// response status, and whether it had to open a new connection.
type countingHTTPClient struct {
	inner aws.HTTPClient
	m     *httpMetrics
}

func (c countingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.m.requests.Add(1)

	// httptrace counts connection reuse at the transport level, independent of
	// the SDK's transport configuration: a non-reused connection means a fresh
	// dial + TLS handshake, which is the connection-churn signal we care about.
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if !info.Reused {
				c.m.newConns.Add(1)
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := c.inner.Do(req)
	if resp != nil {
		switch {
		case resp.StatusCode == http.StatusServiceUnavailable,
			resp.StatusCode == http.StatusTooManyRequests:
			c.m.throttle.Add(1)
		case resp.StatusCode >= 500:
			c.m.serverErr.Add(1)
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			c.m.success.Add(1)
		}
	}
	return resp, err
}

// newInstrumentedHTTPClient returns the SDK's default HTTP client wrapped to
// record HTTP metrics. The SDK transport (timeouts, connection pool sizing) is
// preserved, so the measured behavior matches an uninstrumented run.
func newInstrumentedHTTPClient(m *httpMetrics) aws.HTTPClient {
	return countingHTTPClient{inner: awshttp.NewBuildableClient(), m: m}
}

// goroutineSampler tracks the peak number of goroutines over a measurement
// window by polling runtime.NumGoroutine. A high peak relative to the offered
// concurrency hints at goroutines piling up behind a shared bottleneck.
type goroutineSampler struct {
	quit chan struct{}
	peak chan int
}

func startGoroutineSampler() *goroutineSampler {
	s := &goroutineSampler{quit: make(chan struct{}), peak: make(chan int, 1)}
	go func() {
		peak := runtime.NumGoroutine()
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-s.quit:
				s.peak <- peak
				return
			case <-t.C:
				if n := runtime.NumGoroutine(); n > peak {
					peak = n
				}
			}
		}
	}()
	return s
}

func (s *goroutineSampler) stopAndPeak() int {
	close(s.quit)
	return <-s.peak
}
