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

package testkit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func NewGCSClient(ctx context.Context, t testing.TB, logging bool) *storage.Client {
	t.Helper()

	srv := newFakeGCSServer()
	t.Cleanup(srv.Close)

	hclient := srv.Client()
	if logging {
		hclient.Transport = logTransport{hclient.Transport}
	}

	client, err := storage.NewClient(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(srv.URL),
		option.WithHTTPClient(hclient),
	)
	if err != nil {
		t.Fatalf("creating fake client: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
	})

	return client
}

type logTransport struct {
	inner http.RoundTripper
}

func (l logTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	log1 := l.logRequest(req)
	resp, err := l.inner.RoundTrip(req)
	log2 := l.logResponse(resp)
	fmt.Printf("- request: %s\n  response: %s\n", log1, log2)
	return resp, err
}

func (l logTransport) logRequest(req *http.Request) string {
	var buf bytes.Buffer

	// Log headers and form.
	_ = req.ParseForm()
	fmt.Fprintf(&buf, "method=%s path=%s headers=%v form=%v",
		req.Method, req.URL.Path, req.Header, req.Form)

	// Log body.
	if req.Body != nil {
		defer req.Body.Close()
		buf.WriteString(" body=")
		req.Body = copyAndRewind(req.Body, &buf)
	}

	return buf.String()
}

func (l logTransport) logResponse(r *http.Response) string {
	if r == nil {
		return "nil"
	}

	var buf bytes.Buffer

	fmt.Fprintf(&buf, "status=%s headers=%v", r.Status, r.Header)

	if r.Body != nil {
		defer r.Body.Close()
		buf.WriteString(" body=")
		r.Body = copyAndRewind(r.Body, &buf)
	}

	return buf.String()
}

func copyAndRewind(r io.Reader, w io.Writer) io.ReadCloser {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	_, _ = w.Write(buf.Bytes())
	return dummyCloser{&buf, err}
}

type dummyCloser struct {
	*bytes.Buffer
	err error
}

func (d dummyCloser) Close() error {
	return d.err
}
