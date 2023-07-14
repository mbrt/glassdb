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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"

	"github.com/gorilla/mux"
)

var errNotImplemented = withStatus(
	errors.New("not implemented"), http.StatusNotImplemented)

func newFakeGCSServer() *httptest.Server {
	srv := &fakeGCSServer{gcs: newFakeGCS()}
	mux := srv.buildMux()
	return httptest.NewServer(mux)
}

type fakeGCSServer struct {
	gcs *gcs
}

func (s *fakeGCSServer) buildMux() *mux.Router {
	jsonHandlers := []struct {
		path    string
		methods []string
		handler jsonHandler
	}{
		{
			path:    "/b",
			methods: []string{http.MethodPost},
			handler: s.createBucket,
		},
		{
			path:    "/b/{bucketName}/o",
			methods: []string{http.MethodGet},
			handler: s.listObjects,
		},
		{
			path:    "/upload/storage/v1/b/{bucketName}/o",
			methods: []string{http.MethodPost},
			handler: s.uploadObject,
		},
		{
			path:    "/b/{bucketName}/o/{objectName:.+}",
			methods: []string{http.MethodPatch},
			handler: s.patchObject,
		},
		{
			path:    "/b/{bucketName}/o/{objectName:.+}",
			methods: []string{http.MethodDelete},
			handler: s.deleteObject,
		},
	}

	m := mux.NewRouter()
	for _, h := range jsonHandlers {
		m.Path(h.path).Methods(h.methods...).HandlerFunc(jsonToHTTPHandler(h.handler))
	}

	// Raw handlers.
	m.Path("/b/{bucketName}/o/{objectName:.+}").Methods(
		http.MethodGet, http.MethodHead).HandlerFunc(s.getObject)
	m.Path("/{bucketName}/{objectName:.+}").Methods(
		http.MethodGet, http.MethodHead).HandlerFunc(rawToHTTPHandler(s.downloadObject))

	return m
}

func (s *fakeGCSServer) createBucket(req *http.Request) (jsonResponse, error) {
	data, err := fromCreateBucketRequest(req)
	if err != nil {
		return jsonResponse{}, err
	}
	resp, err := s.gcs.NewBucket(data.Name)
	return jsonResponse{Data: resp}, err
}

func (s *fakeGCSServer) listObjects(req *http.Request) (jsonResponse, error) {
	data, err := fromListObjectsRequest(req)
	if err != nil {
		return jsonResponse{}, err
	}
	resp, err := s.gcs.ListObjects(data)
	return jsonResponse{Data: resp}, err
}

func (s *fakeGCSServer) uploadObject(req *http.Request) (jsonResponse, error) {
	data, err := fromUploadObjectRequest(req)
	if err != nil {
		return jsonResponse{}, err
	}
	resp, err := s.gcs.UploadObject(data)
	return jsonResponse{Data: resp}, err
}

func (s *fakeGCSServer) patchObject(req *http.Request) (jsonResponse, error) {
	data, err := fromPatchObjectRequest(req)
	if err != nil {
		return jsonResponse{}, err
	}
	resp, err := s.gcs.PatchObject(data)
	return jsonResponse{Data: resp}, err
}

func (s *fakeGCSServer) deleteObject(req *http.Request) (jsonResponse, error) {
	data, err := fromDeleteObjectRequest(req)
	if err != nil {
		return jsonResponse{}, err
	}
	if err := s.gcs.DeleteObject(data); err != nil {
		return jsonResponse{}, err
	}
	return jsonResponse{Status: http.StatusNoContent}, nil
}

func (s *fakeGCSServer) getObject(w http.ResponseWriter, req *http.Request) {
	if alt := req.URL.Query().Get("alt"); alt == "media" || req.Method == http.MethodHead {
		rawToHTTPHandler(s.downloadObject)(w, req)
		return
	}
	handler := jsonToHTTPHandler(func(r *http.Request) (jsonResponse, error) {
		data, err := fromGetObjectRequest(req)
		if err != nil {
			return jsonResponse{}, err
		}
		res, err := s.gcs.DownloadObject(data)
		return jsonResponse{Data: res}, err
	})
	handler(w, req)
}

func (s *fakeGCSServer) downloadObject(req *http.Request) (rawResponse, error) {
	data, err := fromDownloadObjectRequest(req)
	if err != nil {
		return rawResponse{}, err
	}
	res, err := s.gcs.DownloadObject(data)
	if err != nil {
		return rawResponse{}, err
	}

	body := res.Contents
	if req.Method == http.MethodHead {
		// Do not send contents for HEAD requests.
		body = nil
	}
	header := http.Header{}
	header.Set("Accept-Ranges", "bytes")
	header.Set("Content-Length", strconv.Itoa(int(res.Size)))
	header.Set("X-Goog-Generation", strconv.FormatInt(res.Generation, 10))
	header.Set("X-Goog-Metageneration", strconv.FormatInt(res.Metageneration, 10))

	return rawResponse{Body: body, Header: header}, nil
}

type errStatus struct {
	error
	Code int
}

func withStatus(err error, status int) error {
	return errStatus{err, status}
}

func statusFromError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var es errStatus
	if errors.As(err, &es) {
		return es.Code
	}
	return http.StatusInternalServerError
}

type jsonResponse struct {
	Header http.Header
	Data   interface{}
	Status int
}

type jsonHandler = func(r *http.Request) (jsonResponse, error)

func jsonToHTTPHandler(h jsonHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := h(r)
		w.Header().Set("Content-Type", "application/json")
		for name, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}

		var (
			status int
			data   interface{}
		)

		if err != nil {
			status = statusFromError(err)
			data = newErrorResponse(status, err.Error(), nil)
		} else {
			status = resp.Status
			if status == 0 {
				status = http.StatusOK
			}
			data = resp.Data
		}

		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(data)
	}
}

type rawResponse struct {
	Header http.Header
	Body   []byte
}

type rawResponseHandler = func(r *http.Request) (rawResponse, error)

func rawToHTTPHandler(h rawResponseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := h(r)
		w.Header().Set("Content-Type", "application/octet-stream")
		for name, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}

		status := statusFromError(err)

		var data []byte
		if status >= 400 {
			data = []byte(err.Error())
		} else {
			data = resp.Body
		}

		w.WriteHeader(status)
		if len(data) > 0 {
			_, _ = w.Write(data)
		}
	}
}
