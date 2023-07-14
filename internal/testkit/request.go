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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

func fromCreateBucketRequest(req *http.Request) (createBucketRequest, error) {
	defer req.Body.Close()
	res := createBucketRequest{}
	err := decodeJSONRequest(&res, req.Body)
	return res, err
}

type createBucketRequest struct {
	Name string `json:"name"`
}

func fromListObjectsRequest(req *http.Request) (listObjectsRequest, error) {
	q := req.URL.Query()
	if q.Get("versions") == "true" {
		return listObjectsRequest{}, fmt.Errorf("versions=true: %w", errNotImplemented)
	}
	if q.Get("includeTrailingDelimiter") == "true" {
		return listObjectsRequest{},
			fmt.Errorf("includeTrailingDelimiter=true: %w", errNotImplemented)
	}
	return listObjectsRequest{
		Bucket:      mux.Vars(req)["bucketName"],
		Prefix:      q.Get("prefix"),
		Delimiter:   q.Get("delimiter"),
		StartOffset: q.Get("startOffset"),
		EndOffset:   q.Get("endOffset"),
	}, nil
}

type listObjectsRequest struct {
	Bucket      string
	Prefix      string
	Delimiter   string
	StartOffset string
	EndOffset   string
}

func fromUploadObjectRequest(req *http.Request) (uploadObjectRequest, error) {
	defer req.Body.Close()

	res := uploadObjectRequest{}

	var err error
	res.Conditions, err = parseRequestConditions(req)
	if err != nil {
		return res, err
	}

	_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return res, withStatus(fmt.Errorf("invalid Content-Type header: %v", err),
			http.StatusBadRequest)
	}
	boundary := params["boundary"]
	if boundary == "" {
		return res, withStatus(errors.New("expected 'boundary' header"), http.StatusBadRequest)
	}
	chunks, err := chunksFromMultipart(req.Body, boundary)
	if err != nil {
		return res, err
	}
	if len(chunks) != 2 {
		return res, withStatus(fmt.Errorf("expected two chunks, got %d", len(chunks)),
			http.StatusBadRequest)
	}
	res.Data = chunks[1]
	err = decodeJSONRequest(&res.Metadata, bytes.NewReader(chunks[0]))
	return res, err
}

type uploadObjectRequest struct {
	Conditions objectConditions
	Metadata   uploadMetadata
	Data       []byte
}

type uploadMetadata struct {
	Bucket   string            `json:"bucket"`
	Name     string            `json:"name"`
	Metadata map[string]string `json:"metadata"`
}

func parseRequestConditions(req *http.Request) (objectConditions, error) {
	if err := req.ParseForm(); err != nil {
		return objectConditions{},
			withStatus(fmt.Errorf("invalid form: %v", err), http.StatusBadRequest)
	}

	parseInt64 := func(name string) *int64 {
		val := req.Form.Get(name)
		if val == "" {
			return nil
		}
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil
		}
		return &i
	}

	return objectConditions{
		IfGenerationMatch:        parseInt64("ifGenerationMatch"),
		IfMetagenerationMatch:    parseInt64("ifMetagenerationMatch"),
		IfGenerationNotMatch:     parseInt64("ifGenerationNotMatch"),
		IfMetagenerationNotMatch: parseInt64("ifMetagenerationNotMatch"),
	}, nil
}

type objectConditions struct {
	IfGenerationMatch        *int64
	IfMetagenerationMatch    *int64
	IfGenerationNotMatch     *int64
	IfMetagenerationNotMatch *int64
}

func fromDownloadObjectRequest(req *http.Request) (downloadObjectRequest, error) {
	cond, err := parseRequestConditions(req)
	if err != nil {
		return downloadObjectRequest{}, err
	}

	vars := mux.Vars(req)
	return downloadObjectRequest{
		Bucket:     vars["bucketName"],
		Name:       vars["objectName"],
		Conditions: cond,
	}, nil
}

type downloadObjectRequest struct {
	Bucket     string
	Name       string
	Conditions objectConditions
}

func fromGetObjectRequest(req *http.Request) (downloadObjectRequest, error) {
	return fromDownloadObjectRequest(req)
}

func fromPatchObjectRequest(req *http.Request) (patchObjectRequest, error) {
	defer req.Body.Close()

	cond, err := parseRequestConditions(req)
	if err != nil {
		return patchObjectRequest{}, err
	}

	vars := mux.Vars(req)
	res := patchObjectRequest{
		Bucket:     vars["bucketName"],
		Name:       vars["objectName"],
		Conditions: cond,
	}
	err = decodeJSONRequest(&res, req.Body)

	return res, err
}

type patchObjectRequest struct {
	Bucket     string `json:"bucket"`
	Name       string
	Metadata   map[string]string `json:"metadata"`
	Conditions objectConditions
}

func fromDeleteObjectRequest(req *http.Request) (deleteObjectRequest, error) {
	defer req.Body.Close()

	cond, err := parseRequestConditions(req)
	if err != nil {
		return deleteObjectRequest{}, err
	}

	vars := mux.Vars(req)
	return deleteObjectRequest{
		Bucket:     vars["bucketName"],
		Name:       vars["objectName"],
		Conditions: cond,
	}, nil
}

type deleteObjectRequest struct {
	Bucket     string
	Name       string
	Conditions objectConditions
}

func decodeJSONRequest(res interface{}, r io.Reader) error {
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&res); err != nil {
		return withStatus(err, http.StatusBadRequest)
	}
	return nil
}

func chunksFromMultipart(r io.Reader, boundary string) ([][]byte, error) {
	// The expected format is as follow:
	// --boundary
	// Content-Type: <a>
	// <empty-line>
	// <contents>
	// <... contents>
	// --boundary
	// ...
	// --boundary--
	var chunks [][]byte

	mpr := multipart.NewReader(r, boundary)
	for {
		part, err := mpr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return chunks, err
		}
		chunk, err := io.ReadAll(part)
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}
