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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"strings"
	"sync"

	"github.com/mbrt/glassdb/internal/stringset"
)

func newFakeGCS() *gcs {
	return &gcs{
		buckets: make(map[string]*bucket),
		nextGen: 100,
	}
}

type gcs struct {
	buckets map[string]*bucket
	nextGen int64
	m       sync.Mutex
}

func (g *gcs) NewBucket(name string) (createBucketResponse, error) {
	g.m.Lock()
	defer g.m.Unlock()

	if _, ok := g.buckets[name]; ok {
		return createBucketResponse{},
			withStatus(fmt.Errorf("bucket %q already exists", name), http.StatusConflict)
	}
	b := &bucket{
		Name:    name,
		Objects: make(map[string]*bucketObject),
	}
	g.buckets[name] = b

	return newCreateBucketResponse(b), nil
}

func (g *gcs) ListObjects(req listObjectsRequest) (listObjectsResponse, error) {
	g.m.Lock()
	defer g.m.Unlock()

	b, ok := g.buckets[req.Bucket]
	if !ok {
		return listObjectsResponse{},
			withStatus(fmt.Errorf("bucket %q not found", req.Bucket), http.StatusNotFound)
	}

	var objs []*bucketObject
	prefixes := stringset.New()

	for name, obj := range b.Objects {
		if req.Prefix != "" {
			if !strings.HasPrefix(name, req.Prefix) {
				continue
			}
			name = strings.Replace(name, req.Prefix, "", 1)
		}
		delimPos := strings.Index(name, req.Delimiter)
		if req.Delimiter != "" && delimPos > -1 {
			prefix := obj.Name[:len(req.Prefix)+delimPos+1]
			if isInOffset(prefix, req.StartOffset, req.EndOffset) {
				prefixes.Add(prefix)
			}
			continue
		}
		if isInOffset(obj.Name, req.StartOffset, req.EndOffset) {
			objs = append(objs, obj)
		}
	}

	return newListObjectsResponse(b, prefixes.ToSlice(), objs), nil
}

func (g *gcs) UploadObject(req uploadObjectRequest) (uploadObjectResponse, error) {
	if !g.verifyChecksum(req.Data, req.Metadata.Crc32) {
		return uploadObjectResponse{},
			withStatus(errors.New("wrong checksum"), http.StatusBadRequest)
	}

	g.m.Lock()
	defer g.m.Unlock()

	meta := req.Metadata
	bucket, obj, err := g.getObject(meta.Bucket, meta.Name)
	if err != nil && bucket == nil {
		return uploadObjectResponse{}, err
	}
	// Check conditions (even for null objects).
	if err := g.checkConditions(obj, req.Conditions); err != nil {
		return uploadObjectResponse{}, err
	}

	if obj == nil {
		// This is a new object.
		obj = &bucketObject{
			Name:           meta.Name,
			Contents:       req.Data,
			Metadata:       meta.Metadata,
			Generation:     g.nextGeneration(),
			Metageneration: 1,
		}
		bucket.Objects[meta.Name] = obj
		return newUploadObjectResponse(bucket, obj), nil
	}

	// Update the object.
	g.updateMetadata(obj, meta.Metadata)
	obj.Contents = req.Data
	obj.Generation = g.nextGeneration()

	return newUploadObjectResponse(bucket, obj), nil
}

func (g *gcs) PatchObject(req patchObjectRequest) (patchObjectResponse, error) {
	g.m.Lock()
	defer g.m.Unlock()

	bucket, obj, err := g.getObject(req.Bucket, req.Name)
	if err != nil {
		return patchObjectResponse{}, err
	}
	if err := g.checkConditions(obj, req.Conditions); err != nil {
		return patchObjectResponse{}, err
	}
	g.updateMetadata(obj, req.Metadata)

	return newPatchObjectReponse(bucket, obj), nil
}

func (g *gcs) DeleteObject(req deleteObjectRequest) error {
	g.m.Lock()
	defer g.m.Unlock()

	bucket, obj, err := g.getObject(req.Bucket, req.Name)
	if err != nil {
		return err
	}
	if err := g.checkConditions(obj, req.Conditions); err != nil {
		return err
	}

	delete(bucket.Objects, req.Name)
	return nil
}

func (g *gcs) DownloadObject(req downloadObjectRequest) (downloadObjectResponse, error) {
	g.m.Lock()
	defer g.m.Unlock()

	bucket, obj, err := g.getObject(req.Bucket, req.Name)
	if err != nil {
		return downloadObjectResponse{}, err
	}
	if err := g.checkConditions(obj, req.Conditions); err != nil {
		return downloadObjectResponse{}, err
	}

	return newDownloadObjectResponse(bucket, obj), nil
}

func (g *gcs) nextGeneration() int64 {
	// Note that this is not locked.
	res := g.nextGen
	g.nextGen++
	return res
}

func (g *gcs) checkConditions(obj *bucketObject, c objectConditions) error {
	if obj == nil {
		return g.checkConditionsForNewObject(c)
	}

	// Check conditions for previous object.
	if gm := c.IfGenerationMatch; gm != nil && *gm != obj.Generation {
		return withStatus(fmt.Errorf("generation is %v, required %v", obj.Generation, *gm),
			http.StatusPreconditionFailed)
	}
	if gm := c.IfMetagenerationMatch; gm != nil && *gm != obj.Metageneration {
		return withStatus(fmt.Errorf("metageneration is %v, required %v", obj.Metageneration, *gm),
			http.StatusPreconditionFailed)
	}
	if gm := c.IfGenerationNotMatch; gm != nil && *gm == obj.Generation {
		return withStatus(fmt.Errorf("generation is %v, required different", obj.Generation),
			http.StatusPreconditionFailed)
	}
	if gm := c.IfMetagenerationNotMatch; gm != nil && *gm == obj.Metageneration {
		return withStatus(fmt.Errorf("metageneration is %v, required different", obj.Metageneration),
			http.StatusPreconditionFailed)
	}

	return nil
}

func (g *gcs) checkConditionsForNewObject(c objectConditions) error {
	if gm := c.IfGenerationMatch; gm != nil && *gm != 0 {
		// When zero it means "if it doesn't exist", which is the only
		// supported condition on null objects.
		return withStatus(fmt.Errorf("object doesn't exist, required %v generation", *gm),
			http.StatusPreconditionFailed)
	}
	// If any other condition is given, it's a failure.
	if gm := c.IfMetagenerationMatch; gm != nil {
		return withStatus(fmt.Errorf("object doesn't exist, required %v metageneration", *gm),
			http.StatusPreconditionFailed)
	}
	if gm := c.IfGenerationNotMatch; gm != nil {
		return withStatus(fmt.Errorf("object doesn't exist, required not %v generation", *gm),
			http.StatusPreconditionFailed)
	}
	if gm := c.IfMetagenerationNotMatch; gm != nil {
		return withStatus(fmt.Errorf("object doesn't exist, required not %v metageneration", *gm),
			http.StatusPreconditionFailed)
	}

	return nil
}

func (g *gcs) verifyChecksum(data []byte, crc32c string) bool {
	if crc32c == "" {
		return true
	}
	// CRC32c checksum, encoded using base64 in big-endian byte order.
	// See https://cloud.google.com/storage/docs/json_api/v1/objects/insert
	buf, err := base64.StdEncoding.DecodeString(crc32c)
	if err != nil {
		return false
	}
	want := binary.BigEndian.Uint32(buf)
	got := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	return want == got
}

func (g *gcs) getObject(bucketName, name string) (*bucket, *bucketObject, error) {
	bucket, ok := g.buckets[bucketName]
	if !ok {
		return nil, nil,
			withStatus(fmt.Errorf("bucket %q not found", bucketName), http.StatusNotFound)
	}
	obj, ok := bucket.Objects[name]
	if !ok {
		return bucket, nil,
			withStatus(fmt.Errorf("object %q not found", name), http.StatusNotFound)
	}
	return bucket, obj, nil
}

func (g *gcs) updateMetadata(obj *bucketObject, meta map[string]string) {
	if len(meta) == 0 {
		return
	}
	if obj.Metadata == nil {
		obj.Metadata = make(map[string]string)
	}
	for k, v := range meta {
		obj.Metadata[k] = v
	}
	obj.Metageneration++
}

type bucket struct {
	Name    string
	Objects map[string]*bucketObject
}

type bucketObject struct {
	Name           string
	Contents       []byte
	Metadata       map[string]string
	Generation     int64
	Metageneration int64
}

func isInOffset(name, start, end string) bool {
	switch {
	case end != "" && start != "":
		return strings.Compare(name, end) < 0 && strings.Compare(name, start) >= 0
	case end != "":
		return strings.Compare(name, end) < 0
	case start != "":
		return strings.Compare(name, start) >= 0
	default:
		return true
	}
}
