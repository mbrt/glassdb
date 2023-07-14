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
	"path"
	"sort"
)

func newErrorResponse(code int, message string, errs []apiError) errResponse {
	return errResponse{
		Error: httpError{
			Code:    code,
			Message: message,
			Errors:  errs,
		},
	}
}

type errResponse struct {
	Error httpError `json:"error"`
}

type httpError struct {
	Code    int        `json:"code"`
	Message string     `json:"message"`
	Errors  []apiError `json:"errors,omitempty"`
}

type apiError struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

func newCreateBucketResponse(b *bucket) createBucketResponse {
	// Example:
	//
	// {
	//     "kind": "storage#bucket",
	//     "id": "test-bucket",
	//     "name": "test-bucket",
	//     "versioning": {},
	//     "timeCreated": "2022-08-03T13:20:02.723954Z"
	// }
	return createBucketResponse{
		Kind: "storage#bucket",
		ID:   b.Name,
		Name: b.Name,
	}
}

type createBucketResponse struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newListObjectsResponse(b *bucket, prefixes []string, objs []*bucketObject) listObjectsResponse {
	// The response needs to be sorted lexicographically.
	sort.Strings(prefixes)
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Name < objs[j].Name
	})

	items := make([]downloadObjectResponse, len(objs))
	for i, obj := range objs {
		items[i] = newDownloadObjectResponse(b, obj)
	}

	return listObjectsResponse{
		Kind:     "storage#objects",
		Items:    items,
		Prefixes: prefixes,
	}
}

type listObjectsResponse struct {
	Kind     string `json:"kind"`
	Items    []downloadObjectResponse
	Prefixes []string `json:"prefixes"`
}

func newUploadObjectResponse(b *bucket, o *bucketObject) uploadObjectResponse {
	// Example:
	//
	// {
	//     "kind": "storage#object",
	//     "name": "example/_c/cm13LWM=/_k/a2V5",
	//     "id": "test-bucket/example/_c/cm13LWM=/_k/a2V5",
	//     "bucket": "test-bucket",
	//     "size": "0",
	//     "contentType": "application/octet-stream",
	//     "crc32c": "AAAAAA==",
	//     "acl": [
	//         {
	//             "bucket": "test-bucket",
	//             "entity": "projectOwner",
	//             "object": "example/_c/cm13LWM=/_k/a2V5",
	//             "projectTeam": {},
	//             "role": "OWNER"
	//         }
	//     ],
	//     "md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg==",
	//     "etag": "\"1B2M2Y8AsgTpgAmY7PhCfg==\"",
	//     "timeCreated": "2022-08-03T13:20:02.725508Z",
	//     "timeDeleted": "0001-01-01T00:00:00Z",
	//     "updated": "2022-08-03T13:20:02.725511Z",
	//     "generation": "1659532802725512",
	//     "metadata": {
	//         "lock-type": "w",
	//         "locked-by": "ZStxstNEv+M3FWKQHCL9RA=="
	//     }
	// }
	return uploadObjectResponse{
		Kind:           "storage#object",
		Name:           o.Name,
		ID:             o.Name,
		Bucket:         b.Name,
		Size:           int64(len(o.Contents)),
		ContentType:    "application/octet-stream",
		Generation:     o.Generation,
		Metageneration: o.Metageneration,
		Metadata:       copyMeta(o.Metadata),
	}
}

type uploadObjectResponse struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Bucket         string            `json:"bucket"`
	Size           int64             `json:"size,string"`
	ContentType    string            `json:"contentType"`
	Generation     int64             `json:"generation,string"`
	Metageneration int64             `json:"metageneration,string"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

func newPatchObjectReponse(b *bucket, o *bucketObject) patchObjectResponse {
	// Example:
	//
	// {
	//     "bucket": "test-bucket",
	//     "name": "test-obj",
	//     "size": "5",
	//     "contentType": "application/octet-stream",
	//     "contentEncoding": "",
	//     "crc32c": "4eADYw==",
	//     "md5Hash": "IGPBYI1uC6+AJJxC4r5YBA==",
	//     "etag": "\"IGPBYI1uC6+AJJxC4r5YBA==\"",
	//     "acl": [
	//         {
	//             "entity": "projectOwner",
	//             "entityId": "",
	//             "role": "OWNER",
	//             "domain": "",
	//             "email": "",
	//             "projectTeam": null
	//         }
	//     ],
	//     "created": "2022-08-10T12:27:54.363598Z",
	//     "updated": "2022-08-10T12:27:54.363602Z",
	//     "deleted": "0001-01-01T00:00:00Z",
	//     "generation": "1660134474363604",
	//     "metadata": {
	//         "k1": "v1",
	//         "k2": "v2"
	//     }
	// }
	return patchObjectResponse{
		Kind:           "storage#object",
		Name:           o.Name,
		Bucket:         b.Name,
		Size:           int64(len(o.Contents)),
		ContentType:    "application/octet-stream",
		Generation:     o.Generation,
		Metageneration: o.Metageneration,
		Metadata:       copyMeta(o.Metadata),
	}
}

type patchObjectResponse struct {
	Kind           string            `json:"kind"`
	Name           string            `json:"name"`
	Bucket         string            `json:"bucket"`
	Size           int64             `json:"size,string"`
	ContentType    string            `json:"contentType"`
	Generation     int64             `json:"generation,string"`
	Metageneration int64             `json:"metageneration,string"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

func newDownloadObjectResponse(b *bucket, o *bucketObject) downloadObjectResponse {
	// Example:
	//
	// {
	//     "kind": "storage#object",
	//     "name": "example/_c/cm13LWM=/_k/a2V5",
	//     "id": "test-bucket/example/_c/cm13LWM=/_k/a2V5",
	//     "bucket": "test-bucket",
	//     "size": "0",
	//     "contentType": "application/octet-stream",
	//     "crc32c": "AAAAAA==",
	//     "acl": [
	//         {
	//             "bucket": "test-bucket",
	//             "entity": "projectOwner",
	//             "object": "example/_c/cm13LWM=/_k/a2V5",
	//             "projectTeam": {},
	//             "role": "OWNER"
	//         }
	//     ],
	//     "md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg==",
	//     "etag": "\"1B2M2Y8AsgTpgAmY7PhCfg==\"",
	//     "timeCreated": "2022-08-03T13:20:02.725508Z",
	//     "timeDeleted": "0001-01-01T00:00:00Z",
	//     "updated": "2022-08-03T13:20:02.725511Z",
	//     "generation": "1659532802725512",
	//     "metadata": {
	//         "lock-type": "w",
	//         "locked-by": "ZStxstNEv+M3FWKQHCL9RA=="
	//     }
	// }
	return downloadObjectResponse{
		Kind:           "storage#object",
		Name:           o.Name,
		ID:             path.Join(b.Name, o.Name),
		Bucket:         b.Name,
		Size:           int64(len(o.Contents)),
		ContentType:    "application/octet-stream",
		Generation:     o.Generation,
		Metageneration: o.Metageneration,
		Metadata:       copyMeta(o.Metadata),
		Contents:       o.Contents,
	}
}

type downloadObjectResponse struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Bucket         string            `json:"bucket"`
	Size           int64             `json:"size,string"`
	ContentType    string            `json:"contentType"`
	Generation     int64             `json:"generation,string"`
	Metageneration int64             `json:"metageneration,string"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Contents       []byte            `json:"-"`
}

func copyMeta(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	res := make(map[string]string)
	for k, v := range m {
		res[k] = v
	}
	return res
}
