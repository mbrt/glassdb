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

// Package stringset helps with common set operations on strings.
//
// This is inspired by https://github.com/uber/kraken/blob/master/utils/stringset/stringset.go.
package stringset

// Set is a nifty little wrapper for common set operations on a map. Because it
// is equivalent to a map, make/range/len will still work with Set.
type Set map[string]struct{}

// New creates a new Set with xs.
func New(xs ...string) Set {
	s := make(Set)
	for _, x := range xs {
		s.Add(x)
	}
	return s
}

// Add adds x to s.
func (s Set) Add(x string) {
	s[x] = struct{}{}
}

// Remove removes x from s.
func (s Set) Remove(x string) {
	delete(s, x)
}

// Has returns true if x is in s.
func (s Set) Has(x string) bool {
	_, ok := s[x]
	return ok
}

// ToSlice converts s to a slice.
func (s Set) ToSlice() []string {
	var xs []string
	for x := range s {
		xs = append(xs, x)
	}
	return xs
}
