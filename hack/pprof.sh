#!/bin/bash

# Copyright 2023 The glassdb Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

PATTERN=${1:-.}

cd $(dirname $(readlink -f $0))/..
mkdir -p profile

go test -test.v -bench "${PATTERN}" -cpuprofile=profile/cpu.prof
go test -test.v -benchmem -bench "${PATTERN}" -memprofile=profile/mem.prof
go test -test.v -tags tracing -bench "${PATTERN}" -trace=profile/trace.out
go tool pprof -http=":8081" glassdb.test profile/cpu.prof
go tool pprof -http=":8081" glassdb.test profile/mem.prof
go tool trace profile/trace.out
