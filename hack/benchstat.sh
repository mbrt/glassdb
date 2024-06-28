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


# See https://pkg.go.dev/golang.org/x/perf/cmd/benchstat

set -e
set -o pipefail

PATTERN=${1:-.}

cd $(dirname $(readlink -f $0))/..
STATS=$(mktemp)

echo "Saving results to ${STATS}"

for i in {1..10}; do
    echo "Benchmark $i/10"
    go test -benchmem -bench "${PATTERN}" --print-stats | tee -a "${STATS}"
done

benchstat "${STATS}"
echo "All runs saved in ${STATS}"
