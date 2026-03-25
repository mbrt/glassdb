#!/bin/bash

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

go tool benchstat "${STATS}"
echo "All runs saved in ${STATS}"
