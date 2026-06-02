#!/bin/bash

set -euo pipefail

cd $(dirname $(readlink -f $0))/..

# FuzzConcurrentTx drives the full DB stack, so measure coverage across all
# packages (including internal/trans) to see what the fuzzer actually exercises.
go test -run=FuzzConcurrentTx -coverpkg=./... -coverprofile=/tmp/cover.out .
go tool cover -html=/tmp/cover.out
