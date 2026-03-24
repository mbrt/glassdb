#!/bin/bash

set -e

PATTERN=${1:-.}

cd $(dirname $(readlink -f $0))/..

go test -test.v -bench "${PATTERN}" -cpuprofile=docs/profile/cpu.prof
go test -test.v -benchmem -bench "${PATTERN}" -memprofile=docs/profile/mem.prof
go test -test.v -tags tracing -bench "${PATTERN}" -trace=docs/profile/trace.out
go tool pprof -http=":8081" glassdb.test docs/profile/cpu.prof
go tool pprof -http=":8081" glassdb.test docs/profile/mem.prof
go tool trace docs/profile/trace.out
