#!/bin/bash

set -xe

go test -timeout=120s ./...
go test -timeout=180s -race ./...
go test -timeout=30s -count=10 ./internal/trans
# The repeated full-suite run exercises three backends (memory, gcs, s3); the
# HTTP-backed s3 and gcs subtests dominate, so this needs a generous timeout.
# Failing tests dump buffered debug logs automatically, so --debug-logs (live
# streaming) is no longer needed here and would only slow the run down.
go test -timeout=420s -count=10 .
