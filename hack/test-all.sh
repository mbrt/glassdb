#!/bin/bash

set -xe

go test -timeout=30s ./...
go test -timeout=120s -race ./...
go test -timeout=30s -count=10 ./internal/trans
go test -timeout=120s -count=10 . --debug-logs
