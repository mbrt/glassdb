#!/bin/bash

set -e

protoc -I=. --go_out=. --go_opt=paths=source_relative \
    --plugin=protoc-gen-go="$(go tool -n protoc-gen-go)" \
    transaction.proto
