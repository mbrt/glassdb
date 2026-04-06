#!/bin/bash

set -euo pipefail

cd $(dirname $(readlink -f $0))/..

go test -run=FuzzAlgoConcurrentTx -coverprofile=/tmp/cover.out ./internal/trans
go tool cover -html=/tmp/cover.out
