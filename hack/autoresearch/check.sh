#!/bin/bash
#
# Correctness gate for the autoresearch loop. This protects strict
# serializability: an experiment may only be kept if this script passes.
#
# This file is part of the autoresearch fixed infrastructure and must NOT be
# modified by autoresearch experiments.
#
# Usage:
#   hack/autoresearch/check.sh            # fast tier (run every experiment)
#   hack/autoresearch/check.sh --full     # full tier (before keeping a change)
#
# Tunables (env):
#   FUZZTIME       fuzz budget per target in the fast tier (default 30s)
#   FULL_FUZZTIME  fuzz budget per target in the full tier (default 120s)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

FUZZTIME="${FUZZTIME:-30s}"
FULL_FUZZTIME="${FULL_FUZZTIME:-120s}"

mode="fast"
if [[ "${1:-}" == "--full" ]]; then
	mode="full"
fi

echo "== build =="
go build ./...

if [[ "$mode" == "full" ]]; then
	echo "== full test suite (make test) =="
	make test

	echo "== serializability fuzz (FuzzConcurrentTx, ${FULL_FUZZTIME}) =="
	go test -run='^$' -fuzz='^FuzzConcurrentTx$' -fuzztime="${FULL_FUZZTIME}" -timeout=0 .

	echo "== serializability fuzz (FuzzAlgoConcurrentTx, ${FULL_FUZZTIME}) =="
	go test -run='^$' -fuzz='^FuzzAlgoConcurrentTx$' -fuzztime="${FULL_FUZZTIME}" -timeout=0 ./internal/trans

	echo "== check OK (full) =="
	exit 0
fi

echo "== race tests =="
go test -race -timeout=180s ./...

echo "== serializability fuzz (FuzzConcurrentTx, ${FUZZTIME}) =="
go test -run='^$' -fuzz='^FuzzConcurrentTx$' -fuzztime="${FUZZTIME}" -timeout=0 .

echo "== serializability fuzz (FuzzAlgoConcurrentTx, ${FUZZTIME}) =="
go test -run='^$' -fuzz='^FuzzAlgoConcurrentTx$' -fuzztime="${FUZZTIME}" -timeout=0 ./internal/trans

echo "== check OK (fast) =="
