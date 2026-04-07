.PHONY: test unit-test lint format fuzz

FUZZTIME ?= 300s

test: lint unit-test

unit-test:
	hack/test-all.sh

fuzz:
	go test -fuzz=FuzzConcurrentTx -fuzztime=$(FUZZTIME) -timeout=0 .
	go test -fuzz=FuzzAlgoConcurrentTx -fuzztime=$(FUZZTIME) -timeout=0 ./internal/trans

lint:
	go tool revive -config revive.toml ./...
	@test -z "$$(gofmt -s -l .)" || (echo "Unformatted files:"; gofmt -s -l .; exit 1)
	@files=$$(go fix -json ./... | jq -r 'to_entries[] | select(.key | test("internal/proto") | not) | .value | .. | .filename? // empty' | sort -u); \
	if [ -n "$$files" ]; then echo "Files need go fix:"; echo "$$files"; exit 1; fi

format:
	go fmt ./...
