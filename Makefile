.PHONY: test unit-test lint format build build-docker

test: lint unit-test

unit-test:
	hack/test-all.sh

lint:
	go tool revive ./...
	@test -z "$$(gofmt -s -l .)" || (echo "Unformatted files:"; gofmt -s -l .; exit 1)

format:
	go fmt ./...
