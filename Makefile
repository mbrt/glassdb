.PHONY: test unit-test lint format

test: lint unit-test

unit-test:
	hack/test-all.sh

lint:
	go tool revive -config revive.toml ./...
	@test -z "$$(gofmt -s -l .)" || (echo "Unformatted files:"; gofmt -s -l .; exit 1)

format:
	go fmt ./...
