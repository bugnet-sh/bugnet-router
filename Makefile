.PHONY: test lint build

test:
	go test -race ./...

lint:
	golangci-lint run ./...

build:
	go build ./...
