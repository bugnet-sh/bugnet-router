.PHONY: test lint build

test:
	go test -race -timeout 40m ./...

lint:
	golangci-lint run ./...

build:
	go build ./...
