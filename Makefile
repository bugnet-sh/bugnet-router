.PHONY: test lint build image-inputs x402-local

test:
	go test -race -timeout 40m ./...

lint:
	golangci-lint run ./...

build:
	go build ./...

# Rewrites deploy/image/inputs.sha256 once VERSION moved or while no build record of it exists,
# and fails when an input moved under a version that was built, CP 19.6.3
image-inputs:
	go run ./internal/image/inputs

# The x402 local run, XS 11.2 and 11.3: the real router, egress and Payments on a fake chain
X402_BIN = $(CURDIR)/bin/x402local

x402-local:
	mkdir -p $(X402_BIN)
	go build -tags localrun -o $(X402_BIN)/ ./cmd/router-api ./cmd/router-worker ./cmd/bugnet-dev
	cd ../bugnet-edges && go build -tags localrun -o $(X402_BIN)/ ./cmd/bugnet-egress \
		&& go build -o $(X402_BIN)/ ./cmd/x402-fake-seller
	cd ../bugnet-run && go build -o $(X402_BIN)/ ./cmd/bugnet-run
	BUGNET_TEST_X402_LOCAL=1 BUGNET_X402_LOCAL_BIN=$(X402_BIN) \
		go test -count=1 -v -timeout 20m ./test/x402local/
