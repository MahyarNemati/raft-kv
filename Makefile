GO ?= go
# The macOS SDK on some hosts cannot link cgo; everything here is pure Go.
export CGO_ENABLED ?= 0
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: all build test race lint fmt vet cover bench cluster cluster-stop clean

all: lint test

build:
	$(GO) build -trimpath -ldflags "-s -w" -o bin/raftkv      ./cmd/raftkv
	$(GO) build -trimpath -ldflags "-s -w" -o bin/raftkvctl   ./cmd/raftkvctl
	$(GO) build -trimpath -ldflags "-s -w" -o bin/raftkv-bench ./cmd/raftkv-bench

## test: unit, cluster and linearizability tests
test:
	$(GO) test -race -count=1 ./...

## cover: coverage summary
cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

## bench: throughput and latency across storage modes
bench: build
	./bin/raftkv-bench -workload write,read,mixed -storage memory,wal,wal-fsync -markdown

lint: fmt vet

fmt:
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

vet:
	$(GO) vet ./...

## cluster: start a 3-node cluster as local processes
cluster: build
	./scripts/local-cluster.sh start

cluster-stop:
	./scripts/local-cluster.sh stop

clean:
	rm -rf bin data coverage.out
