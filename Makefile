.DEFAULT_GOAL := help

TEST_PACKAGE = ./...
IMAGE_NAME = maxpoletaev/kivi
GO_MODULE = github.com/maxpoletaev/kivi
PROTO_FILES = $(shell find . -type f -name '*.proto')

PLATFORM = $(shell uname)

.PHONY: help
help:  ## print help (this message)
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| sed -n 's/^\(.*\): \(.*\)## \(.*\)/\1;\3/p' \
	| column -t  -s ';'

.PHONY: test
test:  ## run go tests
	@echo "--------- running: $@ ---------"
	go test -v -race -timeout=30s -count=1 $(GO_MODULE)/$(TEST_PACKAGE)

.PHONY: bench
bench:  ## run benchmarks
	@echo "--------- running: $@ ---------"
	go test -bench=. -run=^$$ -benchmem ./...

.PHONY: godoc
godoc:  ## start godoc server at :8000
	@echo "--------- running: $@ ---------"
ifeq ($(PLATFORM),Darwin)
	@(sleep 2; open http://localhost:8000/pkg/$(GO_MODULE)) &
endif
	@echo "serving godoc at http://127.0.0.1:8000"
	@godoc -http=127.0.0.1:8000

.PHONY: generate
generate:  ## run go generate ./...
	@echo "--------- running: $@ ---------"
	go generate ./...

.PHONY: build
build:  ## build the binaries
	@echo "--------- running: $@ ---------"
	go build -o bin/kivi-server $(GO_MODULE)/cmd/kivi-server

.PHONY: image
image: ## build the docker image
	@echo "--------- running: $@ ---------"
	docker build -t kv -t $(IMAGE_NAME) .

.PHONY: proto-clean
proto-clean:  ## clean generated protobuf code
	@echo "--------- running: $@ ---------"
	find . -name '*.pb.go' -not -path './vendor' -delete

.PHONY: proto-gen
proto-gen:  ## generate protobuf/grpc models
	@echo "--------- running: $@ ---------"
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $(PROTO_FILES)

.PHONY: proto
proto: proto-clean proto-gen ## re-generate protobuf models

.PHONY: lint
lint:  ## run linter
	@echo "--------- running: $@ ---------"
	golangci-lint run $(LINT_PACKAGE)

.PHONY: escape
escape: ## run escape analysis
	@echo "--------- running: $@ ---------"
	go test -run=^$$ -gcflags="-m" ./... 2>&1 | grep -Ev '(_test\.go|\$$GOROOT|<autogenerated>|^/var)'
