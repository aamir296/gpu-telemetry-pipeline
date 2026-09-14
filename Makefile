SHELL := /bin/sh
.DEFAULT_GOAL := help

GOCACHE ?= $(CURDIR)/.cache/go-build
GOLANGCI_LINT ?= golangci-lint
IMAGE_REPOSITORY ?= gpu-telemetry
IMAGE_TAG ?= dev
INPUT_CSV ?= input/real-dcgm.csv
CLUSTER_PROVIDER ?= kind
CLUSTER_NAME ?= gpu-telemetry-e2e
RELEASE_NAME ?= telemetry
NAMESPACE ?= gpu-telemetry
STREAMER_REPLICAS ?= 1
COLLECTOR_REPLICAS ?= 1
API_LOCAL_PORT ?= 18080
COVERAGE_MIN ?= 80
COVER_PACKAGES := ./internal/...

export GOCACHE
export IMAGE_REPOSITORY IMAGE_TAG INPUT_CSV CLUSTER_PROVIDER CLUSTER_NAME RELEASE_NAME NAMESPACE STREAMER_REPLICAS COLLECTOR_REPLICAS API_LOCAL_PORT

.PHONY: help doctor fmt fmt-check lint test integration integration-test benchmark coverage coverage-check build image images openapi openapi-check helm-lint verify deploy scale logs e2e resilience-e2e local-e2e cluster-down clean
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

doctor: ## Check macOS/local-cluster prerequisites and Docker availability.
	@./scripts/doctor.sh

fmt: ## Format all Go code.
	@gofmt -w $$(find cmd internal -name '*.go' -type f)

fmt-check: ## Fail if tracked Go files are not formatted.
	@files=$$(gofmt -l $$(find cmd internal -name '*.go' -type f)); test -z "$$files" || { echo "Unformatted files:"; echo "$$files"; exit 1; }

lint: fmt-check ## Run static analysis (and golangci-lint when installed).
	@go vet ./...
	@if command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then $(GOLANGCI_LINT) run; else echo "golangci-lint not installed; go vet completed"; fi

test: ## Run the complete unit test suite with the race detector.
	@go test -race ./...

integration: ## Run PostgreSQL repository integration tests in an ephemeral container.
	@./scripts/integration.sh

integration-test: integration ## Alias for integration, convenient in CI and review scripts.

benchmark: ## Run queue performance smoke benchmarks without enforcing machine-specific numbers.
	@go test -run '^$$' -bench . -benchtime=1s ./internal/broker/store

coverage: ## Generate coverage.out and print function coverage.
	@go test -covermode=atomic -coverprofile=coverage.out $(COVER_PACKAGES)
	@go tool cover -func=coverage.out

coverage-check: coverage ## Enforce meaningful line coverage (default 80%).
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$$3); print $$3}'); awk -v actual="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN {if (actual+0 < minimum+0) {printf "coverage %.1f%% is below %.1f%%\n",actual,minimum; exit 1}; printf "coverage gate passed: %.1f%% >= %.1f%%\n",actual,minimum}'

build: ## Build all service binaries for the host.
	@mkdir -p bin
	@for service in queue streamer collector api migrate retention e2e openapi; do go build -trimpath -o bin/$$service ./cmd/$$service; done

image: ## Build the local container image for the host architecture.
	@docker build --build-arg INPUT_CSV="$(INPUT_CSV)" --tag $(IMAGE_REPOSITORY):$(IMAGE_TAG) .

images: image ## Alias for the single multi-service image build.

openapi: ## Regenerate the committed OpenAPI contract.
	@mkdir -p docs
	@go run ./cmd/openapi > docs/openapi.yaml

openapi-check: ## Verify the generated OpenAPI contract is current.
	@tmp=$$(mktemp); trap 'rm -f "$$tmp"' EXIT; go run ./cmd/openapi > "$$tmp"; cmp -s "$$tmp" docs/openapi.yaml || { echo "docs/openapi.yaml is stale; run make openapi"; diff -u docs/openapi.yaml "$$tmp" || true; exit 1; }

helm-lint: ## Lint and render the Helm chart without a cluster.
	@helm lint deploy/helm/gpu-telemetry
	@helm template telemetry deploy/helm/gpu-telemetry >/dev/null

verify: lint test coverage-check openapi-check helm-lint build ## Run all deterministic pre-deployment checks.

deploy: ## Build and install/upgrade the stack, then run API acceptance checks only.
	@RUN_RESILIENCE_E2E=0 ./scripts/local-e2e.sh

scale: ## Scale workers in the retained local cluster (STREAMER_REPLICAS/COLLECTOR_REPLICAS).
	@context=$$(if [ "$(CLUSTER_PROVIDER)" = kind ]; then echo "kind-$(CLUSTER_NAME)"; else echo "$(CLUSTER_NAME)"; fi); \
	 kubectl --context "$$context" -n "$(NAMESPACE)" scale deployment/$(RELEASE_NAME)-gpu-telemetry-streamer --replicas="$(STREAMER_REPLICAS)"; \
	 kubectl --context "$$context" -n "$(NAMESPACE)" scale deployment/$(RELEASE_NAME)-gpu-telemetry-collector --replicas="$(COLLECTOR_REPLICAS)"

logs: ## Show recent logs for all application components in the retained cluster.
	@context=$$(if [ "$(CLUSTER_PROVIDER)" = kind ]; then echo "kind-$(CLUSTER_NAME)"; else echo "$(CLUSTER_NAME)"; fi); \
	 kubectl --context "$$context" -n "$(NAMESPACE)" logs -l app.kubernetes.io/instance=$(RELEASE_NAME) --all-containers --prefix --tail=100

e2e: local-e2e ## Alias for the complete local-cluster acceptance workflow.

resilience-e2e: ## Run scaling and restart checks against the retained cluster.
	@./scripts/resilience-e2e.sh

local-e2e: ## Build, deploy, and verify everything in kind (or CLUSTER_PROVIDER=minikube).
	@./scripts/local-e2e.sh

cluster-down: ## Delete only the named local E2E cluster.
	@./scripts/cluster-down.sh

clean: ## Remove generated host artifacts (does not touch clusters or persisted data).
	@rm -rf bin .cache coverage.out
