# Testing and local evaluation

## Test policy

“75–80% passing” should not mean accepting failing tests. Every test must pass; the numeric threshold is treated as line coverage. `make test` runs all unit tests with Go's race detector. `make coverage-check` measures every package under `./internal/...` and enforces at least 80% statement coverage. The current measured value is 80.7%; collector and broker-client coverage are independently above 80%.

Database adapter behavior is deliberately not inflated with mocks. `make integration` launches real PostgreSQL 17, applies the embedded schema, checks transactional persistence/idempotency, and exercises inventory, exact lookup, and stable cursor pages. `make local-e2e` then validates the assembled container/Kubernetes system.

## Acceptance sequence

Run from repository root:

```bash
make doctor
make lint
make test
make coverage-check
make integration
make benchmark
make helm-lint
make openapi-check
make local-e2e
```

The API assertion waits for data and verifies:

- at least one GPU and telemetry row are queryable;
- row/source IDs and all three timestamp roles are present;
- dynamic `observed_at` differs from historical `source_timestamp`;
- an inclusive zero-width time window includes its boundary row;
- `event_id` returns exactly one row;
- a one-row cursor page advances without repeating the event.

The resilience phase then verifies:

- a 5-to-1 streamer scale-down in the middle of a cycle replays that same cycle to exactly the CSV row count;
- long-running cycles renew producer leases without manufacturing membership changes;
- collector termination recovers without advancing an unpersisted offset;
- the queue restarts from its PVC while producers are active and ingestion resumes;
- no duplicate `source_event_key` exists;
- every component exposes application and runtime Prometheus metrics.

## Inspect and troubleshoot

```bash
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry get pods,pvc,svc
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry logs deployment/telemetry-gpu-telemetry-streamer
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry logs deployment/telemetry-gpu-telemetry-collector
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry logs statefulset/telemetry-gpu-telemetry-queue
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry describe pod POD_NAME
```

If `make doctor` reports `docker-daemon NOT RUNNING`, start Docker Desktop and wait until `docker info` succeeds. On Apple Silicon, the default build is native arm64; kind loads that local image without a registry. Release automation can use `docker buildx build --platform linux/amd64,linux/arm64` with the same Dockerfile.

`make local-e2e` retains the cluster after success or failure so evidence is inspectable. `make cluster-down` deletes only the configured local cluster name. Override `CLUSTER_NAME`, `NAMESPACE`, or `RELEASE_NAME` if those defaults conflict with another local project.

## Failure-injection checks

The automated unit suite covers torn log tails, record checksum corruption, idempotent republish/reopen, monotonic offset commits, invalid commits, stale producer generations, long-cycle producer heartbeats, membership expiry and scale-down replay, collector persist-before-commit ordering, cursor validation, API safety ceilings, malformed frames/batches, and invalid CSV fields. `make local-e2e` automates the durability demonstration. To repeat only the live failure checks against its retained cluster, run `make resilience-e2e`.

```bash
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry delete pod telemetry-gpu-telemetry-queue-0
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry rollout status statefulset/telemetry-gpu-telemetry-queue
```
