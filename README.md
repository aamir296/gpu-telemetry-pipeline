# GPU Telemetry Pipeline

A complete implementation of the GPU Telemetry Pipeline Message Queue exercise. It streams the supplied DCGM CSV repeatedly, assigns a fresh UTC observation timestamp to every emitted row, routes records through a custom durable queue, stores them in open-source PostgreSQL, and exposes the full history through a documented HTTP API.

The repository is intentionally self-contained: one Go module, one container image, a Helm chart, an honest whole-`internal` 80% coverage gate, PostgreSQL integration tests, and one-command kind/minikube verification.

## Quick start on macOS with kind

Prerequisites:

- Docker Desktop (or another Docker-compatible daemon)
- Go 1.25+
- `kind`, `kubectl`, Helm 3 or 4, and `jq`
- approximately 3 GB of free memory and 3 GB of disk

Homebrew can install the CLI tools:

```bash
brew install go kind kubectl helm jq
open -a Docker
```

From this repository:

```bash
make doctor
make test
make coverage-check
make local-e2e
```

`make local-e2e` builds the image for the Mac's active architecture, creates/reuses only the `gpu-telemetry-e2e` kind cluster, loads the image, installs the Helm release, waits for readiness, and verifies ingestion, queries, a live 5-to-1 streamer scale-down, collector termination, queue restart recovery, cycle completeness, idempotency, and metrics. It leaves the cluster running for inspection. Remove it with `make cluster-down`. Set `RUN_RESILIENCE_E2E=0` only when a shorter API-only development loop is desired.

Port 18080 is used only for the temporary local API tunnel. If it is occupied, select another one, for example `make local-e2e API_LOCAL_PORT=18082`. The script waits for the tunnel to become healthy and prints its log if startup fails.

To run the same workflow with minikube:

```bash
make local-e2e CLUSTER_PROVIDER=minikube
make cluster-down CLUSTER_PROVIDER=minikube
```

To exercise horizontal membership and rebalancing (the exercise bounds both worker types at 10):

```bash
make local-e2e STREAMER_REPLICAS=2 COLLECTOR_REPLICAS=2
```

## Use the supplied or another CSV

The supplied 2,470-row DCGM file is committed as `input/real-dcgm.csv` and is the default, so a clean clone exercises 247 GPUs rather than a toy dataset. `examples/sample.csv` remains available for fast parser experiments. To exercise another source, copy it inside the repository's ignored `input/` directory, then build/deploy it:

```bash
mkdir -p input
cp /path/to/dcgm_metrics.csv input/input.csv
make local-e2e INPUT_CSV=input/input.csv
```

The parser uses column names, not positions. Required columns are `timestamp`, `metric_name`, `gpu_id`, `device`, `modelName`, `Hostname`, `value`, and `labels_raw`; `uuid`, `container`, `pod`, and `namespace` are preserved when present. For inputs too large or sensitive to place in an image, set `streamer.inputPVC` and mount a PVC containing `input.csv`.

## Query the result

Keep a port-forward open:

```bash
kubectl --context kind-gpu-telemetry-e2e -n gpu-telemetry \
  port-forward service/telemetry-gpu-telemetry-api 18080:8080
```

Then select a real GPU and one of its returned events. These commands derive IDs and timestamps from the running system, so they continue to work when the input CSV changes:

```bash
# Inventory of GPU IDs
curl -s http://127.0.0.1:18080/api/v1/gpus

GPU_ID=$(curl -s http://127.0.0.1:18080/api/v1/gpus | jq -r '.items[0].id')

# Every stored row from every previous cycle for one GPU
curl -s "http://127.0.0.1:18080/api/v1/gpus/$GPU_ID/telemetry"

# Inclusive dynamic observation-time window
FIRST_PAGE=$(curl -s "http://127.0.0.1:18080/api/v1/gpus/$GPU_ID/telemetry?limit=1")
OBSERVED_AT=$(printf '%s' "$FIRST_PAGE" | jq -r '.items[0].observed_at')
curl -sG "http://127.0.0.1:18080/api/v1/gpus/$GPU_ID/telemetry" \
  --data-urlencode "start_time=$OBSERVED_AT" --data-urlencode "end_time=$OBSERVED_AT"

# One exact stored row
EVENT_ID=$(printf '%s' "$FIRST_PAGE" | jq -r '.items[0].event_id')
curl -sG "http://127.0.0.1:18080/api/v1/gpus/$GPU_ID/telemetry" --data-urlencode "event_id=$EVENT_ID"

# Snapshot-stable pages
curl -s "http://127.0.0.1:18080/api/v1/gpus/$GPU_ID/telemetry?limit=100"
```

Each response row contains every source field plus `event_id`, `source_event_key`, `source_timestamp`, `observed_at`, `ingested_at`, dataset/cycle/row identity, streamer identity, and queue partition/offset. `source_timestamp` preserves the CSV value. `observed_at` is generated from the current UTC clock when the row is emitted, so each replay produces a new searchable observation while retries stay idempotent.

Interactive API documentation is at `/docs`; the committed generated contract is [docs/openapi.yaml](docs/openapi.yaml).

## Useful Make targets

```text
make help             list targets
make build            compile every executable
make lint             formatting, go vet, optional golangci-lint
make test             all unit tests with the race detector (100% must pass)
make coverage-check   enforce >=80% total across all internal packages
make integration      real PostgreSQL test in an ephemeral Docker container
make benchmark        durable queue performance smoke benchmark
make helm-lint        validate and render the Helm chart
make openapi-check    verify the committed API contract is current
make image            build the local image
make local-e2e        complete local-cluster acceptance test
make resilience-e2e   rerun scale-down/restart tests on the retained cluster
make scale            set streamer/collector replicas on the retained cluster
make logs             show recent component logs
make verify           lint, race tests, coverage, OpenAPI, Helm, and build
make cluster-down     delete only the named local test cluster
```

## Production scope

The implementation uses non-root/read-only application containers, resource requests/limits, health probes, graceful shutdown, Prometheus metrics, generated credentials that survive reinstall, persistent volume claims, serialized/versioned migrations, strict configuration validation, query time/size/concurrency limits, checksummed logs, producer fencing with in-cycle lease heartbeats, durable consumer offsets, and transactional database watermarks.

The custom queue is deliberately a single broker replica, as scoped for this exercise. Its PVC survives pod restarts, but there is no replicated broker quorum or cross-node failover. Fully consumed closed segments are reclaimed; the configured byte ceiling applies backpressure before unconsumed data can fill the volume. The persisted queue partition count is immutable so a Helm change cannot silently hide history. Expanding/archiving a queue volume remains an operator action. PostgreSQL is also deployed as one local instance for reproducible laptop testing. For a real multi-node environment, use managed/HA PostgreSQL and either add consensus/replication to the broker or replace it with an established queue.

Database retention is disabled by default because the API requirement is to return all persisted history. Operators can opt into bounded retention with `--set retention.enabled=true --set retention.maxAge=168h`; cleanup runs in bounded transactions through a Kubernetes CronJob. Size the PostgreSQL PVC for the chosen ingestion rate and retention window.

Every component exposes `/metrics`, `/healthz`, and `/readyz`. The API serves them on port 8080; queue, streamer, and collector serve them on port 9090. Metrics deliberately use fixed operation labels and never GPU, event, or pod IDs, avoiding unbounded cardinality.

See [architecture](docs/architecture.md), [queue semantics](docs/queue-design.md), and [testing/runbook](docs/testing.md) for the detailed decisions and failure behavior.

## AI assistance

This project was built with an AI coding agent throughout. The agent drafted the plan, the Go implementation, the tests, the packaging, and the local verification runs. Requirements ownership, architecture decisions, and acceptance criteria stayed with me, and several of them changed the design materially: timestamp semantics, full-history and exact-row query behaviour, cycle identity and deduplication, pagination snapshots, and the decision to build a real broker rather than dress up a database table.

The parts worth reading are the failures. Generated Helm templates rendered integers in scientific notation, a coverage gate was quietly measuring only its strongest packages, and a streamer scale-down silently dropped telemetry until live cluster testing caught it. [AI_USAGE.md](AI_USAGE.md) documents the prompts, what the agent got wrong, and how each problem was found and fixed.
