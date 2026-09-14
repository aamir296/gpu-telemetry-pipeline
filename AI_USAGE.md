# AI assistance and development workflow

This project was built with an AI coding agent throughout, and the record below is meant to be read as an honest account rather than a highlight reel.

The division of labour stayed consistent. I owned the requirements, the architecture decisions, and the acceptance criteria. The agent did the drafting: research, plan iterations, Go implementation, tests, packaging, and the local verification runs. Where the agent's output was wrong, I caught it either by reading the design critically or by running the thing on a real cluster, and those corrections are documented in their own section because they say more about the workflow than the successes do.

Public repositories addressing the same exercise were read as design input. No implementation was copied. Every pattern that survived was re-checked against the PDF and then tested locally.

## How the agent was driven

Four working patterns did most of the useful work, and they are worth naming because the output quality tracked them closely.

**Goal first, implementation later.** Every phase opened with a goal and its constraints rather than a task list. Stating the acceptance criteria up front ("statement coverage reported over all internal packages behind a Makefile gate") produced better results than describing steps, because it left the agent room to propose an approach I could then argue with.

**A plan loop before any code.** The architecture went through several revision rounds while the repository was still empty. Each round meant handing the plan out for independent critique, feeding the findings back, and requiring the agent to accept, modify, or reject each one with a justification tied to the PDF. Nothing was built until the plan stopped attracting blocking objections. This is where the queue design changed from a database table to a real broker, which would have been an expensive discovery after implementation.

**A build and verify loop.** Once building, the agent ran its own gates (`make lint`, `make test`, `make coverage-check`, `make integration`, `make helm-lint`) and iterated on failures without being prompted for each one. It also drove a real kind cluster, read pod logs, and corrected its own configuration from what it observed. That closed loop caught things no amount of review would have: the scientific-notation rendering bug and the PostgreSQL initialization failure were both found by the agent watching its own deployment fail.

**Review as a separate pass, deliberately adversarial.** Implementation review was run as its own exercise against a running system rather than as a code read, with instructions to reproduce claims rather than trust them. That is how the scale-down data loss surfaced, and also how several confidently-reported findings turned out to be wrong and were withdrawn.

The one guardrail that mattered throughout: commit before changing anything. Every review cycle started from a clean tree so that a fix attempt could always be compared against, or reverted to, a known state.

## How each part of the work was bootstrapped

**The repository and module layout** started from a single planning pass. Before any code existed, I had the agent produce a component boundary map and a delivery sequence, then reviewed it against the PDF line by line. The layout that emerged (`cmd/` entrypoints, `internal/` packages split by domain, `deploy/helm`, `scripts/`) came out of that plan rather than being grown incrementally, which is why the package boundaries hold up.

**The code** was written in vertical slices on my instruction. The first milestone was a thin end-to-end path (streamer to broker to collector to PostgreSQL to API) behind the final broker interface, using an in-memory queue. Only once that ran did the durable segment log replace it, with no changes to the service clients. This ordering was a deliberate choice: it meant there was always something demonstrable, and it kept the interface honest.

**Unit tests** were driven by asking for behaviour tables rather than test files. For each component I asked what could fail, then had the agent write tests against that list. The broker got frame codec round trips, torn-write truncation at every byte offset of a small synthetic segment, and restart recovery. The collector got its transaction ordering invariant. The API got inclusive boundary cases and cursor stability. Tests I asked for after finding a bug are noted below, since those are the ones that mattered most.

**The build and test environment** was set up by the agent under supervision, and this is where the most time was lost. Podman reported a healthy VM and then exited immediately, so rather than claim an unverified end-to-end result I had it install Docker Desktop user-locally and rerun the full kind workflow from an empty cluster. The `make doctor` preflight target exists because of that afternoon.

## Prompt log

These are the prompts that shaped the work, lightly edited for length. The mix is intentional: broad goal-setting early, narrow technical directives once the design was settled, and short approvals where the agent had already earned them.

**Requirements and framing**

> Read the exercise PDF and the DCGM CSV. Treat the PDF as the authoritative specification and the CSV as one example input, not a schema contract. List every explicit deliverable, then tell me which requirements are genuinely ambiguous before you propose anything.

> Confirm the timestamp rule with me. The CSV timestamp is provenance only. The API must order and filter on the time each row is processed. Keep both values on the record so nothing is discarded.

> The sample data repeats `gpu_id` 0 through 7 across 31 hosts, so it cannot be the key. Define a globally unique GPU identity that holds for any input file, with a documented fallback when no UUID column is present.

**Design constraints**

> The PDF requires a custom message queue. A relational table with row locking does not satisfy that requirement, it just relocates it. Design an actual broker: framed wire protocol, partitioned append-only log, explicit offsets, consumer groups, and backpressure. PostgreSQL stores telemetry only.

> Telemetry queries must return every retained row for a GPU across all prior cycles, ordered by time, with inclusive bounds on both ends. Add exact single-row retrieval. Bound the response so an unfiltered query cannot exhaust the API process.

> Ten streamers reading the same file will emit ten copies of every row. Decide how work is partitioned, make the assignment dynamic so it survives scaling, and tell me what happens to a shard when its owner disappears mid-pass.

> Sequence the delivery so a thin end-to-end path runs first behind the final broker interface, then deepen the broker without touching the service clients. I want something demonstrable at every phase boundary.

**Quality standards**

> Define a measurable test standard. Every test must pass, and statement coverage must be reported over all internal packages behind a Makefile gate. Do not select favourable packages to reach the threshold.

> Assume the reviewer is on macOS with Docker and has no context on this project. Design the Makefile and scripts so a clean clone reaches a verified end-to-end run in kind with one command. No hidden setup steps, no manual edits.

**Research and review**

> Survey public solutions to this exercise. Extract the patterns worth adopting and the specification violations to avoid. Design input only, no copied implementations.

> Two independent architecture reviews are attached. For each finding, decide accept, modify, or reject, and justify every rejection against the PDF specifically. Then give me the revised plan.

> Approved. Build it end to end in a new directory.

**Post-implementation corrections**

> Commit the current state before you touch anything. Then work through this review report and separate the real defects from the findings that are incorrect, with evidence either way.

> Live testing shows cycles left permanently incomplete after a streamer scale-down. Find the root cause in the cycle completion logic. Fix it so a membership change invalidates prior completion and forces replay under the new assignment. Add a unit test for both scaling directions and a cluster-level regression test that asserts exact per-cycle row counts.

> The coverage gate measures a hand-picked subset of packages, which is not an honest number. Point it at all internal packages and raise real coverage with tests for the collector, the broker client, and the storage layer. Keep the PostgreSQL integration suite reported separately so it cannot inflate the unit gate.

> The documented query examples reference an identifier that exists in neither dataset, so the sample workflow fails on first use. Rewrite it to discover a real GPU and event from the running API, and make the supplied 2,470 row file the default input for a clean clone.

## Where AI output fell short

None of the agent's output was accepted as correct on sight. These are the specific failures and what fixed them, roughly in the order they surfaced.

1. **Query semantics were read too narrowly at first.** The initial plan treated telemetry retrieval as a recent-window query. Clarifying that all retained history must be reachable, plus exact-row lookup, changed the API model and added an end-to-end assertion that at least two distinct cycles are visible.

2. **Prior art pointed in the wrong direction.** Several public solutions used Kustomize instead of Helm, or used a relational table as the queue. Both violate the specification. Independent review confirmed it, and the design moved to a framed TCP broker with a Helm-only deployment contract.

3. **Idempotency was initially coupled to producer identity.** The first deduplication key included the streamer instance, which would have permitted duplicates precisely when a shard changed hands mid-cycle. The key became dataset plus broker-owned cycle plus CSV row identity, which makes failover replay safe by construction.

4. **The first pagination design could silently drop rows.** Capturing a boundary from the maximum sequence value looked correct but ignored the gap between sequence allocation and commit visibility. The fix records a per-partition watermark inside the same transaction as the telemetry insert, then carries it in a versioned cursor.

5. **Generated Helm templates rendered large integers in scientific notation**, which strict Go parsing rejected at startup. Nothing in review caught this. The first real kind run did, and the templates now render integer strings.

6. **The PostgreSQL security context was too aggressive.** Dropping capabilities the official image needs to initialize a fresh volume left the pod failing on first install. Application containers stayed restricted; PostgreSQL was allowed its documented initialization behaviour.

7. **A field crossed the queue without being persisted.** A final audit found `assignment_generation` present on the wire and absent from the table. Fixed with a forward migration, insert and select changes, and stronger field assertions in the end-to-end suite.

8. **The container runtime lied about its own health.** Podman reported a successful VM start and stopped immediately. A separate test VM was tried and removed before switching to Docker Desktop and rerunning the whole workflow from an empty cluster.

9. **Coverage was measured on flattering terms.** The first implementation gated on its strongest packages. Independent review measured 66.7% across all internal packages, with the collector at zero. The gate now measures every internal package, and tests for the collector, broker client, and storage layer raised the real figure above the threshold.

10. **Scale-down lost telemetry, and nothing detected it.** Live review reproduced permanently truncated cycles after reducing streamer replicas. The cause was cycle completion being evaluated only against surviving members, so departing members' unfinished shards were abandoned as the cycle rolled over. Membership changes now clear prior completion, bump the assignment generation even when a single member remains, and force same-cycle replay. This one is worth dwelling on: the bug existed because the collector had no tests and nothing emitted metrics, so the pipeline lost data quietly. Both gaps were closed as part of the fix.

11. **The first fix exposed a second-order lease bug.** One streamer takes roughly 25 seconds to emit the supplied file, longer than the original 15 second membership lease. Its own next heartbeat could therefore expire and re-add it, manufacturing a membership change and replaying the same cycle forever. Streamers now heartbeat during a cycle, abort only on a genuine assignment change, and a renewal can never expire its own caller.

12. **Documentation drifted from reality.** Generated README examples used a fabricated GPU UUID, so every documented query returned nothing. They were replaced with a workflow that reads a real identifier from the live API.

## Where AI did the heavy lifting

- Repository and module layout, service entrypoints, configuration parsing and validation
- Binary telemetry codec and the framed queue protocol
- Partitioned segment persistence, CRC verification, torn-write recovery, deduplication, offset tracking, capacity backpressure
- Producer and consumer membership, assignment generations, rendezvous sharding, broker-owned replay cycles
- CSV decoding, timestamp separation, retry and backoff behaviour
- PostgreSQL schema and migrations, transactional collector persistence, query repository
- Type-driven Huma handlers and the generated OpenAPI contract
- Unit, corruption and recovery, integration, benchmark, and Kubernetes end-to-end tests
- Distroless image, Helm resources and health settings, Makefile, CI workflow, documentation

## Validation actually executed

Run, not inferred:

- `make lint`, covering `gofmt` and `go vet`, with `golangci-lint` when present
- `make test` under the race detector, all packages passing
- `make coverage-check`, reporting 80.7% across every internal package against an 80% gate
- `make integration` against an ephemeral PostgreSQL 17 container
- `make benchmark` on an Apple M3 Pro as a smoke measurement, not a throughput claim
- `make openapi-check` and `make helm-lint`
- clean container image build
- clean kind cluster and Helm install using the supplied 2,470 row CSV
- end-to-end result: 247 GPUs discovered, history retained across cycles, processing-time and source timestamps both present, complete field coverage, exact-row lookup, inclusive bounds, and snapshot-stable pagination verified
- resilience result: an abrupt five-to-one streamer scale-down replayed the interrupted cycle to its exact row count, collector and queue restarts resumed ingestion without loss, duplicate source keys stayed at zero, and all four services served Prometheus metrics

The failed cluster runs are documented here deliberately. They are the clearest evidence in this record that execution overruled plausible-looking generated configuration, which is the part of working with an AI agent that actually needs proving.
