# Architecture and requirement mapping

## Data path

```text
CSV -> streamer replicas -> custom TCP queue -> collector replicas -> PostgreSQL -> HTTP API
          current UTC          durable log       transaction          history/query
          GPU sharding         partitions        then offset commit   exact row
```

The queue is an application component, not a PostgreSQL table used as a disguised broker. PostgreSQL is solely the queryable telemetry store.

| Requirement | Implementation |
|---|---|
| Repeated CSV telemetry | Broker-owned numbered cycles; a new cycle starts only after all active owners complete and the interval floor has elapsed |
| Current timestamps | Streamer sets `observed_at = clock.Now().UTC()` immediately before batching each owned row |
| Preserve input | CSV timestamp remains `source_timestamp`; all named DCGM columns and raw labels are stored and returned |
| Multiple streamers | Membership generations fence stale writers; rendezvous hashing assigns a GPU to exactly one active streamer per cycle |
| Custom queue | Versioned CRC32C-framed TCP protocol and compact binary telemetry data plane |
| Message durability | Partitioned append-only segment files; publication acknowledged only after `fsync` |
| Multiple collectors | Consumer group membership assigns queue partitions; offsets commit only after the database transaction succeeds |
| Open-source database | PostgreSQL 17 in the local Helm release; external PostgreSQL accepted through the secret interface |
| All previous matches | An omitted `limit` reads all matching persisted cycles up to explicit row/byte safety ceilings |
| Specific row | `event_id` filter returns the immutable event; `source_event_key` remains the retry identity |
| Time range | Inclusive RFC3339Nano `start_time` / `end_time` against dynamic `observed_at` |
| Local Kubernetes | Helm chart plus a tested kind-first and parameterized minikube workflow |

## Identity and timestamp model

- GPU key: `uuid` when supplied, otherwise `hostname/gpu_id`.
- `source_event_key`: SHA-256 of dataset ID, broker cycle ID, one-based CSV row number, and normalized row hash. It is stable across retries and broker restart replay, but different in the next cycle.
- `event_id`: generated once by the broker for a new accepted record and used for exact lookup.
- `source_timestamp`: immutable time supplied in the CSV.
- `observed_at`: current UTC time at streamer emission. This is the API's time-filter dimension.
- `ingested_at`: PostgreSQL server time when the row is first inserted.

This separation is what permits old example CSV data to simulate live events without losing source provenance.

## Delivery and consistency

Delivery is at least once between queue and collector. Effective storage is exactly once for a logical source event because `source_event_key` is the database uniqueness boundary. A collector transaction inserts rows, updates each affected partition watermark, and commits. Only then does it commit next offsets to the queue. A crash in between repeats the delivery, and the uniqueness constraint makes the database write harmless.

API pages are stable while new events arrive. The first query captures the single queue-partition watermark for that GPU. The signed-by-structure opaque cursor carries the cursor schema version, partition, fixed watermark, last observation timestamp, and ingest sequence. Later pages keep the same watermark and order by `(observed_at, ingest_seq)`.

## Failure behavior

- Streamer crash or scale-down: streamers renew their producer lease during long cycles. Every real membership transition creates a new generation, invalidates completion from the old assignment, and makes surviving members abort and replay the same cycle. Already accepted rows deduplicate; the cycle cannot advance while departed shards are incomplete.
- Queue pod restart: segment indexes and dedup keys are rebuilt; a torn final record is truncated; checksum corruption in a complete record fails startup rather than silently dropping data.
- Collector/DB failure: queue offsets do not advance. Consumption resumes from the last durable commit.
- Offset-commit failure after DB commit: the batch is delivered again and is idempotent.
- API/DB failure: readiness fails and queries return problem-details errors; writes and ingestion are isolated.
- Capacity reached: new publications receive an error and streamers retry with bounded exponential backoff; duplicates remain admissible.
- Partition-count change: the broker fails startup with the stored and configured counts instead of silently reinterpreting offsets or hiding history.

## Scaling boundaries

Streamer and collector Deployments may be scaled horizontally. API replicas are stateless. Queue partitions set the maximum useful collector parallelism. The broker StatefulSet is schema-constrained to one replica because this implementation has durable restart recovery but no consensus protocol. PostgreSQL's local StatefulSet is for evaluation; its secret contract permits replacing application database URLs with an external service.
