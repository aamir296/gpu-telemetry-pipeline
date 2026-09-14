CREATE TABLE IF NOT EXISTS gpus (
    gpu_key         text PRIMARY KEY,
    uuid            text NOT NULL DEFAULT '',
    hostname        text NOT NULL,
    gpu_id          text NOT NULL,
    device          text NOT NULL,
    model_name      text NOT NULL,
    queue_partition integer NOT NULL,
    first_seen      timestamptz NOT NULL,
    last_seen       timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS telemetry_events (
    ingest_seq            bigserial PRIMARY KEY,
    event_id              text NOT NULL,
    source_event_key      text NOT NULL UNIQUE,
    queue_partition       integer NOT NULL,
    queue_offset          bigint NOT NULL,
    source_timestamp      timestamptz NOT NULL,
    observed_at           timestamptz NOT NULL,
    ingested_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    metric_name           text NOT NULL,
    gpu_key               text NOT NULL REFERENCES gpus(gpu_key),
    gpu_id                text NOT NULL,
    device                text NOT NULL,
    uuid                  text NOT NULL DEFAULT '',
    model_name            text NOT NULL,
    hostname              text NOT NULL,
    container_name        text NOT NULL DEFAULT '',
    pod_name              text NOT NULL DEFAULT '',
    namespace_name        text NOT NULL DEFAULT '',
    metric_value          double precision NOT NULL,
    labels_raw            text NOT NULL,
    dataset_id            text NOT NULL,
    cycle_id              bigint NOT NULL,
    row_number            bigint NOT NULL,
    streamer_id           text NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_gpu_time_idx
    ON telemetry_events (gpu_key, observed_at, ingest_seq);
CREATE INDEX IF NOT EXISTS telemetry_observed_at_idx
    ON telemetry_events (observed_at);
CREATE INDEX IF NOT EXISTS telemetry_event_id_idx
    ON telemetry_events (event_id);

CREATE TABLE IF NOT EXISTS telemetry_partition_watermarks (
    queue_partition integer PRIMARY KEY,
    highest_offset  bigint NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING;
