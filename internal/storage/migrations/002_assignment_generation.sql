ALTER TABLE telemetry_events
    ADD COLUMN IF NOT EXISTS assignment_generation bigint NOT NULL DEFAULT 0;

INSERT INTO schema_migrations(version) VALUES (2) ON CONFLICT DO NOTHING;
