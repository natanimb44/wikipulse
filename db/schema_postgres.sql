-- WikiPulse schema for plain Postgres (the hosted, batch-mode deployment).
-- Same tables as db/init.sql minus the TimescaleDB-only parts: no hypertables,
-- and retention is handled by batch/ingest.py deleting old rows each run.
-- Run once against the new database:  psql "$DATABASE_URL" -f db/schema_postgres.sql

CREATE TABLE IF NOT EXISTS window_stats (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    entity_type   TEXT NOT NULL,   -- 'page' or 'editor'
    entity_key    TEXT NOT NULL,   -- page title or username / temp account
    edit_count    INT NOT NULL,
    revert_count  INT NOT NULL DEFAULT 0,
    anon_ratio    FLOAT,
    baseline_ewma FLOAT,
    baseline_var  FLOAT,
    z_score       FLOAT,
    PRIMARY KEY (window_start, entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_window_stats_entity
    ON window_stats (entity_type, entity_key, window_start DESC);

CREATE TABLE IF NOT EXISTS entity_baseline (
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    ewma          FLOAT NOT NULL,
    variance      FLOAT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_entity_baseline_updated ON entity_baseline (updated_at);

CREATE TABLE IF NOT EXISTS anomalies (
    id            BIGSERIAL PRIMARY KEY,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start  TIMESTAMPTZ NOT NULL,
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    metric        TEXT NOT NULL,    -- 'edit_rate' or 'revert_rate'
    value         FLOAT NOT NULL,
    baseline      FLOAT NOT NULL,
    z_score       FLOAT NOT NULL,
    severity      TEXT NOT NULL     -- 'low' / 'medium' / 'high'
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies (severity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_entity ON anomalies (entity_type, entity_key, detected_at DESC);

-- One row per batch run: the watermark for the next run, and a record of what each run did
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_from  TIMESTAMPTZ NOT NULL,
    window_to    TIMESTAMPTZ NOT NULL,
    edits        INT NOT NULL,
    anomalies    INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_window_to ON ingest_runs (window_to DESC);
