-- WikiPulse TimescaleDB schema

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Windowed stats (one row per page/editor per window)
CREATE TABLE IF NOT EXISTS window_stats (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    entity_type   TEXT NOT NULL,   -- 'page' or 'editor'
    entity_key    TEXT NOT NULL,   -- page title or username/IP
    edit_count    INT NOT NULL,
    revert_count  INT NOT NULL DEFAULT 0,
    anon_ratio    FLOAT,
    baseline_ewma FLOAT,
    baseline_var  FLOAT,
    z_score       FLOAT,
    PRIMARY KEY (window_start, entity_type, entity_key)
);

SELECT create_hypertable('window_stats', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_window_stats_entity
    ON window_stats (entity_type, entity_key, window_start DESC);

-- Bound storage growth in production: the dashboard only ever queries the last
-- couple of hours, so nothing older than a few days needs to stick around.
SELECT add_retention_policy('window_stats', INTERVAL '3 days', if_not_exists => TRUE);

-- Baseline state, keyed per entity, updated incrementally by the Spark job
-- via foreachBatch (v1 "simplest" approach from the brief: state lives here,
-- not in Spark's own state store).
CREATE TABLE IF NOT EXISTS entity_baseline (
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    ewma          FLOAT NOT NULL,
    variance      FLOAT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_key)
);

-- Flagged anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id            BIGSERIAL,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start  TIMESTAMPTZ NOT NULL,
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    metric        TEXT NOT NULL,    -- e.g. 'edit_rate', 'revert_rate'
    value         FLOAT NOT NULL,
    baseline      FLOAT NOT NULL,
    z_score       FLOAT NOT NULL,
    severity      TEXT NOT NULL,    -- 'low' / 'medium' / 'high'
    PRIMARY KEY (id, detected_at)
);

SELECT create_hypertable('anomalies', 'detected_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies (severity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_entity ON anomalies (entity_type, entity_key, detected_at DESC);

SELECT add_retention_policy('anomalies', INTERVAL '3 days', if_not_exists => TRUE);
