# WikiPulse: how I designed this

## Why I built this

I wanted a portfolio project that actually proves I can work with real time data — not another CRUD app, and not a notebook full of pandas that only ever ran once on a CSV I downloaded. So the goal was to ingest Wikipedia's live edit stream, run it through real distributed stream processing, and flag suspicious edit behavior — vandalism spikes, bot bursts, coordinated editing, edit wars — while it's actually happening, live, on a dashboard anyone can pull up.

Basically a scaled-down version of the abuse detection systems platforms like Meta, Reddit, or TikTok run internally. I cared a lot more about having a real, working, end to end pipeline than about the anomaly detection itself being clever. A dumb z-score check running on live data is worth more to me than a fancy model that only ever worked on a static CSV once.

## The data

Wikipedia streams every edit across every language edition, live, no auth needed:

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- It's server-sent events (SSE), one JSON object per edit
- Volume is usually a few hundred to a couple thousand events a minute globally
- Docs, if you want to dig in: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

Fields I actually care about from each event (double checked these against the live stream since the docs are a little loose in places):
- `title` — page title
- `user` — username, or an IP if the edit's anonymous
- `bot` — self-reported bot flag
- whether `user` looks like an IP address, which I use as a stand-in for "anonymous edit"
- `timestamp`
- `type` — I only keep `edit` and `new`, there's also log/categorize events I don't care about
- `wiki` — filtered to `enwiki` only for now, to keep the volume manageable
- `length.old` / `length.new` — I take the absolute diff
- `revision.old` / `revision.new`
- `comment` — reverts usually have "revert" or "undo" somewhere in the edit summary
- `namespace` — filtered to `0` (main articles only), to cut out talk page and user page noise

## Architecture

```
[Wikipedia EventStreams SSE]
        |
        v
[Python Producer Service]  --publishes JSON-->  [Redpanda topic: wiki-edits]
        |
        v
[Spark Structured Streaming Job]
   - reads from Redpanda (Kafka-compatible consumer)
   - parses and filters events
   - windowed aggregations (tumbling 1-min windows, 2 min watermark):
       - edits_per_page
       - edits_per_editor
       - revert_count_per_page (comment contains "revert"/"undo")
       - anon_edit_ratio_per_page
   - rolling baseline per page/editor (EWMA over prior windows)
   - flags anomalies where the current window blows past the baseline
   - writes:
       a) windowed aggregates -> TimescaleDB `window_stats` table
       b) flagged anomalies -> TimescaleDB `anomalies` table
        |
        v
[TimescaleDB (Postgres + timeseries extension)]
        |
        v
[FastAPI backend] serving REST endpoints off TimescaleDB
        |
        v
[React dashboard]
   - live anomaly feed
   - spike chart for a selected page
   - "hot right now" flagged pages list
```

## Stack

Everything's containerized with Docker Compose for local dev:

- **Redpanda** (single node, speaks the Kafka API) as the message broker. Picked this over raw Kafka + Zookeeper mostly to avoid the ops headache of running my own Zookeeper.
- **Apache Spark** (Structured Streaming, PySpark) for the stream processing.
- **TimescaleDB** (Postgres 15 with the Timescale extension) for windowed stats and anomalies.
- **FastAPI** for the REST layer.
- **React** (Vite) for the dashboard.
- **Docker Compose** to run all of it locally.
- Deployed later to Railway (backend/Spark) and Vercel (frontend).

## Dev setup

I don't have admin rights on my own machine, so I did all of this in GitHub Codespaces instead of locally. The repo lives on GitHub; spinning up a Codespace gives me a full Linux VM with Docker, Node, Python, and git already set up — no local installs needed.

I worked through the VS Code desktop app connected to the Codespace. Every `docker compose` command in the README assumes you're running it from inside that Codespace terminal. Ports get auto-forwarded and show up under the Ports tab in VS Code.

One thing worth knowing: Codespaces' free tier has a monthly core-hour limit, so I got in the habit of stopping the Codespace any time I wasn't actively working on it instead of leaving it running in the background.

## Database schema

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- one row per page/editor per window
CREATE TABLE window_stats (
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
SELECT create_hypertable('window_stats', 'window_start');
SELECT add_retention_policy('window_stats', INTERVAL '3 days');

-- baseline state per entity, updated incrementally each micro-batch
CREATE TABLE entity_baseline (
    entity_type   TEXT NOT NULL,
    entity_key    TEXT NOT NULL,
    ewma          FLOAT NOT NULL,
    variance      FLOAT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_key)
);

-- flagged anomalies
CREATE TABLE anomalies (
    id           BIGSERIAL,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start TIMESTAMPTZ NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_key   TEXT NOT NULL,
    metric       TEXT NOT NULL,    -- e.g. 'edit_rate', 'revert_rate'
    value        FLOAT NOT NULL,
    baseline     FLOAT NOT NULL,
    z_score      FLOAT NOT NULL,
    severity     TEXT NOT NULL,    -- 'low' / 'medium' / 'high' bucketed from z_score
    PRIMARY KEY (id, detected_at)
);
SELECT create_hypertable('anomalies', 'detected_at');
SELECT add_retention_policy('anomalies', INTERVAL '3 days');
```

`entity_baseline` is where the EWMA/variance state actually lives between batches — it's not in the original design doc I sketched out before building this, but I split it out from `window_stats` once I realized I needed somewhere to persist state that wasn't just "the last row I wrote," and a separate table made the read-then-update logic in `foreachBatch` a lot easier to reason about.

## How the anomaly detection actually works

Nothing fancy, just statistics — no ML model, at least not yet. For each page or editor, in every 1-minute tumbling window:

1. Keep an EWMA baseline of `edit_count` per entity, smoothing factor around 0.3, updated window over window.
2. Track rolling variance for that same entity the same way (EWMA of squared deviation from the baseline, not a separate pass), and compute `z_score = (current_count - ewma_baseline) / max(sqrt(rolling_variance), 1.0)`. The floor on the denominator keeps a brand-new entity's z-score from blowing up before it has any real history.
3. Flag it as an anomaly if `abs(z_score) > 3`. I picked 3 as a starting point and figured I'd tune it after watching real traffic for a while — haven't gone back and revisited it yet.
4. Bucket severity off the z-score: 3–4 is low, 4–6 is medium, 6+ is high.
5. Separately, flag anything where `revert_count / edit_count > 0.5` in a window as an "edit war," regardless of z-score — a high revert ratio matters even on a page that's already busy enough that its edit count alone wouldn't look unusual.

The baseline state has to persist across streaming batches somehow. I went with reading and writing it to TimescaleDB each batch through `foreachBatch`, instead of Spark's native stateful streaming operators (`mapGroupsWithState` / `applyInPandasWithState`), because I wanted the state management to be something I could query and verify by hand while I was still checking the logic was even correct. Slower, but a lot easier to trust. Switching to Spark's native state store would be the natural next optimization.

## API endpoints

- `GET /anomalies/recent?limit=50` — most recent flagged anomalies
- `GET /anomalies/severity/{level}` — filter by severity
- `GET /stats/page/{title}?minutes=60` — time series for one page
- `GET /stats/editor/{username}?minutes=60` — time series for one editor
- `GET /stats/global?minutes=60` — global edit rate time series for the overview chart
- `GET /stats/trending?minutes=10&limit=8` — most-edited pages right now
- `GET /health` — basic healthcheck

## Frontend

- A live anomaly feed that polls every 5–10 seconds
- A spike chart — click an anomaly, see `edit_count` over time for that page with the baseline overlaid so the spike is obvious
- A global "pulse" chart showing total edits per minute across English Wikipedia

## How I built it, phase by phase

**Phase 1, ingestion.** Done when the `wiki-edits` Redpanda topic showed a continuous stream of JSON messages (checked with `rpk topic consume wiki-edits`).

**Phase 2, stream processing.** Done when `window_stats` filled up with fresh rows roughly every minute, for both pages and editors.

**Phase 3, anomaly detection.** Done when `anomalies` actually populated with reasonable-looking flags during a real burst. I tested this by watching a page that happened to be trending at the time, and separately by temporarily lowering the z-score threshold to force some flags through so I could check the plumbing worked end to end.

**Phase 4, serving and dashboard.** Done when the dashboard showed a live-updating feed and at least one working spike chart, all running locally through `docker-compose up`.

**Phase 5, deployment.** Done when it was actually live somewhere public — Railway for the backend and pipeline, Vercel for the frontend.

## What I deliberately skipped for v1

- Other language wikis — English only for now
- Any ML-based anomaly detection — plain statistics are good enough to start, and I'd rather get the pipeline right first
- Auth on the dashboard — it's public and read-only anyway
- Historical backfill or replay — live stream only, no way to go back and reprocess old data
