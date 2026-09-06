# WikiPulse: how I designed this

## Why I built this

I wanted a portfolio project that actually proves I can work with real time data, not just another CRUD app or a notebook full of pandas. So the goal here was to ingest Wikipedia's live edit stream, run it through some actual distributed stream processing, and flag suspicious edit behavior (vandalism spikes, bot bursts, coordinated editing, edit wars) as it's happening, live, on a public dashboard.

Basically I wanted something close to a scaled down version of the abuse detection systems that platforms like Meta, Reddit, or TikTok run internally. I care more about having a real, working, end to end pipeline than about the anomaly detection being fancy. A dumb z-score check running on live data beats a sophisticated model that only works on a static CSV I downloaded once.

## The data

Wikipedia streams every single edit across every language edition, live, with no auth needed:

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- It's server sent events (SSE), one JSON object per edit
- Volume is usually a few hundred to a couple thousand events a minute globally
- Docs are here if you want to dig in: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

Fields I care about from each event (I double checked these against the live stream since the docs are a little loose):
- `title` (page title)
- `user` (username, or an IP if it's anonymous)
- `bot` (self reported bot flag)
- whether `user` looks like an IP, which I use as a stand in for "anonymous edit"
- `timestamp`
- `type` (I only keep `edit` and `new`, there's also log/categorize events I don't care about)
- `wiki` (I filter to `enwiki` only for now to keep the volume manageable)
- `length.old` / `length.new`, I take the absolute diff
- `revision.old` / `revision.new`
- `comment`, since reverts usually have "revert" or "undo" in the edit summary
- `namespace`, filtered to `0` (main articles only) to cut out talk page and user page noise

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

- **Redpanda** (single node, speaks the Kafka API) as the message broker. I picked this over raw Kafka + Zookeeper mostly to avoid the ops headache.
- **Apache Spark** (Structured Streaming, PySpark) for the stream processing.
- **TimescaleDB** (Postgres 15 with the Timescale extension) to store windowed stats and anomalies.
- **FastAPI** for the REST layer.
- **React** (Vite) for the dashboard.
- **Docker Compose** to run all of it locally.
- Deployed later to Railway (backend/Spark) and Vercel (frontend).

## Dev setup

I don't have admin rights on my own machine, so I did all of this in GitHub Codespaces instead of locally. The repo lives on GitHub, spinning up a Codespace gives me a full Linux VM with Docker, Node, Python, and git already set up, no local installs needed.

I worked through the VS Code desktop app connected to the Codespace. All the `docker compose` commands in the README assume you're running them from inside that Codespace terminal. Ports get auto forwarded and show up under the Ports tab in VS Code.

One thing to watch out for: Codespaces free tier has a monthly core hour limit, so I made a habit of stopping the Codespace whenever I wasn't actively working instead of leaving it running.

## Database schema

```sql
-- one row per page/editor per window
CREATE TABLE window_stats (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    entity_type  TEXT NOT NULL,   -- 'page' or 'editor'
    entity_key   TEXT NOT NULL,   -- page title or username/IP
    edit_count   INT NOT NULL,
    revert_count INT NOT NULL DEFAULT 0,
    anon_ratio   FLOAT,
    baseline_ewma FLOAT,
    z_score      FLOAT
);
SELECT create_hypertable('window_stats', 'window_start');

-- flagged anomalies
CREATE TABLE anomalies (
    id           BIGSERIAL PRIMARY KEY,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start TIMESTAMPTZ NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_key   TEXT NOT NULL,
    metric       TEXT NOT NULL,    -- e.g. 'edit_rate', 'revert_rate'
    value        FLOAT NOT NULL,
    baseline     FLOAT NOT NULL,
    z_score      FLOAT NOT NULL,
    severity     TEXT NOT NULL     -- 'low' / 'medium' / 'high' bucketed from z_score
);
SELECT create_hypertable('anomalies', 'detected_at');
```

## How the anomaly detection actually works

Nothing fancy here, just statistics, no ML model (yet). For each page or editor, in every 1 minute tumbling window:

1. Keep an EWMA baseline of edit_count per entity, smoothing factor around 0.3, updated window over window.
2. `z_score = (current_count - ewma_baseline) / rolling_stddev`, with the rolling stddev also updated incrementally (Welford style, or a simpler fixed lookback if that's easier to get right first).
3. Flag it as an anomaly if `z_score > 3`. I plan to tune this threshold after watching real traffic for a day or so.
4. Bucket severity: z of 3 to 4 is low, 4 to 6 is medium, 6+ is high.
5. Separately, flag anything where `revert_count / edit_count > 0.5` in a window as an "edit war" regardless of z-score, since a high revert ratio matters even on pages that are already busy.

The baseline state has to persist across streaming batches. I went with reading and writing it to TimescaleDB each batch through `foreachBatch`, since Spark's native stateful streaming operators (`mapGroupsWithState` etc.) would've been faster but harder to get right on the first try. Started simple and correct, can optimize later.

## API endpoints

- `GET /anomalies/recent?limit=50`, most recent flagged anomalies
- `GET /anomalies/severity/{level}`, filter by severity
- `GET /stats/page/{title}?minutes=60`, time series for one page
- `GET /stats/global?minutes=60`, global edit rate time series for the overview chart
- `GET /health`, basic healthcheck

## Frontend

- A live anomaly feed that polls every 5 to 10 seconds
- A spike chart, click an anomaly and see edit_count over time for that page with the baseline overlaid so the spike is obvious
- A global "pulse" chart showing total edits per minute across English Wikipedia

## How I built it, phase by phase

**Phase 1, ingestion.** Done when the `wiki-edits` Redpanda topic shows a continuous stream of JSON messages (checked this with `rpk topic consume wiki-edits`).

**Phase 2, stream processing.** Done when `window_stats` fills up with fresh rows roughly every minute, for both pages and editors.

**Phase 3, anomaly detection.** Done when `anomalies` actually populates with reasonable looking flags during a real burst. I tested this by watching a page that was trending at the time, and also by temporarily lowering the z-score threshold to force some flags through.

**Phase 4, serving and dashboard.** Done when the dashboard shows a live updating feed and at least one working spike chart, all running locally through `docker-compose up`.

**Phase 5, deployment.** Done when it's actually live somewhere public (Railway for the backend, Vercel for the frontend) and the README has real throughput and latency numbers from the load test script plus a diagram.

## What I deliberately skipped for v1

- Other language wikis, English only for now
- Any ML based anomaly detection, statistics are good enough to start
- Auth on the dashboard, it's public read only anyway
- Historical backfill or replay, live stream only
