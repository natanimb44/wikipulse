# Project: WikiPulse — Real-Time Wikipedia Edit Anomaly Detector

## 1. Vision

Build a production-grade, real-time streaming data pipeline that ingests Wikipedia's
live global edit feed, computes windowed statistics per page and per editor using
distributed stream processing, and flags anomalous edit behavior (vandalism spikes,
bot bursts, coordinated editing, edit wars) as it happens — surfaced on a live
public dashboard.

This is a portfolio project targeting Big Tech / Big Data engineering roles
(ML Engineer, Data Engineer, Backend/Distributed Systems). The point of the
project is to demonstrate real competence with:
- Event streaming (Kafka-protocol pub/sub)
- Distributed stream processing with windowed aggregation (Spark Structured Streaming)
- Time-series storage at scale
- Statistical anomaly detection on streaming data
- A deployed, live, end-to-end system (not just a notebook)

The narrative to build toward: "I built a scaled-down version of the kind of
real-time trust & safety / platform integrity system that Meta, Reddit, or
TikTok run to detect abuse as it happens."

Prioritize correctness and a genuinely working end-to-end pipeline over
model sophistication. A simple z-score anomaly detector running on a real,
live, distributed pipeline is more impressive than a fancy model that only
runs on a static CSV.

## 2. Data Source

Wikipedia publishes every edit across all language editions in real time via
Server-Sent Events (SSE), no authentication required:

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- Protocol: SSE (`text/event-stream`), one JSON object per edit event
- Volume: typically several hundred to a few thousand events per minute globally
- Docs: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

Each event JSON includes (field names approximate — verify against live stream):
- `title` (page title)
- `user` (username or IP string)
- `bot` (boolean — self-reported bot flag)
- `anon` / whether `user` looks like an IP address (used as anonymous-edit proxy)
- `timestamp` (unix epoch seconds)
- `type` (edit / new / log / categorize — filter to `edit` and `new` only)
- `wiki` (which wiki, e.g. `enwiki` — filter to `enwiki` only for v1 to bound volume)
- `length.old` / `length.new` (byte diff — compute `abs(new - old)`)
- `revision.old` / `revision.new` (revision IDs)
- `comment` (edit summary — reverts often contain "revert"/"undo" in this field)
- `server_url`, `namespace` (filter to `namespace == 0`, i.e. main article namespace only, to reduce noise from talk/user pages)

## 3. Architecture

```
[Wikipedia EventStreams SSE]
        |
        v
[Python Producer Service]  --publishes JSON-->  [Redpanda topic: wiki-edits]
        |
        v
[Spark Structured Streaming Job]
   - reads from Redpanda (Kafka-compatible consumer)
   - parses + filters events
   - computes windowed aggregations (tumbling 1-min windows, watermark 2 min):
       - edits_per_page
       - edits_per_editor
       - revert_count_per_page (comment contains "revert"/"undo")
       - anon_edit_ratio_per_page
   - computes rolling baseline per page/editor (EWMA over prior windows)
   - flags anomalies where current window >> baseline (z-score or ratio threshold)
   - writes:
       a) windowed aggregates -> TimescaleDB `window_stats` table
       b) flagged anomalies -> TimescaleDB `anomalies` table
        |
        v
[TimescaleDB (Postgres + timeseries extension)]
        |
        v
[FastAPI backend] -- serves REST endpoints, reads from TimescaleDB
        |
        v
[React (or React Native/Expo, matching PropMetrics stack) dashboard]
   - live anomaly feed (polling or WebSocket)
   - spike chart for a selected page
   - "hot right now" flagged pages list
```

## 4. Tech Stack (all containerized via Docker Compose for local dev)

- **Redpanda** (single-node, Kafka-API compatible) — message broker, replaces raw Kafka+Zookeeper to avoid ops overhead
- **Apache Spark** (Structured Streaming, Python/PySpark) — stream processing
- **TimescaleDB** (Postgres 15 + Timescale extension) — storage for windowed stats and anomalies
- **FastAPI** (Python) — REST API layer, same stack as PropMetrics
- **React** (or React Native/Expo web build, to reuse PropMetrics frontend patterns) — dashboard
- **Docker Compose** — local orchestration of all services
- **Deployment targets** (Phase 5, later): Railway for backend/Spark job, Vercel for frontend — same pattern as PropMetrics

## 5. Dev Environment

The developer does not have admin/install rights on their local machine, so
all Docker-based work happens in **GitHub Codespaces**, not locally.

- The project repo lives on GitHub; a Codespace is launched from it, which
  boots a remote Linux VM with Docker, Node.js, Python, and git pre-installed
  — no local installation of anything is required.
- Development happens through the **VS Code desktop app** connected to the
  Codespace via the GitHub Codespaces extension (or the browser-based VS Code
  if the desktop app can't be installed either).
- **Claude Code runs inside the Codespace terminal itself** (installed via
  `npm install -g @anthropic-ai/claude-code`), so it has direct access to
  run `docker compose`, inspect container logs, query TimescaleDB, and
  iterate on the pipeline exactly as if it had a normal local dev machine —
  because from its point of view, it does.
- All `docker-compose up` / `docker compose` commands referenced throughout
  this brief should be run from that Codespace terminal.
- Ports (FastAPI, dashboard dev server, Spark UI, etc.) are auto-forwarded by
  Codespaces and accessible via the **Ports** tab in VS Code.
- Be mindful of Codespaces' free-tier monthly core-hour limit — stop the
  Codespace when not actively working rather than leaving it running idle.

## 6. Repo Structure

```
wikipulse/
├── docker-compose.yml
├── README.md
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py          # SSE consumer -> Redpanda publisher
├── spark-job/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── stream_processor.py  # Spark Structured Streaming job
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py               # FastAPI app
│   ├── models.py             # Pydantic + DB models
│   └── db.py                 # TimescaleDB connection
├── frontend/
│   └── ...                   # React app (Vite) or Expo web
├── db/
│   └── init.sql              # TimescaleDB schema + hypertable setup
└── scripts/
    └── load_test.py          # measures throughput/latency for the README
```

## 7. Database Schema (TimescaleDB)

```sql
-- Raw-ish windowed stats (one row per page/editor per window)
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

-- Flagged anomalies
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

## 8. Anomaly Detection Logic (v1 — statistical, no ML model yet)

For each entity (page or editor) and each 1-minute tumbling window:
1. Maintain an EWMA baseline of edit_count with smoothing factor alpha (e.g. 0.3),
   updated incrementally window-over-window, keyed by entity.
2. Compute `z_score = (current_count - ewma_baseline) / rolling_stddev`
   (rolling_stddev also maintained via an incremental/Welford-style update, or
   approximate with a fixed lookback window if simpler to implement first).
3. Flag as anomaly if `z_score > 3` (tune threshold empirically after observing
   real traffic for a day).
4. Bucket severity: z 3-4 = low, 4-6 = medium, 6+ = high.
5. Separately flag `revert_count / edit_count > 0.5` within a window as an
   "edit war" signal regardless of z-score, since revert ratio spikes are
   meaningful even on already-high-traffic pages.

State for baselines must be maintained across streaming batches — use Spark's
`mapGroupsWithState` / `flatMapGroupsWithState` (or `applyInPandasWithState` in
newer PySpark) keyed by entity_key, OR maintain baselines in an external store
(simplest v1: read/write baseline state to/from TimescaleDB each batch via
foreachBatch — slower but much simpler to implement correctly first).

**Build order recommendation for Claude Code**: implement the foreachBatch +
TimescaleDB-read-baseline approach first (simpler, correct), then optimize to
Spark stateful streaming (`mapGroupsWithState`) as a v2 if time allows — this
progression itself is a good story for interviews ("started simple, then
optimized for lower latency/state management").

## 9. FastAPI Endpoints (v1)

- `GET /anomalies/recent?limit=50` — most recent flagged anomalies, newest first
- `GET /anomalies/severity/{level}` — filter by severity
- `GET /stats/page/{title}?minutes=60` — windowed stats time series for one page
- `GET /stats/global?minutes=60` — global edit-rate time series (for an overview chart)
- `GET /health` — basic healthcheck (confirms DB connectivity)

## 10. Frontend (v1 dashboard)

- **Live anomaly feed**: auto-refreshing list (poll every 5-10s) of recent
  flagged anomalies — page/editor name, metric, severity badge, time
- **Spike chart**: click an anomaly to see a line chart of edit_count over
  time for that page, with the baseline overlaid, so the spike is visually obvious
- **Global activity chart**: total edits/minute across all of English Wikipedia,
  as a "pulse" visualization
- Reuse component/style patterns from PropMetrics where sensible for speed

## 11. Build Phases & Acceptance Criteria

**Phase 1 — Ingestion**
Done when: Redpanda topic `wiki-edits` shows a continuous live stream of
JSON messages when inspected via `rpk topic consume wiki-edits` (Redpanda's
CLI) or a simple Python consumer script.

**Phase 2 — Stream Processing**
Done when: `window_stats` table in TimescaleDB is populated continuously
with fresh rows every ~1 minute while the Spark job runs, for both `page`
and `editor` entity types.

**Phase 3 — Anomaly Detection**
Done when: `anomalies` table populates with plausible flagged events during
a real burst (test this by watching a currently-trending Wikipedia page, or
by temporarily lowering the z-score threshold to force test flags).

**Phase 4 — Serving + Dashboard**
Done when: dashboard shows a live-updating anomaly feed and at least one
working spike chart, running locally end-to-end via `docker-compose up`.

**Phase 5 — Deployment + Polish**
Done when: system is deployed (Railway backend/Spark, Vercel frontend) and
publicly viewable at a URL, and README includes measured throughput/latency
numbers from `scripts/load_test.py` plus an architecture diagram.

## 12. Non-Goals (for v1 — explicitly defer to avoid scope creep)

- Multi-language wiki support (English Wikipedia only for v1)
- ML-based anomaly detection (Isolation Forest etc.) — statistical z-score
  approach is sufficient for v1 and should ship first
- User authentication on the dashboard (public read-only is fine)
- Historical backfill / replay of past edits — live stream only

## 13. What to hand back for review

After each phase, report: what was built, how it was verified (exact commands
run and their output), any deviations from this spec and why, and what's
blocking the next phase if anything.