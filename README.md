# WikiPulse

Real-time Wikipedia edit anomaly detector — a streaming pipeline that ingests
Wikipedia's live global edit feed, computes windowed per-page and per-editor
statistics with Spark Structured Streaming, and flags anomalous edit behavior
(vandalism spikes, bot bursts, edit wars) as it happens on a live dashboard.

**Live demo:** https://wikipulse-six.vercel.app
**API:** https://backend-production-1064.up.railway.app/docs

See [project_brief.md](project_brief.md) for the full design.

## Architecture

```
Wikipedia EventStreams (SSE)
        |
Python producer  -->  Redpanda topic `wiki-edits`
        |
Spark Structured Streaming (1-min tumbling windows, 2-min watermark)
   - EWMA baseline + z-score anomaly detection per page/editor
        |
TimescaleDB (window_stats, anomalies, entity_baseline)
        |
FastAPI  -->  React dashboard
```

## Running locally (Codespaces)

```
docker compose up --build
```

Services and forwarded ports:

| Service     | Port | Purpose                              |
|-------------|------|---------------------------------------|
| redpanda    | 9092 | Kafka-protocol broker                 |
| redpanda    | 9644 | Admin API                             |
| timescaledb | 5432 | Postgres + Timescale                  |
| backend     | 8000 | FastAPI (`/docs` for OpenAPI UI)      |
| spark-job   | 4040 | Spark UI (while a query is running)   |
| frontend    | 5173 | React dashboard (Vite dev server)     |

## Verifying each phase

**Phase 1 — ingestion**
```
docker exec -it redpanda rpk topic consume wiki-edits -n 5
```

**Phase 2 — stream processing**
```
docker exec -it timescaledb psql -U wikipulse -d wikipulse \
  -c "SELECT * FROM window_stats ORDER BY window_start DESC LIMIT 10;"
```

**Phase 3 — anomaly detection**
```
docker exec -it timescaledb psql -U wikipulse -d wikipulse \
  -c "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 10;"
```
To force test flags quickly, lower `Z_THRESHOLD` in `docker-compose.yml` (e.g. to `1.0`)
and restart the `spark-job` service.

**Phase 4 — dashboard**

Open the forwarded port-5173 URL from the Codespaces **Ports** tab.

## Load test

From the Codespace terminal (after the stack has been running a few minutes so
`window_stats` has data):

```
pip install -r scripts/requirements.txt
python scripts/load_test.py --duration 60
```

<!-- Throughput/latency numbers from a real run go here once collected. -->

## Repo structure

```
wikipulse/
├── docker-compose.yml
├── producer/         # SSE consumer -> Redpanda publisher (local dev)
├── spark-job/        # Spark Structured Streaming job (local dev)
├── ingestion/        # producer + spark-job combined into one deployable
│                     # service for Railway (see Deployment below)
├── backend/          # FastAPI REST API
├── frontend/         # React (Vite) dashboard
├── db/init.sql       # TimescaleDB schema
└── scripts/          # load_test.py
```

## Deployment

Deployed on Railway (Redpanda, TimescaleDB, the combined ingestion service,
FastAPI backend) and Vercel (frontend), chosen to run at effectively zero
committed cost:

- **No card on file.** The whole backend runs on Railway's one-time $5 trial
  credit — no payment method attached, so the hard ceiling on possible spend
  is that $5, not an open-ended bill. Once it's exhausted, Railway pauses the
  services rather than charging anything.
- **Vercel's Hobby tier is free** for the frontend outright.
- Every service is right-sized to fit the trial plan's per-service cap
  (1 GB RAM / 2 vCPU / 500 MB volume): Redpanda runs with a 512 MB memory
  budget and a 15-minute topic retention (no replay is a stated non-goal, so
  there's no reason to keep more), and TimescaleDB has automated 3-day
  retention policies on both hypertables so storage never grows unbounded.

**Deviations from local dev, and why:**

- **Producer + Spark job run as one combined service** (`ingestion/`,
  mirrors `producer/` and `spark-job/` but packaged together), not two.
  Railway's trial plan caps a project at 4 services total, and the pipeline
  needs Redpanda + TimescaleDB + backend + ingestion — exactly 4. The
  producer runs in the background of the same container; Spark runs in the
  foreground via `spark-submit`, tying the container's lifecycle to the more
  important of the two processes.
- **Fitting Spark Structured Streaming inside a 1 GB container required real
  JVM tuning**, not just "make it smaller": `spark.driver.memory=512m` (the
  practical floor — Spark itself refuses to start below ~450 MB),
  `-XX:MaxMetaspaceSize=256m` (too low and Spark's own class loading OOMs
  the metaspace independently of the heap), `-XX:MaxDirectMemorySize=64m`
  (caps the off-heap Netty buffer growth in Spark's Kafka connector — the
  actual slow-growth culprit that was pushing the container to its ceiling
  over several minutes), and `-XX:+UseSerialGC` (G1's overhead isn't worth
  it below ~1 GB heaps). `--master local[1]` and `spark.ui.enabled=false`
  trim the rest. This is the "started simple, then optimized for a real
  resource constraint" progression the project brief called out as a good
  interview story — just triggered by a billing constraint instead of a
  latency one.
- **The `producer` (ingestion) service's restart policy is set to `ALWAYS`**
  (via the Railway API — not exposed by `railway up` or the IaC helper), so
  it self-heals from the memory-pressure restarts above instead of giving up
  after Railway's default 10 retries and sitting dead until someone notices.
  **This setting does not survive a redeploy** (`railway up` resets it to
  the default `ON_FAILURE`/10) — after every redeploy of this service, re-set
  it: `railway up ... --service producer` then immediately update the
  service's restart policy back to `ALWAYS` (Railway dashboard, or the
  `update-service` Railway MCP tool / API).
- Spark still runs in local mode (`apache/spark:3.5.1`, single container)
  rather than a separate master/worker cluster, both locally and in
  production — this is the same footprint-vs-fidelity tradeoff, just applied
  twice. Scaling to a real multi-worker cluster is a natural follow-up.
- Anomaly baseline state (EWMA + variance) is stored in a Postgres table
  (`entity_baseline`) and read/updated per micro-batch via `foreachBatch`,
  per the brief's recommended v1 approach — simpler and easier to verify
  correct than Spark's native stateful streaming operators. Migrating to
  `applyInPandasWithState` is the natural v2 optimization.
- `window_stats` has a composite primary key `(window_start, entity_type, entity_key)`
  with upsert-on-conflict, so the job can be restarted without duplicating rows.
- The Kafka client is `kafka-python`, not `confluent-kafka` — the latter
  needs to compile against `librdkafka` from source on the ingestion
  container's Python 3.8 (no prebuilt wheel available there), and the
  Debian-shipped `librdkafka-dev` is too old for the version `confluent-kafka`
  needs. A pure-Python client sidesteps the whole C-extension version-matching
  problem, at a throughput cost that's irrelevant at this project's volume.
