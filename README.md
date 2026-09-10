# WikiPulse

WikiPulse A real time anomaly detector for Wikipedia. It watches every edit happening on English Wikipedia as they happen, runs the numbers through Spark Strucured Streaming in one-minute windows, and flags whatever looks off, whether it's a page's edit rate spiking way past its normal baseline, a sudden burst of anonymous editing, two editors reverting each other back and forth. It shows up on a live dashboard within a minute or two of actually happening.

I wanted real experience with streaming data and distributed processing in a similar way that abuse detection pipeline trust and safety teams at companies like Meta or Reddit would run but at a smaller scope.

**Live:** https://wikipulse-six.vercel.app

`project_brief.md` is the longer writeup — why I made the design calls I made and what I'd do differently next time.

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

## Running it locally (Codespaces)

```
docker compose up --build
```

Ports it forwards:

| Service     | Port | What it's for                        |
|-------------|------|---------------------------------------|
| redpanda    | 9092 | Kafka-protocol broker                 |
| redpanda    | 9644 | Admin API                             |
| timescaledb | 5432 | Postgres + Timescale                  |
| backend     | 8000 | FastAPI (`/docs` for the OpenAPI UI)  |
| spark-job   | 4040 | Spark UI (only while a query runs)    |
| frontend    | 5173 | React dashboard (Vite dev server)     |

## Checking each part actually works

**Ingestion**
```
docker exec -it redpanda rpk topic consume wiki-edits -n 5
```

**Stream processing**
```
docker exec -it timescaledb psql -U wikipulse -d wikipulse \
  -c "SELECT * FROM window_stats ORDER BY window_start DESC LIMIT 10;"
```

**Anomaly detection**
```
docker exec -it timescaledb psql -U wikipulse -d wikipulse \
  -c "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 10;"
```
If you want flags to show up faster while testing, drop `Z_THRESHOLD` in `docker-compose.yml` down to something like `1.0` and restart the `spark-job` service.

**Dashboard**

Open the forwarded port 5173 URL from the Ports tab in Codespaces.

## Load test

Once the stack's been running a few minutes (so `window_stats` actually has data in it):

```
pip install -r scripts/requirements.txt
python scripts/load_test.py --duration 60
```

Haven't run this for a real throughput/latency number yet — it's on the list, I just haven't gotten around to it.

## Repo structure

```
wikipulse/
├── docker-compose.yml
├── producer/         # SSE consumer, publishes to Redpanda (local dev)
├── spark-job/        # Spark Structured Streaming job (local dev)
├── ingestion/        # producer + spark-job combined into one service
│                     # for Railway, see Deployment below for why
├── backend/          # FastAPI REST API
├── frontend/         # React (Vite) dashboard
├── db/init.sql       # TimescaleDB schema
└── scripts/          # load_test.py
```
