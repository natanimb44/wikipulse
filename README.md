# WikiPulse

A real time anomaly detector for Wikipedia. It watches every edit happening on English Wikipedia right now, runs the numbers through Spark Structured Streaming in one-minute windows, and flags whatever looks off — a page's edit rate spiking way past its normal baseline, a sudden burst of anonymous editing, two editors reverting each other back and forth. It shows up on a live dashboard within a minute or two of actually happening.

I wanted real experience with streaming data and distributed processing, not another notebook that only works once on a CSV I downloaded. This is a small, one-person version of the kind of abuse detection pipeline trust and safety teams at somewhere like Meta or Reddit would run — same idea, obviously way smaller scope.

**Live demo:** https://wikipulse-six.vercel.app
**API docs:** https://backend-production-1064.up.railway.app/docs

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

## Deployment

Everything runs on Railway (Redpanda, TimescaleDB, the ingestion service, the FastAPI backend) plus Vercel for the frontend. I picked this combo mainly because I could get it running for basically nothing:

- No card on file anywhere. The backend runs entirely on Railway's one-time $5 trial credit, so worst case I lose $5, not get hit with a surprise bill. Once the credit's gone Railway just pauses the services instead of charging me.
- Vercel's free Hobby tier covers the frontend.
- Everything had to fit inside the trial plan's per-service limit (1 GB RAM, 2 vCPU, 500 MB volume). Redpanda runs on a 512 MB memory budget with 15 minutes of topic retention — I'm not replaying old data, so there's no reason to keep more than that. TimescaleDB has a retention policy that drops anything older than 3 days so storage doesn't just keep growing on me.

**A few things that are different from local dev, and why:**

- The producer and Spark job run as one combined service in production (`ingestion/` — basically `producer/` and `spark-job/` glued together) instead of two separate ones. Railway's trial plan caps a project at 4 services, and between Redpanda, TimescaleDB, the backend, and the pipeline itself, combining producer + Spark was the only way to fit. The producer runs in the background of the container while Spark runs in the foreground via `spark-submit`.
- Fitting Spark Structured Streaming into a 1 GB container took actual JVM tuning, not just picking smaller numbers and hoping. I ended up at `spark.driver.memory=512m` (Spark won't really start below ~450 MB), `-XX:MaxMetaspaceSize=256m` (any lower and Spark's own class loading OOMs the metaspace), `-XX:MaxDirectMemorySize=64m` (this was the real culprit — off-heap Netty buffers in Spark's Kafka connector were slowly growing and pushing the container over its limit after a few minutes of running), and `-XX:+UseSerialGC` (G1's overhead isn't worth it at heaps this small). `--master local[1]` and `spark.ui.enabled=false` trim it further.
- The producer's restart policy has to be set to `ALWAYS` through the Railway API — there's no way to do it from `railway up` or the config file. That's what lets it recover on its own from the memory-pressure restarts mentioned above instead of burning through Railway's default 10 retries and just sitting there dead. Annoying part: this setting doesn't survive a redeploy. `railway up` quietly resets it back to `ON_FAILURE`, so every time I redeploy that service I have to go back into the dashboard and flip it to `ALWAYS` again.
- Spark still runs in local mode (`apache/spark:3.5.1`, one container), both locally and in prod, instead of a real master/worker cluster. Same footprint-over-correctness tradeoff applied in both places. A real multi-worker setup would be the obvious next step if this needed to scale.
- The anomaly baseline (EWMA and variance, per page/editor) lives in a Postgres table called `entity_baseline` and gets read and updated every micro-batch through `foreachBatch`. Spark has native stateful streaming operators (`applyInPandasWithState`) that would be faster, but I wanted the logic to be simple enough to verify by hand before optimizing it.
- `window_stats` has a composite primary key of `(window_start, entity_type, entity_key)` with upsert on conflict, so restarting the job doesn't leave duplicate rows behind.
- I ended up using `kafka-python` instead of `confluent-kafka`. The latter needs to compile against `librdkafka` from source, and the Debian `librdkafka-dev` package on the ingestion container's Python is too old for what `confluent-kafka` wants. Pure Python client sidesteps the whole problem, and the throughput hit doesn't matter at this scale.
