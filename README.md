# WikiPulse

A real time anomaly detector for Wikipedia edits. It listens to Wikipedia's live edit feed, crunches the numbers in windows using Spark Structured Streaming, and flags weird stuff as it happens: vandalism spikes, bot bursts, edit wars, that kind of thing. Everything shows up on a live dashboard.

I built this to get real hands on experience with streaming data and distributed processing, the kind of stuff that trust and safety teams at places like Meta or Reddit would use to catch abuse in real time, just scaled way down.

**Live demo:** https://wikipulse-six.vercel.app
**API:** https://backend-production-1064.up.railway.app/docs

Check out [project_brief.md](project_brief.md) if you want the full writeup of how I designed this thing.

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
If you want to see flags show up faster for testing, drop `Z_THRESHOLD` in `docker-compose.yml` down to something like `1.0` and restart the `spark-job` service.

**Dashboard**

Open the forwarded port 5173 URL from the Ports tab in Codespaces.

## Load test

Once the stack has been running a few minutes (so `window_stats` actually has some data in it):

```
pip install -r scripts/requirements.txt
python scripts/load_test.py --duration 60
```

<!-- I still need to run this for real and drop the throughput/latency numbers here. -->

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

Everything runs on Railway (Redpanda, TimescaleDB, the ingestion service, the FastAPI backend) plus Vercel for the frontend. I picked this combo because I could get it running for basically nothing:

- No card on file anywhere. The backend runs entirely on Railway's one time $5 trial credit, so the absolute worst case is losing $5, not some surprise bill. Once the credit runs out Railway just pauses the services instead of charging me.
- Vercel's free Hobby tier covers the frontend, no cost there either.
- Had to size everything to fit inside the trial plan's per service limit (1 GB RAM, 2 vCPU, 500 MB volume). Redpanda runs on a 512 MB memory budget with only 15 minutes of topic retention (I'm not trying to replay old data, so no reason to keep more), and TimescaleDB has retention policies set to auto delete anything older than 3 days so storage doesn't just keep growing.

**Stuff that's different from local dev, and why:**

- The producer and Spark job run as one combined service in production (`ingestion/`, basically `producer/` and `spark-job/` glued together) instead of two separate ones. Railway's trial plan caps you at 4 services per project, and I needed Redpanda, TimescaleDB, the backend, and the pipeline itself, which is exactly 4 if I combine producer and Spark. The producer runs in the background of the container while Spark runs in the foreground via `spark-submit`.
- Getting Spark Structured Streaming to actually fit in a 1 GB container took some real JVM tuning, not just picking smaller numbers. I landed on `spark.driver.memory=512m` (Spark won't even start much below ~450 MB), `-XX:MaxMetaspaceSize=256m` (any lower and Spark's class loading OOMs the metaspace on its own), `-XX:MaxDirectMemorySize=64m` (this was the actual culprit, the off heap Netty buffers in Spark's Kafka connector were slowly growing and pushing the container over its limit after a few minutes), and `-XX:+UseSerialGC` (G1's overhead isn't worth it at heaps this small). `--master local[1]` and `spark.ui.enabled=false` trim things further.
- The producer's restart policy is set to `ALWAYS` through the Railway API (you can't set this from `railway up` or the config file), so it recovers on its own from the memory pressure restarts mentioned above instead of dying after Railway's default 10 retries and just sitting there. Heads up: this setting does not survive a redeploy, `railway up` resets it back to `ON_FAILURE`. So after redeploying that service I have to go set it back to `ALWAYS` manually in the Railway dashboard.
- Spark still runs in local mode (`apache/spark:3.5.1`, one container) instead of a real master/worker cluster, both locally and in prod. Same tradeoff of footprint over "doing it properly," just applied in both places. A real multi worker setup would be a good next step.
- The anomaly baseline (EWMA and variance) lives in a Postgres table (`entity_baseline`) and gets read and updated each micro batch through `foreachBatch`. This is simpler to reason about and verify than Spark's native stateful streaming operators, which I'd like to switch to eventually (`applyInPandasWithState`) but wanted correctness first.
- `window_stats` has a composite primary key of `(window_start, entity_type, entity_key)` with upsert on conflict, so restarting the job doesn't duplicate rows.
- I ended up using `kafka-python` instead of `confluent-kafka`. The latter needs to compile against `librdkafka` from source on the ingestion container's Python 3.8, and the Debian `librdkafka-dev` package there is too old for what `confluent-kafka` needs. Pure Python client sidesteps that whole mess, and the throughput hit doesn't matter at this scale anyway.
