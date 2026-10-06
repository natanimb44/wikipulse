"""
Batch version of the WikiPulse pipeline, for running cheaply in production.

The streaming version (producer -> Redpanda -> Spark) needs ~2 GB of RAM running
24/7, which got too expensive to keep hosted. This does the same detection on a
schedule instead: every 15 minutes (GitHub Actions cron) it pulls every English
Wikipedia article edit since the last run from the MediaWiki recentchanges API,
buckets them into the same 1-minute windows per page and per editor, and runs the
same EWMA + z-score logic as spark-job/stream_processor.py.

Trade-off: anomalies show up up to ~15 minutes late instead of within a minute or
two. The streaming version still runs locally with docker compose.

Usage:  DATABASE_URL=postgresql://... python batch/ingest.py
"""
import logging
import math
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

DATABASE_URL = os.environ["DATABASE_URL"]
WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "WikiPulse/0.1 (https://github.com/natanimb44/wikipulse; educational streaming-pipeline project)",
)

# Same knobs and defaults as the Spark job
EWMA_ALPHA = float(os.environ.get("EWMA_ALPHA", "0.3"))
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))
REVERT_RATIO_THRESHOLD = float(os.environ.get("REVERT_RATIO_THRESHOLD", "0.5"))
MIN_STDDEV = 1.0

# Leave the last couple of minutes for the next run, so late-arriving edits
# land in their window (same idea as the 2-minute watermark in Spark).
LAG = timedelta(minutes=2)
# First run, or after an outage: don't try to backfill more than this
MAX_BACKFILL = timedelta(hours=3)
# The dashboard only ever looks at the last 24h, and the free Postgres tier is
# 500 MB, so keep 2 days of windows/anomalies and a week of baselines.
RETENTION = timedelta(days=2)
BASELINE_RETENTION = timedelta(days=7)

# "Undid revision ..." is the standard undo summary, so "undo" alone misses it
REVERT_PATTERN = re.compile(r"revert|undo|undid", re.IGNORECASE)
IP_PATTERN = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$|^[0-9a-fA-F:]+:[0-9a-fA-F:]*$")


def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def fmt(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def severity_for(z: float) -> str:
    az = abs(z)
    if az >= 6:
        return "high"
    if az >= 4:
        return "medium"
    return "low"


def is_anonymous(change: dict) -> bool:
    # Since 2025, logged-out editors on enwiki get temporary accounts ("~2026-12345-67")
    # instead of showing their IP, so the IP check alone misses nearly all of them now.
    user = change.get("user") or ""
    return bool(change.get("temp") or change.get("anon") or user.startswith("~") or IP_PATTERN.match(user))


def fetch_edits(start: datetime, end: datetime) -> list[dict]:
    """Every article edit/creation on enwiki with start <= timestamp < end, oldest first."""
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "list": "recentchanges", "rcnamespace": "0", "rctype": "edit|new",
        "rcprop": "title|user|timestamp|comment|flags", "rclimit": "500",
        # recentchanges lists newest first, so rcstart is the later bound
        "rcstart": fmt(end - timedelta(seconds=1)), "rcend": fmt(start),
    }
    edits = []
    with httpx.Client(headers={"User-Agent": WIKI_USER_AGENT}, timeout=30) as client:
        while True:
            resp = client.get(WIKI_API, params=params)
            resp.raise_for_status()
            body = resp.json()
            edits += body["query"]["recentchanges"]
            if "continue" not in body:
                break
            params.update(body["continue"])
    edits.reverse()
    return edits


def aggregate(edits: list[dict]) -> dict[datetime, dict[tuple[str, str], dict]]:
    """minute -> (entity_type, entity_key) -> {edit_count, revert_count, anon_count}"""
    windows: dict[datetime, dict[tuple[str, str], dict]] = defaultdict(
        lambda: defaultdict(lambda: {"edit_count": 0, "revert_count": 0, "anon_count": 0})
    )
    for e in edits:
        minute = floor_minute(datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")))
        is_revert = bool(REVERT_PATTERN.search(e.get("comment") or ""))
        anon = is_anonymous(e)
        for entity in (("page", e.get("title")), ("editor", e.get("user"))):
            if not entity[1]:  # hidden usernames come back without a user field
                continue
            w = windows[minute][entity]
            w["edit_count"] += 1
            w["revert_count"] += is_revert
            w["anon_count"] += anon
    return windows


def detect(windows, baselines: dict[tuple[str, str], tuple[float, float]]):
    """Walk the windows oldest first, scoring each against the entity's baseline and then
    updating it. Mutates `baselines`. Returns (window_rows, anomaly_rows)."""
    window_rows, anomaly_rows = [], []
    for minute in sorted(windows):
        for (entity_type, entity_key), w in windows[minute].items():
            edit_count, revert_count = w["edit_count"], w["revert_count"]
            ewma, var = baselines.get((entity_type, entity_key), (float(edit_count), 0.0))

            z = (edit_count - ewma) / max(math.sqrt(var), MIN_STDDEV)
            baselines[(entity_type, entity_key)] = (
                EWMA_ALPHA * edit_count + (1 - EWMA_ALPHA) * ewma,
                EWMA_ALPHA * (edit_count - ewma) ** 2 + (1 - EWMA_ALPHA) * var,
            )

            window_rows.append((
                minute, minute + timedelta(minutes=1), entity_type, entity_key,
                edit_count, revert_count, w["anon_count"] / edit_count, ewma, var, z,
            ))
            if abs(z) > Z_THRESHOLD:
                anomaly_rows.append((minute, entity_type, entity_key, "edit_rate",
                                     float(edit_count), ewma, z, severity_for(z)))
            if entity_type == "page":
                revert_ratio = revert_count / edit_count
                if revert_ratio > REVERT_RATIO_THRESHOLD:
                    pseudo_z = revert_ratio / REVERT_RATIO_THRESHOLD
                    anomaly_rows.append((minute, entity_type, entity_key, "revert_rate",
                                         revert_ratio, REVERT_RATIO_THRESHOLD, pseudo_z, severity_for(pseudo_z)))
    return window_rows, anomaly_rows


def load_baselines(cur, windows) -> dict[tuple[str, str], tuple[float, float]]:
    keys = defaultdict(set)
    for entities in windows.values():
        for entity_type, entity_key in entities:
            keys[entity_type].add(entity_key)
    baselines = {}
    for entity_type, entity_keys in keys.items():
        cur.execute(
            "SELECT entity_key, ewma, variance FROM entity_baseline WHERE entity_type = %s AND entity_key = ANY(%s)",
            (entity_type, list(entity_keys)),
        )
        for entity_key, ewma, var in cur.fetchall():
            baselines[(entity_type, entity_key)] = (ewma, var)
    return baselines


def run(now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    end = floor_minute(now - LAG)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            # Only one run at a time, even if a cron run overlaps a manual one
            cur.execute("SELECT pg_try_advisory_xact_lock(4242)")
            if not cur.fetchone()[0]:
                log.info("another run holds the lock, skipping")
                return

            cur.execute("SELECT max(window_to) FROM ingest_runs")
            last = cur.fetchone()[0]
            start = max(last, end - MAX_BACKFILL) if last else end - timedelta(minutes=30)
            if start >= end:
                log.info("nothing new to process (last run covered up to %s)", last)
                return

            edits = fetch_edits(start, end)
            windows = aggregate(edits)
            baselines = load_baselines(cur, windows)
            window_rows, anomaly_rows = detect(windows, baselines)

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO window_stats
                    (window_start, window_end, entity_type, entity_key, edit_count, revert_count,
                     anon_ratio, baseline_ewma, baseline_var, z_score)
                VALUES %s
                ON CONFLICT (window_start, entity_type, entity_key) DO UPDATE SET
                    edit_count = EXCLUDED.edit_count, revert_count = EXCLUDED.revert_count,
                    anon_ratio = EXCLUDED.anon_ratio, baseline_ewma = EXCLUDED.baseline_ewma,
                    baseline_var = EXCLUDED.baseline_var, z_score = EXCLUDED.z_score
                """,
                window_rows, page_size=1000,
            )
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO anomalies (window_start, entity_type, entity_key, metric, value, baseline, z_score, severity)
                VALUES %s
                """,
                anomaly_rows,
            )
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO entity_baseline (entity_type, entity_key, ewma, variance, updated_at)
                VALUES %s
                ON CONFLICT (entity_type, entity_key) DO UPDATE SET
                    ewma = EXCLUDED.ewma, variance = EXCLUDED.variance, updated_at = now()
                """,
                [(t, k, ewma, var) for (t, k), (ewma, var) in baselines.items()],
                template="(%s, %s, %s, %s, now())", page_size=1000,
            )
            cur.execute(
                "INSERT INTO ingest_runs (window_from, window_to, edits, anomalies) VALUES (%s, %s, %s, %s)",
                (start, end, len(edits), len(anomaly_rows)),
            )

            cur.execute("DELETE FROM window_stats WHERE window_start < now() - %s", (RETENTION,))
            cur.execute("DELETE FROM anomalies WHERE detected_at < now() - %s", (RETENTION,))
            cur.execute("DELETE FROM entity_baseline WHERE updated_at < now() - %s", (BASELINE_RETENTION,))
            cur.execute("DELETE FROM ingest_runs WHERE run_at < now() - %s", (RETENTION,))

        log.info("processed %s -> %s: %d edits, %d windows, %d anomalies",
                 fmt(start), fmt(end), len(edits), len(window_rows), len(anomaly_rows))
    finally:
        conn.close()


if __name__ == "__main__":
    run()
