"""
Measures two numbers for the README:
1. Producer -> Redpanda throughput (messages/sec observed on `wiki-edits` over a sample window).
2. End-to-end pipeline lag (how far behind wall-clock the latest finalized window in
   TimescaleDB is - i.e. ingestion -> Spark windowing -> DB write latency).

Run from the host (or any machine that can reach the forwarded ports):
    python scripts/load_test.py --duration 60
"""
import argparse
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer

DEFAULT_BROKERS = "localhost:9092"
DEFAULT_PG = dict(host="localhost", port=5432, dbname="wikipulse", user="wikipulse", password="wikipulse")


def measure_throughput(brokers: str, topic: str, duration: int) -> float:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers.split(","),
        group_id=f"load-test-{int(time.time())}",
        auto_offset_reset="latest",
    )

    count = 0
    start = time.monotonic()
    try:
        while time.monotonic() - start < duration:
            batches = consumer.poll(timeout_ms=1000)
            count += sum(len(records) for records in batches.values())
    finally:
        consumer.close()

    elapsed = time.monotonic() - start
    return count / elapsed if elapsed > 0 else 0.0


def measure_pipeline_lag(pg_config: dict) -> float | None:
    conn = psycopg2.connect(**pg_config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(window_end) FROM window_stats")
            (latest,) = cur.fetchone()
    finally:
        conn.close()

    if latest is None:
        return None
    return (datetime.now(timezone.utc) - latest).total_seconds()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default=DEFAULT_BROKERS)
    parser.add_argument("--topic", default="wiki-edits")
    parser.add_argument("--duration", type=int, default=60, help="seconds to sample throughput over")
    parser.add_argument("--pg-host", default=DEFAULT_PG["host"])
    args = parser.parse_args()

    print(f"Sampling {args.topic} throughput for {args.duration}s...")
    throughput = measure_throughput(args.brokers, args.topic, args.duration)
    print(f"  -> {throughput:.2f} messages/sec")

    pg_config = {**DEFAULT_PG, "host": args.pg_host}
    print("Measuring end-to-end pipeline lag (now - latest finalized window_end)...")
    lag = measure_pipeline_lag(pg_config)
    if lag is None:
        print("  -> no rows in window_stats yet")
    else:
        print(f"  -> {lag:.1f}s behind wall clock")


if __name__ == "__main__":
    main()
