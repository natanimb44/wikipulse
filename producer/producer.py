"""
SSE consumer for Wikipedia's live EventStreams recentchange feed -> Redpanda publisher.

Reads https://stream.wikimedia.org/v2/stream/recentchange, filters down to
plain article edits on English Wikipedia, and republishes the relevant fields
as JSON onto the `wiki-edits` Kafka-protocol topic (keyed by page title so all
edits for a page land on the same partition).
"""
from __future__ import annotations

import json
import logging
import os
import time

import httpx
from httpx_sse import connect_sse
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia's EventStreams endpoint rejects requests without a descriptive
# User-Agent (see https://meta.wikimedia.org/wiki/User-Agent_policy).
USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "WikiPulse/0.1 (https://github.com/natanimb44/wikipulse; educational streaming-pipeline project)",
)
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
TOPIC = os.environ.get("WIKI_EDITS_TOPIC", "wiki-edits")
WIKI_FILTER = os.environ.get("WIKI_FILTER", "enwiki")
NAMESPACE_FILTER = int(os.environ.get("NAMESPACE_FILTER", "0"))
ALLOWED_TYPES = {"edit", "new"}

RECONNECT_BACKOFF_S = 2
MAX_BACKOFF_S = 30
LOG_INTERVAL_S = 30


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        client_id="wikipulse-producer",
        linger_ms=50,
        retries=5,
    )


def delivery_errback(exc: Exception) -> None:
    log.warning("delivery failed: %s", exc)


def looks_anonymous(user: str | None) -> bool:
    if not user:
        return False
    # crude IPv4/IPv6 check used as the "anon edit" proxy per the brief
    parts = user.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        return True
    return ":" in user and all(c in "0123456789abcdefABCDEF:" for c in user)


def extract_event(raw: dict) -> dict | None:
    if raw.get("type") not in ALLOWED_TYPES:
        return None
    if raw.get("wiki") != WIKI_FILTER:
        return None
    if raw.get("namespace") != NAMESPACE_FILTER:
        return None

    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    user = raw.get("user")

    return {
        "title": raw.get("title"),
        "user": user,
        "bot": bool(raw.get("bot", False)),
        "anon": looks_anonymous(user),
        "timestamp": raw.get("timestamp"),
        "type": raw.get("type"),
        "wiki": raw.get("wiki"),
        "namespace": raw.get("namespace"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "revision_old": revision.get("old"),
        "revision_new": revision.get("new"),
        "comment": raw.get("comment"),
        "server_url": raw.get("server_url"),
    }


def run():
    producer = make_producer()
    backoff = RECONNECT_BACKOFF_S
    sent = 0
    filtered_in = 0
    last_log = time.monotonic()

    while True:
        try:
            with httpx.Client(timeout=None, headers={"User-Agent": USER_AGENT}) as client:
                with connect_sse(client, "GET", STREAM_URL) as event_source:
                    log.info("connected to %s", STREAM_URL)
                    backoff = RECONNECT_BACKOFF_S
                    for sse in event_source.iter_sse():
                        if sse.event != "message" or not sse.data:
                            continue
                        try:
                            raw = json.loads(sse.data)
                        except json.JSONDecodeError:
                            continue

                        filtered_in += 1
                        event = extract_event(raw)
                        if event is None:
                            continue

                        producer.send(
                            TOPIC,
                            key=(event["title"] or "").encode("utf-8"),
                            value=json.dumps(event).encode("utf-8"),
                        ).add_errback(delivery_errback)
                        sent += 1

                        now = time.monotonic()
                        if now - last_log >= LOG_INTERVAL_S:
                            log.info(
                                "seen=%d published=%d (last %ds)",
                                filtered_in, sent, LOG_INTERVAL_S,
                            )
                            filtered_in = 0
                            sent = 0
                            last_log = now
        except (httpx.HTTPError, KafkaError) as exc:
            log.warning("stream error: %s - reconnecting in %ds", exc, backoff)
            producer.flush(5)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)


if __name__ == "__main__":
    run()
