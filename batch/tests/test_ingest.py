"""
Run from the repo root:  python -m pytest batch/tests -q
The DB test spins up a throwaway Postgres with pgserver (pip install pgserver)
and is skipped if that isn't installed.
"""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://unused")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ingest  # noqa: E402

T0 = datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc)


def change(minute: int, title="Some Page", user="Someone", comment="", second=10, **flags):
    ts = (T0 + timedelta(minutes=minute, seconds=second)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"title": title, "user": user, "comment": comment, "timestamp": ts, **flags}


def test_temp_accounts_and_ips_count_as_anonymous():
    assert ingest.is_anonymous({"user": "~2026-53667-39", "temp": True})
    assert ingest.is_anonymous({"user": "~2026-53667-39"})  # flag missing, name gives it away
    assert ingest.is_anonymous({"user": "203.0.113.7"})
    assert ingest.is_anonymous({"user": "2001:db8::1"})
    assert not ingest.is_anonymous({"user": "Augmented Seventh"})
    assert not ingest.is_anonymous({})


def test_aggregate_buckets_by_minute_page_and_editor():
    windows = ingest.aggregate([
        change(0, user="A", comment="Reverted edits by B"),
        change(0, user="~2026-1-1", second=50),
        change(1, title="Other", user="A", comment="Undid revision 123"),
        change(1, title="Hidden", user=None),
    ])
    first = windows[T0]
    assert first[("page", "Some Page")] == {"edit_count": 2, "revert_count": 1, "anon_count": 1}
    assert first[("editor", "A")]["edit_count"] == 1
    second = windows[T0 + timedelta(minutes=1)]
    assert second[("page", "Other")]["revert_count"] == 1
    assert ("editor", None) not in second  # hidden usernames only count toward the page
    assert second[("page", "Hidden")]["edit_count"] == 1


def test_detect_matches_spark_job_math():
    # 1 edit/min for 5 minutes builds a flat baseline, then a burst of 12
    edits = [change(m) for m in range(5)] + [change(5, second=s) for s in range(12)]
    rows, anomalies = ingest.detect(ingest.aggregate(edits), {})
    page_rows = [r for r in rows if r[2] == "page"]

    # First window has no history: baseline = its own count, so z = 0
    assert page_rows[0][7] == 1.0 and page_rows[0][9] == 0.0
    burst = page_rows[-1]
    ewma, var, z = burst[7], burst[8], burst[9]
    assert ewma == pytest.approx(1.0) and var == pytest.approx(0.0)
    # variance is 0, so the 1.0 stddev floor applies: z = (12 - 1) / 1
    assert z == pytest.approx(11.0)
    edit_rate = [a for a in anomalies if a[3] == "edit_rate" and a[1] == "page"]
    assert len(edit_rate) == 1 and edit_rate[0][7] == "high"


def test_detect_carries_existing_baseline_forward():
    baselines = {("page", "Some Page"): (10.0, 4.0)}
    rows, _ = ingest.detect(ingest.aggregate([change(0)]), baselines)
    z = rows[0][9]
    assert z == pytest.approx((1 - 10.0) / 2.0)
    new_ewma, new_var = baselines[("page", "Some Page")]
    assert new_ewma == pytest.approx(0.3 * 1 + 0.7 * 10.0)
    assert new_var == pytest.approx(0.3 * (1 - 10.0) ** 2 + 0.7 * 4.0)


def test_revert_rate_only_flags_pages():
    rows, anomalies = ingest.detect(ingest.aggregate([change(0, comment="revert vandalism")]), {})
    assert [(a[1], a[3], a[4]) for a in anomalies] == [("page", "revert_rate", 1.0)]


def test_run_against_real_postgres(monkeypatch):
    pgserver = pytest.importorskip("pgserver")
    psycopg2 = pytest.importorskip("psycopg2")
    srv = pgserver.get_server(tempfile.mkdtemp(), cleanup_mode="stop")
    uri = srv.get_uri()
    schema = (Path(__file__).resolve().parents[2] / "db" / "schema_postgres.sql").read_text()
    with psycopg2.connect(uri) as conn, conn.cursor() as cur:
        cur.execute(schema)

    fetched = []

    def fake_fetch(start, end):
        fetched.append((start, end))
        minutes = int((end - start).total_seconds() // 60)
        base = int((start - T0).total_seconds() // 60)
        return [change(base + m, title="Busy", user="Bot") for m in range(minutes)]

    monkeypatch.setattr(ingest, "DATABASE_URL", uri)
    monkeypatch.setattr(ingest, "fetch_edits", fake_fetch)

    now = T0 + timedelta(hours=1)
    ingest.run(now)
    ingest.run(now)  # same time again: nothing new, must not double-count
    ingest.run(now + timedelta(minutes=15))

    end1 = now - timedelta(minutes=2)
    assert fetched == [(end1 - timedelta(minutes=30), end1), (end1, end1 + timedelta(minutes=15))]
    with psycopg2.connect(uri) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT window_start) FROM window_stats WHERE entity_type = 'page'")
        assert cur.fetchone() == (45, 45)
        cur.execute("SELECT count(*) FROM ingest_runs")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT ewma, variance FROM entity_baseline WHERE entity_key = 'Busy'")
        assert cur.fetchone() == (pytest.approx(1.0), pytest.approx(0.0))
