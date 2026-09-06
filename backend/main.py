from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from db import get_cursor, healthcheck
from models import Anomaly, GlobalStat, TrendingPage, WindowStat

app = FastAPI(title="WikiPulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

VALID_SEVERITIES = {"low", "medium", "high"}


@app.get("/health")
def health():
    ok = healthcheck()
    return {"status": "ok" if ok else "unavailable"}


@app.get("/anomalies/recent", response_model=list[Anomaly])
def recent_anomalies(limit: int = 50):
    limit = max(1, min(limit, 500))
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, detected_at, window_start, entity_type, entity_key,
                   metric, value, baseline, z_score, severity
            FROM anomalies
            ORDER BY detected_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


@app.get("/anomalies/severity/{level}", response_model=list[Anomaly])
def anomalies_by_severity(level: str, limit: int = 50):
    if level not in VALID_SEVERITIES:
        raise HTTPException(status_code=400, detail=f"severity must be one of {sorted(VALID_SEVERITIES)}")
    limit = max(1, min(limit, 500))
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, detected_at, window_start, entity_type, entity_key,
                   metric, value, baseline, z_score, severity
            FROM anomalies
            WHERE severity = %s
            ORDER BY detected_at DESC
            LIMIT %s
            """,
            (level, limit),
        )
        return cur.fetchall()


def _entity_stats(entity_type: str, entity_key: str, minutes: int) -> list[dict]:
    minutes = max(1, min(minutes, 1440))
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT window_start, window_end, entity_type, entity_key,
                   edit_count, revert_count, anon_ratio, baseline_ewma, z_score
            FROM window_stats
            WHERE entity_type = %s
              AND entity_key = %s
              AND window_start >= now() - (%s || ' minutes')::interval
            ORDER BY window_start ASC
            """,
            (entity_type, entity_key, minutes),
        )
        return cur.fetchall()


@app.get("/stats/page/{title}", response_model=list[WindowStat])
def page_stats(title: str, minutes: int = 60):
    return _entity_stats("page", title, minutes)


@app.get("/stats/editor/{username}", response_model=list[WindowStat])
def editor_stats(username: str, minutes: int = 60):
    return _entity_stats("editor", username, minutes)


@app.get("/stats/global", response_model=list[GlobalStat])
def global_stats(minutes: int = 60):
    minutes = max(1, min(minutes, 1440))
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT window_start, SUM(edit_count) AS edit_count
            FROM window_stats
            WHERE entity_type = 'page'
              AND window_start >= now() - (%s || ' minutes')::interval
            GROUP BY window_start
            ORDER BY window_start ASC
            """,
            (minutes,),
        )
        return cur.fetchall()


@app.get("/stats/trending", response_model=list[TrendingPage])
def trending_pages(minutes: int = 10, limit: int = 8):
    minutes = max(1, min(minutes, 1440))
    limit = max(1, min(limit, 50))
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT entity_key,
                   SUM(edit_count)::int AS edit_count,
                   SUM(revert_count)::int AS revert_count,
                   AVG(anon_ratio) AS anon_ratio
            FROM window_stats
            WHERE entity_type = 'page'
              AND window_start >= now() - (%s || ' minutes')::interval
            GROUP BY entity_key
            ORDER BY edit_count DESC
            LIMIT %s
            """,
            (minutes, limit),
        )
        return cur.fetchall()
