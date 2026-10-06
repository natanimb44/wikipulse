"""
MCP server for WikiPulse. Lets an LLM agent (Claude, or any MCP client) query
the live anomaly data and investigate what's behind a flag.

Mounted inside the FastAPI backend at /mcp (see main.py) instead of running as
its own service, because the Railway trial plan caps the project at 4 services
and all 4 are already in use.

Everything here is read-only. The DB only stores 1-minute aggregates, not raw
edits, so get_anomaly_context pulls the actual revisions for the flagged window
from Wikipedia's public API.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from db import LATEST_WINDOW, get_cursor

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKI_USER_AGENT = os.environ.get(
    "WIKI_USER_AGENT",
    "WikiPulse/0.1 (https://github.com/natanimb44/wikipulse; educational streaming-pipeline project)",
)

# Mirrors the defaults in the Spark job. The backend doesn't get the Spark
# job's env vars, so these are just for describing the method to the model.
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))
REVERT_RATIO_THRESHOLD = float(os.environ.get("REVERT_RATIO_THRESHOLD", "0.5"))
EWMA_ALPHA = float(os.environ.get("EWMA_ALPHA", "0.3"))

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)

mcp = MCPServer(
    name="wikipulse",
    title="WikiPulse",
    instructions=(
        "WikiPulse watches every edit on English Wikipedia in real time and flags pages or editors "
        "whose activity breaks from their own baseline. Start with get_pipeline_health if results look "
        "empty (no anomalies can mean a quiet minute or a stalled pipeline). Use get_active_anomalies to "
        "see what's flagged, then get_anomaly_context to see the actual edits behind a flag. "
        "Read wikipulse://detection-method before explaining what a z-score or severity means."
    ),
)


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(value, high))


def _iso(rows: list[dict]) -> list[dict[str, Any]]:
    # RealDictCursor hands back datetimes, which aren't JSON-serializable
    return [{k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()} for row in rows]


# ---------- Tools ----------

@mcp.tool(annotations=READ_ONLY)
def get_active_anomalies(
    minutes: int = 30,
    severity: Literal["low", "medium", "high"] | None = None,
    entity_type: Literal["page", "editor"] | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """List anomalies flagged in the last `minutes` (max 1440), newest first.

    Each anomaly has an `id` you can pass to get_anomaly_context. `metric` is either
    'edit_rate' (edits per minute way above the entity's own baseline) or 'revert_rate'
    (more than half the edits in the window were reverts/undos).
    """
    minutes = _clamp(minutes, 1, 1440)
    limit = _clamp(limit, 1, 100)
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, detected_at, window_start, entity_type, entity_key,
                   metric, value, baseline, z_score, severity
            FROM anomalies
            WHERE detected_at >= now() - (%s || ' minutes')::interval
              AND (%s::text IS NULL OR severity = %s)
              AND (%s::text IS NULL OR entity_type = %s)
            ORDER BY detected_at DESC, window_start DESC, id DESC
            LIMIT %s
            """,
            (minutes, severity, severity, entity_type, entity_type, limit),
        )
        return _iso(cur.fetchall())


@mcp.tool(annotations=READ_ONLY)
def get_entity_timeline(
    entity_type: Literal["page", "editor"], entity_key: str, minutes: int = 60
) -> list[dict[str, Any]]:
    """Minute-by-minute activity for one page (by exact title) or editor (username or IP).

    Returns edit_count, revert_count, anon_ratio, the EWMA baseline the window was compared
    against, and its z-score. Use this to see whether a spike is a one-off or sustained.
    """
    minutes = _clamp(minutes, 1, 1440)
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT window_start, edit_count, revert_count, anon_ratio, baseline_ewma, z_score
            FROM window_stats
            WHERE entity_type = %s
              AND entity_key = %s
              AND window_start >= {LATEST_WINDOW} - (%s || ' minutes')::interval
            ORDER BY window_start ASC
            """,
            (entity_type, entity_key, minutes),
        )
        return _iso(cur.fetchall())


@mcp.tool(annotations=READ_ONLY)
def get_trending_pages(minutes: int = 10, limit: int = 10) -> list[dict[str, Any]]:
    """Most-edited pages over the last `minutes`, with revert counts and the share of anonymous edits.

    Busy isn't the same as anomalous (a page can trend without breaking its own baseline),
    so cross-check with get_active_anomalies.
    """
    minutes = _clamp(minutes, 1, 1440)
    limit = _clamp(limit, 1, 50)
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT entity_key AS title,
                   SUM(edit_count)::int AS edit_count,
                   SUM(revert_count)::int AS revert_count,
                   AVG(anon_ratio) AS anon_ratio
            FROM window_stats
            WHERE entity_type = 'page'
              AND window_start >= {LATEST_WINDOW} - (%s || ' minutes')::interval
            GROUP BY entity_key
            ORDER BY edit_count DESC
            LIMIT %s
            """,
            (minutes, limit),
        )
        return cur.fetchall()


@mcp.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False, open_world_hint=True))
def get_anomaly_context(anomaly_id: int) -> dict[str, Any]:
    """Everything needed to judge one anomaly: the flag itself, the entity's last 30 minutes of
    activity, any other flags on the same entity in the last 24h, and the actual Wikipedia edits
    made during the flagged window (editor, timestamp, edit summary, byte change).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, detected_at, window_start, entity_type, entity_key,
                   metric, value, baseline, z_score, severity
            FROM anomalies
            WHERE id = %s
            """,
            (anomaly_id,),
        )
        anomaly = cur.fetchone()
        if anomaly is None:
            raise ToolError(f"No anomaly with id {anomaly_id}. It may have aged out (anomalies are kept 2 days).")

        cur.execute(
            """
            SELECT window_start, edit_count, revert_count, anon_ratio, baseline_ewma, z_score
            FROM window_stats
            WHERE entity_type = %s AND entity_key = %s
              AND window_start BETWEEN %s - interval '30 minutes' AND %s + interval '5 minutes'
            ORDER BY window_start ASC
            """,
            (anomaly["entity_type"], anomaly["entity_key"], anomaly["window_start"], anomaly["window_start"]),
        )
        timeline = cur.fetchall()

        cur.execute(
            """
            SELECT id, detected_at, metric, z_score, severity
            FROM anomalies
            WHERE entity_type = %s AND entity_key = %s AND id <> %s
              AND detected_at >= now() - interval '24 hours'
            ORDER BY detected_at DESC
            LIMIT 20
            """,
            (anomaly["entity_type"], anomaly["entity_key"], anomaly_id),
        )
        related = cur.fetchall()

    try:
        edits = _fetch_wikipedia_edits(anomaly["entity_type"], anomaly["entity_key"], anomaly["window_start"])
    except httpx.HTTPError as e:
        # The DB half is still useful on its own, so don't fail the whole call
        edits = {"error": f"Couldn't reach Wikipedia API: {e}"}

    return {
        "anomaly": _iso([anomaly])[0],
        "timeline": _iso(timeline),
        "related_anomalies_24h": _iso(related),
        "wikipedia_edits_in_window": edits,
    }


@mcp.tool(annotations=READ_ONLY)
def get_pipeline_health() -> dict[str, Any]:
    """Is the pipeline actually running? Returns how stale the newest processed window is, edit
    volume over the last 5 minutes, and anomaly counts by severity over the last hour.

    Check this before concluding "nothing is happening": an empty anomaly list with a stale
    latest window means the pipeline is down, not that Wikipedia is quiet.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT max(window_start) AS latest_window,
                   extract(epoch FROM now() - max(window_start))::int AS seconds_behind
            FROM window_stats
            WHERE window_start >= now() - interval '1 day'
            """
        )
        latest = cur.fetchone()
        cur.execute(
            f"""
            SELECT coalesce(sum(edit_count), 0)::int AS edits_last_5m,
                   count(DISTINCT entity_key)::int AS pages_last_5m
            FROM window_stats
            WHERE entity_type = 'page' AND window_start >= {LATEST_WINDOW} - interval '5 minutes'
            """
        )
        volume = cur.fetchone()
        cur.execute(
            """
            SELECT severity, count(*)::int AS n
            FROM anomalies
            WHERE detected_at >= now() - interval '1 hour'
            GROUP BY severity
            """
        )
        by_severity = {row["severity"]: row["n"] for row in cur.fetchall()}

    seconds_behind = latest["seconds_behind"]
    # The hosted pipeline runs every 15 minutes (GitHub cron, which can start a few
    # minutes late) and holds back the last 2 minutes, so up to ~30 minutes is normal
    status = "down" if seconds_behind is None or seconds_behind > 3600 else "lagging" if seconds_behind > 1800 else "ok"
    return {
        "status": status,
        "latest_window": latest["latest_window"].isoformat() if latest["latest_window"] else None,
        "seconds_behind": seconds_behind,
        **volume,
        "anomalies_last_hour": {s: by_severity.get(s, 0) for s in ("high", "medium", "low")},
    }


def _fetch_wikipedia_edits(
    entity_type: str, entity_key: str, window_start: datetime, limit: int = 25
) -> list[dict[str, Any]]:
    # Pad the 1-minute window a bit: Spark windows on event time, and Wikipedia's
    # revision timestamps can land a few seconds either side of it.
    start = (window_start - timedelta(minutes=1)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (window_start + timedelta(minutes=2)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {"action": "query", "format": "json", "formatversion": "2"}
    if entity_type == "page":
        params |= {
            "prop": "revisions", "titles": entity_key, "rvprop": "ids|timestamp|user|comment|size",
            # No rvend, and one extra revision: the first edit before the window is
            # needed to work out how many bytes the oldest in-window edit changed
            "rvstart": end, "rvlimit": limit + 1,
        }
    else:
        params |= {
            "list": "usercontribs", "ucuser": entity_key, "ucprop": "ids|title|timestamp|comment|sizediff",
            "ucstart": end, "ucend": start, "uclimit": limit,
        }

    resp = httpx.get(WIKI_API, params=params, headers={"User-Agent": WIKI_USER_AGENT}, timeout=10)
    resp.raise_for_status()
    query = resp.json().get("query", {})

    if entity_type == "editor":
        return [
            {"title": c["title"], "timestamp": c["timestamp"], "comment": c.get("comment", ""),
             "size_change": c.get("sizediff"), "revid": c["revid"]}
            for c in query.get("usercontribs", [])
        ]

    pages = query.get("pages", [])
    revisions = pages[0].get("revisions", []) if pages else []
    edits = []
    # Revisions come back newest first; size change is the diff against the next-older one
    for i, rev in enumerate(revisions):
        if rev["timestamp"] < start:
            break
        older = revisions[i + 1]["size"] if i + 1 < len(revisions) else None
        edits.append({
            "user": rev.get("user"), "timestamp": rev["timestamp"], "comment": rev.get("comment", ""),
            "size_change": rev["size"] - older if older is not None else None, "revid": rev["revid"],
        })
    return edits


# ---------- Resources ----------

@mcp.resource("wikipulse://schema", title="Database schema", mime_type="text/markdown")
def schema() -> str:
    """The tables behind the tools, so the model knows what each field means."""
    return """# WikiPulse data model (TimescaleDB)

## window_stats: one row per entity per 1-minute window
- window_start / window_end: the tumbling window (event time)
- entity_type: 'page' or 'editor'
- entity_key: page title, or username / IP address for anonymous editors
- edit_count, revert_count: edits in the window; a revert is any edit whose summary matches "revert" or "undo"
- anon_ratio: share of the window's edits made by logged-out editors (temporary accounts like "~2026-12345-67"), 0 to 1
- baseline_ewma: the entity's expected edits/minute going into this window
- z_score: how many standard deviations this window's edit_count was from that baseline

## anomalies: windows that crossed a threshold
- metric: 'edit_rate' or 'revert_rate'
- value: edits/minute (edit_rate) or revert ratio (revert_rate)
- baseline: the EWMA (edit_rate) or the ratio threshold (revert_rate)
- z_score, severity: see wikipulse://detection-method

Both tables keep 2 days of data.
"""


@mcp.resource("wikipulse://detection-method", title="How anomalies are detected", mime_type="text/markdown")
def detection_method() -> str:
    """How a z-score and severity are computed, and the known blind spots."""
    return f"""# How WikiPulse flags anomalies

English Wikipedia article edits are grouped into 1-minute windows per page and per editor. The
hosted version runs as a batch job every 15 minutes, so the newest data can be up to ~30 minutes
old; the local version does the same thing continuously with Spark Structured Streaming.

**edit_rate:** each entity keeps an exponentially weighted moving average of its edits/minute
(alpha = {EWMA_ALPHA}) and variance. A window is flagged when |z| > {Z_THRESHOLD}, where
z = (edit_count - ewma) / sqrt(variance).

**revert_rate:** flagged when more than {REVERT_RATIO_THRESHOLD:.0%} of a window's edits are reverts.
Its z_score is a pseudo-score: revert_ratio / {REVERT_RATIO_THRESHOLD}.

**Severity:** |z| >= 6 is high, >= 4 is medium, otherwise low.

**Blind spots worth saying out loud when you explain a flag:**
- A brand-new entity has no history, so its first busy minutes can look extreme.
- Breaking news makes a page spike for legitimate reasons. Check the edit summaries before calling it abuse.
- Revert detection is a keyword match on the edit summary, so it misses silent reverts.
- A high anon_ratio isn't abuse by itself. Lots of good-faith edits are logged out.
"""


# ---------- Prompts ----------

@mcp.prompt(title="Investigate an anomaly")
def investigate_anomaly(anomaly_id: str) -> str:
    """Triage one flagged anomaly like a trust & safety analyst would."""
    return f"""Investigate WikiPulse anomaly {anomaly_id}.

1. Call get_anomaly_context with anomaly_id={anomaly_id}.
2. Read the actual edits: who made them, what the edit summaries say, how big the byte changes are.
3. Decide which of these it most likely is:
   - vandalism or an edit war (back-and-forth reverts, anonymous editors, large removals)
   - breaking news (many different editors adding content, summaries mention a current event)
   - bot or bulk maintenance (one editor, many similar small edits)
   - a false positive (a low baseline, or too few edits to mean anything)
4. Use the timeline to say whether it's still going or already over.

Answer with: a one-line verdict, your confidence (low / medium / high), the 2 or 3 pieces of
evidence that convinced you, and what a human moderator should do next, if anything.
Don't overstate it. If the evidence is thin, say so."""


@mcp.prompt(title="What's happening right now")
def whats_happening(minutes: str = "30") -> str:
    """A quick briefing on unusual Wikipedia activity."""
    return f"""Give me a short briefing on unusual activity on English Wikipedia over the last {minutes} minutes.

First check get_pipeline_health. If the pipeline is down or lagging, lead with that.
Then pull get_active_anomalies and get_trending_pages. Group related flags (the same page
flagged more than once, or an editor flagged alongside the page they're hitting) into stories,
and investigate the top one or two with get_anomaly_context.

Keep it under 200 words, most important first."""
