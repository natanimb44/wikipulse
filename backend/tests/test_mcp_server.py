"""
Tests for the MCP server. No database needed: get_cursor is swapped for a fake
that returns canned rows in order, and Wikipedia's API is stubbed out.

Run from backend/:  python -m pytest tests -q
"""
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

import mcp_server
from mcp_guard import MCPGuard

WINDOW = datetime(2026, 10, 5, 18, 30, tzinfo=timezone.utc)
ANOMALY = {
    "id": 42, "detected_at": WINDOW, "window_start": WINDOW, "entity_type": "page",
    "entity_key": "Taylor Swift", "metric": "edit_rate", "value": 14.0, "baseline": 1.2,
    "z_score": 7.9, "severity": "high",
}


class FakeCursor:
    """Hands back one canned result per execute() call, and records the SQL params."""

    def __init__(self, results: list):
        self.results = list(results)
        self.calls: list[tuple] = []
        self._current = None

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self._current = self.results.pop(0)

    def fetchall(self):
        return self._current

    def fetchone(self):
        return self._current


@pytest.fixture
def fake_db(monkeypatch):
    def install(*results):
        cur = FakeCursor(results)

        @contextmanager
        def get_cursor():
            yield cur

        monkeypatch.setattr(mcp_server, "get_cursor", get_cursor)
        return cur

    return install


async def test_exposes_tools_resources_and_prompts():
    async with Client(mcp_server.mcp) as client:
        tools = (await client.list_tools()).tools
        resources = (await client.list_resources()).resources
        prompts = (await client.list_prompts()).prompts

    assert {t.name for t in tools} == {
        "get_active_anomalies", "get_entity_timeline", "get_trending_pages",
        "get_anomaly_context", "get_pipeline_health",
    }
    # Nothing an agent can call here should be able to change data
    assert all(t.annotations.read_only_hint for t in tools)
    assert all(t.description for t in tools)
    assert {str(r.uri) for r in resources} == {"wikipulse://schema", "wikipulse://detection-method"}
    assert {p.name for p in prompts} == {"investigate_anomaly", "whats_happening"}


async def test_active_anomalies_filters_and_clamps(fake_db):
    cur = fake_db([ANOMALY])
    async with Client(mcp_server.mcp) as client:
        result = await client.call_tool("get_active_anomalies", {"minutes": 99999, "severity": "high", "limit": 5000})

    assert not result.is_error
    rows = result.structured_content["result"]
    assert rows[0]["entity_key"] == "Taylor Swift"
    assert rows[0]["detected_at"] == WINDOW.isoformat()
    _, params = cur.calls[0]
    assert params == (1440, "high", "high", None, None, 100)


async def test_rejects_bad_severity():
    async with Client(mcp_server.mcp) as client:
        result = await client.call_tool("get_active_anomalies", {"severity": "critical"})
    assert result.is_error


async def test_anomaly_context_unknown_id(fake_db):
    fake_db(None)
    async with Client(mcp_server.mcp) as client:
        result = await client.call_tool("get_anomaly_context", {"anomaly_id": 999})
    assert result.is_error
    assert "No anomaly with id 999" in result.content[0].text


async def test_anomaly_context_survives_wikipedia_outage(fake_db, monkeypatch):
    fake_db(ANOMALY, [{"window_start": WINDOW, "edit_count": 14}], [])

    def down(*args, **kwargs):
        raise httpx.ConnectError("no route")

    monkeypatch.setattr(mcp_server.httpx, "get", down)
    async with Client(mcp_server.mcp) as client:
        result = await client.call_tool("get_anomaly_context", {"anomaly_id": 42})

    assert not result.is_error
    body = result.structured_content
    assert body["anomaly"]["id"] == 42
    assert len(body["timeline"]) == 1
    assert "error" in body["wikipedia_edits_in_window"]


def test_wikipedia_page_edits_compute_size_change(monkeypatch):
    captured = {}

    def fake_get(url, params, headers, timeout):
        captured.update(params=params, headers=headers)
        return httpx.Response(200, request=httpx.Request("GET", url), json={"query": {"pages": [{"revisions": [
            {"revid": 3, "user": "1.2.3.4", "timestamp": "2026-10-05T18:30:40Z", "comment": "", "size": 500},
            {"revid": 2, "user": "Editor", "timestamp": "2026-10-05T18:30:10Z", "comment": "Undid revision 1", "size": 9000},
            {"revid": 1, "user": "1.2.3.4", "timestamp": "2026-10-05T18:29:50Z", "comment": "", "size": 500},
            # Before the padded window: only used for revid 1's size change, not returned
            {"revid": 0, "user": "Someone", "timestamp": "2026-10-05T18:10:00Z", "comment": "", "size": 480},
        ]}]}})

    monkeypatch.setattr(mcp_server.httpx, "get", fake_get)
    edits = mcp_server._fetch_wikipedia_edits("page", "Taylor Swift", WINDOW)

    assert [e["size_change"] for e in edits] == [-8500, 8500, 20]
    assert captured["params"]["titles"] == "Taylor Swift"
    # Newest-first, so rvstart is the later bound
    assert captured["params"]["rvstart"] == "2026-10-05T18:32:00Z"
    assert "WikiPulse" in captured["headers"]["User-Agent"]


async def test_pipeline_health_reports_down_when_no_data(fake_db):
    fake_db({"latest_window": None, "seconds_behind": None}, {"edits_last_5m": 0, "pages_last_5m": 0}, [])
    async with Client(mcp_server.mcp) as client:
        result = await client.call_tool("get_pipeline_health", {})
    assert result.structured_content["status"] == "down"
    assert result.structured_content["anomalies_last_hour"] == {"high": 0, "medium": 0, "low": 0}


async def test_prompt_and_resource_render():
    async with Client(mcp_server.mcp) as client:
        prompt = await client.get_prompt("investigate_anomaly", {"anomaly_id": "42"})
        method = await client.read_resource("wikipulse://detection-method")
    assert "anomaly_id=42" in prompt.messages[0].content.text
    assert "|z| >= 6 is high" in method.contents[0].text


# ---------- Over real HTTP: the deployed path (FastAPI mount + key + rate limit) ----------

@pytest.fixture(scope="module")
def live_backend():
    import main

    # One server for all HTTP tests: the MCP session manager can only be started
    # once per process, so a second uvicorn startup would fail its lifespan.
    # Fresh guard with a known key and a small limit, so nothing depends on env vars.
    main.app.router.routes[-1].app = MCPGuard(main.mcp_app, api_key="test-key", per_minute=10)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(main, "healthcheck", lambda: True)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(main.app, host="127.0.0.1", port=port, log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 10
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                pytest.fail("backend didn't start")
            time.sleep(0.05)
        yield f"http://127.0.0.1:{port}"
        server.should_exit = True
        thread.join(5)


def test_http_requires_api_key_and_keeps_rest_api(live_backend):
    assert httpx.get(f"{live_backend}/health").json() == {"status": "ok"}
    assert httpx.post(f"{live_backend}/mcp", json={}).status_code == 401
    bad = httpx.post(f"{live_backend}/mcp", json={}, headers={"Authorization": "Bearer nope"})
    assert bad.status_code == 401


async def test_http_end_to_end_tool_call(live_backend, fake_db):
    fake_db([ANOMALY])
    async with httpx2.AsyncClient(headers={"Authorization": "Bearer test-key"}) as http:
        async with Client(streamable_http_client(f"{live_backend}/mcp", http_client=http)) as client:
            result = await client.call_tool("get_active_anomalies", {"severity": "high"})
    assert not result.is_error
    assert result.structured_content["result"][0]["id"] == 42


def test_http_rate_limit(live_backend):
    headers = {"Authorization": "Bearer test-key", "X-Forwarded-For": "9.9.9.9"}
    codes = [httpx.post(f"{live_backend}/mcp", json={}, headers=headers).status_code for _ in range(12)]
    assert 429 not in codes[:10]
    assert codes[10:] == [429, 429]
