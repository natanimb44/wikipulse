"""
API key check + rate limit in front of the MCP endpoint only. The REST API the
dashboard uses stays open; the MCP endpoint is meant for agents, so it gets a key.

If MCP_API_KEY isn't set (local dev), the key check is skipped.
"""
import hmac
import json
import os
import time
from collections import defaultdict, deque

from starlette.types import ASGIApp, Receive, Scope, Send

MCP_API_KEY = os.environ.get("MCP_API_KEY")
RATE_LIMIT_PER_MIN = int(os.environ.get("MCP_RATE_LIMIT_PER_MIN", "60"))


class MCPGuard:
    def __init__(self, app: ASGIApp, api_key: str | None = MCP_API_KEY, per_minute: int = RATE_LIMIT_PER_MIN):
        self.app = app
        self.api_key = api_key
        self.per_minute = per_minute
        # In-memory sliding window per client. Fine for one backend replica,
        # would need Redis or similar if this ever ran on more than one.
        self.hits: dict[str, deque[float]] = defaultdict(deque)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}

        if self.api_key:
            token = headers.get("authorization", "").removeprefix("Bearer ").strip()
            if not hmac.compare_digest(token.encode(), self.api_key.encode()):
                await _error(send, 401, "missing or invalid API key", {"www-authenticate": "Bearer"})
                return

        # Railway sits behind a proxy, so the real client IP is in X-Forwarded-For
        client = headers.get("x-forwarded-for", "").split(",")[0].strip() or (scope.get("client") or ("unknown",))[0]
        now = time.monotonic()
        window = self.hits[client]
        while window and now - window[0] > 60:
            window.popleft()
        if len(window) >= self.per_minute:
            retry_after = int(60 - (now - window[0])) + 1
            await _error(send, 429, "rate limit exceeded", {"retry-after": str(retry_after)})
            return
        window.append(now)

        await self.app(scope, receive, send)


async def _error(send: Send, status: int, message: str, extra_headers: dict[str, str]) -> None:
    body = json.dumps({"error": message}).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    headers += [(k.encode(), v.encode()) for k, v in extra_headers.items()]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})
