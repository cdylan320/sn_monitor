"""News feed for the trading bots (sn_trader's news trading).

Every news card the monitor posts about a subnet is also published here the moment it is detected — before the
Discord card goes out — as one Server-Sent Event:

    GET /news   Authorization: Bearer <NEWS_FEED_TOKEN>   →   text/event-stream
    data: {"id": 17, "netuid": 92, "kind": "announcement", "who": "team", "source": "discord #92-luis",
           "text": "…", "url": "https://discord.com/channels/…", "posted": 1792000000.1, "sent": 1792000000.4,
           "price": 20000000}

kind: announcement (@everyone) · team (a team / staff update) · x_team (an X post shared by the team) ·
      x_other (an X post shared by anyone else) · x_account (a post by a watched X account, e.g. const)
price: the subnet's price (rao per alpha) when the news was seen.

Only news detected after a bot connected is sent: a bot must never trade old news, so after a restart or an outage it
starts from now. A comment line every 15 s keeps the connection open and lets the bot notice a dead one.
GET /health (no token) → {"ok": true, "bots": <connected>}.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import time
from datetime import datetime

from aiohttp import web

from . import fmt

PING_SECONDS = 15
QUEUE = 200


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class NewsFeed:
    def __init__(self, port: int, token: str, host: str = "0.0.0.0") -> None:
        self.port, self.token, self.host = port, token, host
        self._clients: set[asyncio.Queue] = set()
        self._seq = 0
        self._runner: web.AppRunner | None = None

    @property
    def bots(self) -> int:
        return len(self._clients)

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/news", self._stream)
        app.router.add_get("/health", self._health)
        self._runner = web.AppRunner(app, access_log=None, shutdown_timeout=2)
        await self._runner.setup()
        await web.TCPSite(self._runner, self.host, self.port).start()

    async def close(self) -> None:
        for q in list(self._clients):       # end every open stream first: a stream never ends by itself, and the
            try:                            # server would wait for them on shutdown
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        if self._runner is not None:
            await self._runner.cleanup()

    def publish(self, event: dict) -> int:
        """Send one news event to every connected bot. → how many bots it went to."""
        self._seq += 1
        ev = {"id": self._seq, "sent": time.time(), **event}
        data = f"id: {self._seq}\ndata: {json.dumps(ev, separators=(',', ':'), ensure_ascii=False)}\n\n".encode()
        for q in list(self._clients):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:   # a bot that stopped reading loses events instead of growing memory
                pass
        return len(self._clients)

    def _authorised(self, request: web.Request) -> bool:
        got = request.headers.get("Authorization", "")
        got = got[7:] if got.startswith("Bearer ") else request.query.get("token", "")
        return bool(self.token) and hmac.compare_digest(got.encode(), self.token.encode())

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "bots": self.bots})

    async def _stream(self, request: web.Request) -> web.StreamResponse:
        if not self._authorised(request):
            return web.Response(status=401, text="bad token")
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache",
                                           "X-Accel-Buffering": "no"})
        await resp.prepare(request)
        q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE)
        self._clients.add(q)
        print(fmt.dim(f"{_now()} ── news feed: a trading bot connected from {request.remote} ({self.bots} connected)"))
        try:
            await resp.write(b": connected\n\n")
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), PING_SECONDS)
                except asyncio.TimeoutError:
                    data = b": ping\n\n"
                if data is None:            # the monitor is shutting down
                    break
                await resp.write(data)
        except (ConnectionResetError, ConnectionError):
            pass
        finally:
            self._clients.discard(q)
            print(fmt.dim(f"{_now()} ── news feed: a trading bot disconnected ({self.bots} connected)"))
        return resp
