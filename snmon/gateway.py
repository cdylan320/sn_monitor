"""Read-only Discord client for one user account (the alt that is a member of the server).

- REST for structure and history (guilds, channels with permission overwrites, roles, messages).
- Gateway websocket for real-time events: MESSAGE_CREATE, and PASSIVE_UPDATE_V1 (large guilds
  send only "channel X has a new last_message_id" to clients not focused on them; we then fetch
  the new messages over REST). Reconnects and resumes on its own.

The token is never logged.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import zlib
from typing import Awaitable, Callable

import aiohttp
import orjson

log = logging.getLogger("snmon.gateway")

API = "https://discord.com/api/v9"
GATEWAY = "wss://gateway.discord.gg/?encoding=json&v=9&compress=zlib-stream"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0.0.0 Safari/537.36")
ZLIB_SUFFIX = b"\x00\x00\xff\xff"

Handler = Callable[[str, dict], Awaitable[None] | None]


class DiscordUser:
    def __init__(self, token: str) -> None:
        self.token = token
        self.session: aiohttp.ClientSession | None = None
        self.user: dict | None = None
        self.on_event: Handler | None = None
        self.on_ready: Callable[[], Awaitable[None]] | None = None
        self.connected = False
        self.last_event = 0.0
        self._seq: int | None = None
        self._session_id: str | None = None
        self._resume_url: str | None = None
        self._lock = asyncio.Lock()
        self._limit_until = 0.0

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"Authorization": self.token, "User-Agent": UA},
        )
        self.user = await self.get("/users/@me")
        asyncio.create_task(self._run(), name="discord-gateway")

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    # ── REST ─────────────────────────────────────────────────────────────

    async def get(self, path: str, **params):
        """GET with Discord rate-limit handling. Raises on 401/403 (bad token / no access)."""
        for attempt in range(5):
            wait = self._limit_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            async with self.session.get(API + path, params=params or None) as r:
                if r.status == 429:
                    body = await r.json(content_type=None)
                    retry = float(body.get("retry_after", 1.0))
                    if body.get("global"):
                        self._limit_until = time.monotonic() + retry
                    await asyncio.sleep(retry + 0.1)
                    continue
                if r.status == 401:
                    raise PermissionError("NEWS_DISCORD_TOKEN was rejected (401) — copy a fresh token")
                if r.status == 403:
                    raise PermissionError(f"no access to {path}")
                if r.status >= 500:
                    await asyncio.sleep(1 + attempt)
                    continue
                r.raise_for_status()
                if r.headers.get("X-RateLimit-Remaining") == "0":
                    self._limit_until = time.monotonic() + float(r.headers.get("X-RateLimit-Reset-After", "1"))
                return await r.json()
        raise RuntimeError(f"GET {path} kept failing")

    # ── gateway ──────────────────────────────────────────────────────────

    async def subscribe_guild(self, guild_id: str, channel_id: str | None = None) -> None:
        """Ask for live events from a large guild (op 14, as the desktop client does)."""
        d = {"guild_id": guild_id, "typing": True, "threads": True, "activities": True}
        if channel_id:
            d["channels"] = {channel_id: [[0, 99]]}
        await self._send(14, d)

    async def _send(self, op: int, d) -> None:
        ws = getattr(self, "_ws", None)
        if ws is not None and not ws.closed:
            await ws.send_str(orjson.dumps({"op": op, "d": d}).decode())

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except PermissionError as e:
                log.error("news gateway: %s — stopping", e)
                return
            except Exception as e:
                log.warning("news gateway disconnected (%s) — reconnecting", str(e)[:120])
            self.connected = False
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

    async def _connect(self) -> None:
        url = self._resume_url or GATEWAY
        if "compress=" not in url:
            url += ("&" if "?" in url else "?") + "encoding=json&v=9&compress=zlib-stream"
        inflator = zlib.decompressobj()
        buf = bytearray()
        async with self.session.ws_connect(url, max_msg_size=0, heartbeat=None, compress=0) as ws:
            self._ws = ws
            hb: asyncio.Task | None = None
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        buf.extend(msg.data)
                        if not buf.endswith(ZLIB_SUFFIX):
                            continue
                        raw = inflator.decompress(bytes(buf))
                        buf.clear()
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        raw = msg.data
                    else:
                        break
                    p = orjson.loads(raw)
                    op, t, d = p.get("op"), p.get("t"), p.get("d")
                    if p.get("s") is not None:
                        self._seq = p["s"]
                    if op == 10:  # HELLO
                        hb = asyncio.create_task(self._heartbeat(d["heartbeat_interval"] / 1000))
                        if self._session_id:
                            await self._send(6, {"token": self.token, "session_id": self._session_id, "seq": self._seq})
                        else:
                            await self._identify()
                    elif op == 11:  # heartbeat ACK
                        pass
                    elif op == 1:
                        await self._send(1, self._seq)
                    elif op == 7:  # reconnect requested
                        break
                    elif op == 9:  # invalid session
                        if not d:
                            self._session_id = self._seq = self._resume_url = None
                        await asyncio.sleep(1 + random.random() * 4)
                        break
                    elif op == 0:
                        self.last_event = time.time()
                        if t == "READY":
                            self._session_id = d.get("session_id")
                            self._resume_url = d.get("resume_gateway_url")
                            self.connected = True
                            log.info("news gateway ready as %s", (d.get("user") or {}).get("username"))
                            if self.on_ready:
                                asyncio.create_task(self.on_ready())
                        elif t == "RESUMED":
                            self.connected = True
                            log.info("news gateway resumed")
                        if self.on_event:
                            try:
                                r = self.on_event(t, d)
                                if asyncio.iscoroutine(r):
                                    asyncio.create_task(r)
                            except Exception:
                                log.exception("news event handler failed (%s)", t)
            finally:
                if hb:
                    hb.cancel()
                self._ws = None
        if ws.close_code in (4004,):
            raise PermissionError("Discord refused the token (4004) — copy a fresh NEWS_DISCORD_TOKEN")
        if ws.close_code in (4007, 4009):
            self._session_id = self._seq = self._resume_url = None

    async def _heartbeat(self, interval: float) -> None:
        await asyncio.sleep(interval * random.random())
        while True:
            await self._send(1, self._seq)
            await asyncio.sleep(interval)

    async def _identify(self) -> None:
        await self._send(2, {
            "token": self.token,
            "capabilities": 0,
            "properties": {
                "os": "Windows", "browser": "Chrome", "device": "", "system_locale": "en-US",
                "browser_user_agent": UA, "browser_version": "126.0.0.0", "os_version": "10",
                "referrer": "", "referring_domain": "", "release_channel": "stable",
                "client_build_number": 312000, "client_event_source": None,
            },
            "presence": {"status": "invisible", "since": 0, "activities": [], "afk": False},
            "compress": False,
            "client_state": {"guild_versions": {}},
        })
