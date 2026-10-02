"""Discord webhook client tuned for latency.

- One long-lived HTTPS connection kept warm (a cheap GET every 20s) so an alert never pays
  for DNS + TCP + TLS (~150-300 ms).
- Alerts go out immediately; follow-up edits (trade attribution, pending→landed) go through a
  background lane that never spends the last rate-limit token, so alerts always have room.
- Honors Discord rate-limit headers and 429 retry_after.
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp
import orjson

log = logging.getLogger("snmon.discord")

WARM_URL = "https://discord.com/api/v10/gateway"


class Discord:
    def __init__(self, webhook_url: str, dry_run: bool = False) -> None:
        self.dry_run = dry_run
        self.url = webhook_url.split("?")[0].rstrip("/")
        self.session: aiohttp.ClientSession | None = None
        self._remaining = 5
        self._reset_at = 0.0
        self._edits: asyncio.Queue = asyncio.Queue()
        self.last_ms: float | None = None

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(keepalive_timeout=120, ttl_dns_cache=3600, limit=8),
            timeout=aiohttp.ClientTimeout(total=15),
            json_serialize=lambda o: orjson.dumps(o).decode(),
        )
        await self._warm()
        asyncio.create_task(self._keep_warm(), name="discord:warm")
        asyncio.create_task(self._edit_worker(), name="discord:edits")

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    async def check(self) -> dict:
        async with self.session.get(self.url) as r:
            r.raise_for_status()
            return await r.json()

    async def send(self, payload: dict, files: dict[str, bytes] | None = None) -> str | None:
        """Post a message now; returns its id (for later edits). `files`: filename → bytes."""
        if self.dry_run:
            return None
        data = await self._request("POST", f"{self.url}?wait=true", payload, files)
        return data.get("id") if data else None

    async def edit(self, message_id: str, payload: dict) -> bool:
        """Edit now; False if the message is gone (deleted)."""
        if self.dry_run:
            return True
        return await self._request("PATCH", f"{self.url}/messages/{message_id}", payload) is not None

    async def delete(self, message_id: str) -> None:
        if not self.dry_run:
            await self._request("DELETE", f"{self.url}/messages/{message_id}", None)

    def edit_later(self, message_id: str, payload: dict, files: dict[str, bytes] | None = None) -> None:
        """Queue an edit. With `files`, the message's attachments are replaced by these."""
        if files:
            payload = {**payload, "attachments": [{"id": i, "filename": name} for i, name in enumerate(files)]}
        self._edits.put_nowait((message_id, payload, files))

    # ── internals ────────────────────────────────────────────────────────

    async def _warm(self) -> None:
        try:
            async with self.session.get(WARM_URL) as r:
                await r.read()
        except Exception as e:
            log.debug("warm-up failed: %s", e)

    async def _keep_warm(self) -> None:
        while True:
            await asyncio.sleep(20)
            await self._warm()

    async def _edit_worker(self) -> None:
        while True:
            message_id, payload, files = await self._edits.get()
            # Leave at least one token for a real-time alert.
            while self._remaining <= 1 and time.monotonic() < self._reset_at:
                await asyncio.sleep(max(0.05, self._reset_at - time.monotonic()))
            try:
                await self._request("PATCH", f"{self.url}/messages/{message_id}", payload, files)
            except Exception:
                log.exception("edit failed")

    async def _request(self, method: str, url: str, payload: dict, files: dict[str, bytes] | None = None) -> dict | None:
        for attempt in range(6):
            if self._remaining <= 0 and time.monotonic() < self._reset_at:
                await asyncio.sleep(self._reset_at - time.monotonic())
            t0 = time.perf_counter()
            if files:  # multipart: JSON payload + attachments (rebuilt per attempt; FormData is single-use)
                body = aiohttp.FormData()
                body.add_field("payload_json", orjson.dumps(payload).decode(), content_type="application/json")
                for i, (name, blob) in enumerate(files.items()):
                    body.add_field(f"files[{i}]", blob, filename=name, content_type="image/png")
                kw = {"data": body}
            else:
                kw = {"json": payload}
            try:
                async with self.session.request(method, url, **kw) as r:
                    self._track(r.headers)
                    if r.status == 429:
                        body = await r.json(content_type=None)
                        wait = float(body.get("retry_after", 1.0))
                        log.warning("Discord rate limited — retrying in %.2fs", wait)
                        await asyncio.sleep(wait)
                        continue
                    if r.status >= 500:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    if r.status >= 400:
                        log.error("Discord %s %s → %s %s", method, url.rsplit("/", 1)[-1][:24], r.status, (await r.text())[:500])
                        return None
                    self.last_ms = (time.perf_counter() - t0) * 1000
                    return await r.json(content_type=None) if r.status != 204 else {}
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning("Discord request error (%s), retrying", e)
                await asyncio.sleep(0.3 * (attempt + 1))
        log.error("Discord %s gave up after retries", method)
        return None

    def _track(self, h) -> None:
        try:
            if "X-RateLimit-Remaining" in h:
                self._remaining = int(h["X-RateLimit-Remaining"])
            if "X-RateLimit-Reset-After" in h:
                self._reset_at = time.monotonic() + float(h["X-RateLimit-Reset-After"])
        except ValueError:
            pass
