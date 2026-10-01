"""Minimal, fast JSON-RPC-over-websocket client for a substrate node.

One instance per endpoint. It reconnects forever, re-establishes subscriptions after a
reconnect, and dispatches subscription notifications synchronously from the reader so
there is no queue between "bytes arrived" and "callback ran".
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from typing import Any, Callable
from urllib.parse import urlparse

import orjson
from websockets.asyncio.client import connect

log = logging.getLogger("snmon.rpc")

Callback = Callable[[Any, "Rpc"], None]


class RpcError(Exception):
    pass


def short_name(url: str) -> str:
    """lite.chain.opentensor.ai → lite, entrypoint-finney.opentensor.ai → finney, *.onfinality.io → onfinality"""
    host = urlparse(url).hostname or url
    labels = host.split(".")
    if len(labels) >= 2 and labels[-2] != "opentensor":
        return labels[-2]
    return labels[0].removeprefix("entrypoint-")


class Rpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self.name = short_name(url)
        self._ids = itertools.count(1)
        self._ws = None
        self._pending: dict[int, asyncio.Future] = {}
        self._sub_requests: dict[int, Callback] = {}
        self._subs: dict[str, Callback] = {}
        self._wanted: list[tuple[str, list, Callback]] = []
        self._connected = asyncio.Event()
        self.rtt_ms: float | None = None
        self.last_error: str | None = None
        self._task: asyncio.Task | None = None

    @property
    def up(self) -> bool:
        return self._connected.is_set()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"rpc:{self.name}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._ws is not None:
            await self._ws.close()

    def subscribe(self, method: str, params: list, callback: Callback) -> None:
        """Keep a subscription alive across reconnects."""
        self._wanted.append((method, params, callback))
        if self.up:
            asyncio.create_task(self._send_subscribe(method, params, callback))

    async def wait_up(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def call(self, method: str, params: list | tuple = (), timeout: float = 10.0) -> Any:
        if not self.up and not await self.wait_up(timeout):
            raise RpcError(f"{self.name}: not connected")
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        t0 = time.perf_counter()
        try:
            await self._ws.send(orjson.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": list(params)}).decode())
            result = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            raise RpcError(f"{self.name}: {method} timed out") from None
        finally:
            self._pending.pop(rid, None)
        ms = (time.perf_counter() - t0) * 1000
        self.rtt_ms = ms if self.rtt_ms is None else self.rtt_ms * 0.8 + ms * 0.2
        return result

    async def _send_subscribe(self, method: str, params: list, callback: Callback) -> None:
        rid = next(self._ids)
        self._sub_requests[rid] = callback
        try:
            await self._ws.send(orjson.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}).decode())
        except Exception as e:  # reconnect loop will resubscribe
            self._sub_requests.pop(rid, None)
            log.debug("%s subscribe send failed: %s", self.name, e)

    async def _run(self) -> None:
        backoff = 0.5
        while True:
            try:
                async with connect(
                    self.url,
                    max_size=None,
                    open_timeout=10,
                    close_timeout=2,
                    ping_interval=15,
                    ping_timeout=15,
                ) as ws:
                    self._ws = ws
                    self._subs.clear()
                    self._sub_requests.clear()
                    self._connected.set()
                    self.last_error = None
                    backoff = 0.5
                    log.info("connected  %s", self.name)
                    for method, params, cb in self._wanted:
                        await self._send_subscribe(method, params, cb)
                    await self._read(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"[:160]
            finally:
                was_up = self._connected.is_set()
                self._connected.clear()
                self._ws = None
                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(RpcError(f"{self.name}: connection lost"))
                self._pending.clear()
            if was_up:
                log.warning("disconnected %s (%s) — reconnecting", self.name, self.last_error or "closed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)

    async def _read(self, ws) -> None:
        pending, sub_requests, subs = self._pending, self._sub_requests, self._subs
        async for raw in ws:
            msg = orjson.loads(raw)
            rid = msg.get("id")
            if rid is not None:
                cb = sub_requests.pop(rid, None)
                if cb is not None:
                    if "result" in msg:
                        subs[msg["result"]] = cb
                    else:
                        log.error("%s subscribe failed: %s", self.name, msg.get("error"))
                    continue
                fut = pending.get(rid)
                if fut is not None and not fut.done():
                    if "error" in msg:
                        fut.set_exception(RpcError(f"{self.name}: {msg['error'].get('message')}"))
                    else:
                        fut.set_result(msg.get("result"))
                continue
            params = msg.get("params")
            if params:
                cb = subs.get(params.get("subscription"))
                if cb is not None:
                    try:
                        cb(params.get("result"), self)
                    except Exception:
                        log.exception("%s subscription callback failed", self.name)
