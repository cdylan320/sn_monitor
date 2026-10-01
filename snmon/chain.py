"""The hot path: race new block headers across every node, fetch all subnet prices for the
first copy of each block, and hand them to the app — all without a SCALE library.

Prices come from the chain's own `SwapRuntimeApi_current_alpha_price_all`, so they are
exactly what the runtime would quote (no re-implemented pool math).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import struct
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable

from .config import BLOCK_SECONDS
from .rpc import Rpc, RpcError

log = logging.getLogger("snmon.chain")

PRICE_ALL = "SwapRuntimeApi_current_alpha_price_all"
SIM_BUY = "SwapRuntimeApi_sim_swap_tao_for_alpha"
SIM_SELL = "SwapRuntimeApi_sim_swap_alpha_for_tao"
RAO = 1_000_000_000


# ── SCALE bits ───────────────────────────────────────────────────────────────

def compact_encode(n: int) -> bytes:
    if n < 1 << 6:
        return bytes([n << 2])
    if n < 1 << 14:
        return ((n << 2) | 1).to_bytes(2, "little")
    if n < 1 << 30:
        return ((n << 2) | 2).to_bytes(4, "little")
    b = n.to_bytes((n.bit_length() + 7) // 8, "little")
    return bytes([((len(b) - 4) << 2) | 3]) + b


def compact_decode(buf: bytes, off: int = 0) -> tuple[int, int]:
    mode = buf[off] & 3
    if mode == 0:
        return buf[off] >> 2, off + 1
    if mode == 1:
        return int.from_bytes(buf[off:off + 2], "little") >> 2, off + 2
    if mode == 2:
        return int.from_bytes(buf[off:off + 4], "little") >> 2, off + 4
    size = (buf[off] >> 2) + 4
    return int.from_bytes(buf[off + 1:off + 1 + size], "little"), off + 1 + size


def blake2_256(data: bytes) -> str:
    return "0x" + hashlib.blake2b(data, digest_size=32).hexdigest()


def header_hash(h: dict) -> str:
    """Block hash from a header notification (saves a chain_getBlockHash round trip)."""
    logs = h["digest"]["logs"]
    return blake2_256(
        bytes.fromhex(h["parentHash"][2:])
        + compact_encode(int(h["number"], 16))
        + bytes.fromhex(h["stateRoot"][2:])
        + bytes.fromhex(h["extrinsicsRoot"][2:])
        + compact_encode(len(logs))
        + b"".join(bytes.fromhex(x[2:]) for x in logs)
    )


def aura_slot_time(h: dict) -> float | None:
    """Unix time the block's Aura slot started, i.e. when the block was produced."""
    for x in h["digest"]["logs"]:
        if x.startswith("0x0661757261"):  # PreRuntime(*b"aura", ...)
            b = bytes.fromhex(x[2:])
            _, off = compact_decode(b, 5)
            return struct.unpack_from("<Q", b, off)[0] * BLOCK_SECONDS
    return None


def decode_prices(raw: bytes) -> dict[int, int]:
    """Vec<(NetUid u16, price u64 rao-per-alpha)>."""
    n, off = compact_decode(raw)
    if len(raw) - off != 10 * n:
        raise ValueError(f"unexpected price payload: {n} items in {len(raw) - off} bytes")
    return dict(struct.iter_unpack("<HQ", raw[off:]))


# ── Feed ─────────────────────────────────────────────────────────────────────

@dataclass
class Block:
    number: int
    hash: str
    prices: dict[int, int]
    source: str            # node that delivered the header first
    priced_by: str         # node that answered the price call first
    t_head: float          # perf_counter when the first header arrived
    t_priced: float        # perf_counter when prices were decoded
    wall_head: float       # unix time of first header arrival
    produced_at: float | None
    arrivals: dict[str, float] = field(default_factory=dict)  # node -> ms behind the first

    @property
    def price_ms(self) -> float:
        return (self.t_priced - self.t_head) * 1000

    @property
    def propagation_s(self) -> float | None:
        return None if self.produced_at is None else self.wall_head - self.produced_at


@dataclass
class _Race:
    number: int
    hash: str
    t_head: float
    wall_head: float
    produced_at: float | None
    source: str
    arrivals: dict[str, float]
    tried: set[str] = field(default_factory=set)
    done: bool = False


class Feed:
    """Subscribes to new best heads on every node; first copy of a block wins."""

    def __init__(self, endpoints: list[str], on_block: Callable[[Block], None]) -> None:
        self.clients = [Rpc(u) for u in endpoints]
        seen: set[str] = set()
        for i, c in enumerate(self.clients):
            if c.name in seen:
                c.name = f"{c.name}{i + 1}"
            seen.add(c.name)
        self.on_block = on_block
        self._races: OrderedDict[str, _Race] = OrderedDict()
        self.last_number = 0
        self.last_hash = ""
        self.last_wall = 0.0
        self.wins: dict[str, int] = {c.name: 0 for c in self.clients}

    def start(self) -> None:
        for c in self.clients:
            c.subscribe("chain_subscribeNewHeads", [], self._on_head)
            c.start()

    def healthy(self) -> list[Rpc]:
        """Connected nodes, fastest first."""
        up = [c for c in self.clients if c.up]
        return sorted(up, key=lambda c: c.rtt_ms if c.rtt_ms is not None else 1e9)

    def _on_head(self, header: dict, client: Rpc) -> None:
        t = time.perf_counter()
        wall = time.time()
        bh = header_hash(header)
        race = self._races.get(bh)
        if race is None:
            number = int(header["number"], 16)
            if number < self.last_number - 2:
                return
            race = _Race(number, bh, t, wall, aura_slot_time(header), client.name, {client.name: 0.0})
            self._races[bh] = race
            while len(self._races) > 64:
                self._races.popitem(last=False)
            self.wins[client.name] = self.wins.get(client.name, 0) + 1
            asyncio.create_task(self._price(race, client))
            return
        race.arrivals.setdefault(client.name, (t - race.t_head) * 1000)
        # Hedge: a second node that already has this block also gets asked.
        if not race.done and len(race.tried) < 2 and client.name not in race.tried:
            asyncio.create_task(self._price(race, client))

    async def _price(self, race: _Race, client: Rpc) -> None:
        race.tried.add(client.name)
        try:
            res = await client.call("state_call", [PRICE_ALL, "0x", race.hash], timeout=4)
        except RpcError as e:
            log.debug("price call failed on %s for #%d: %s", client.name, race.number, e)
            if not race.done:
                # Fall back to any other node that hasn't been tried yet.
                for other in self.healthy():
                    if other.name not in race.tried:
                        await asyncio.sleep(0.15)  # give it a moment to import the block
                        return await self._price(race, other)
            return
        if race.done:
            return
        race.done = True
        prices = decode_prices(bytes.fromhex(res[2:]))
        block = Block(
            number=race.number,
            hash=race.hash,
            prices=prices,
            source=race.source,
            priced_by=client.name,
            t_head=race.t_head,
            t_priced=time.perf_counter(),
            wall_head=race.wall_head,
            produced_at=race.produced_at,
            arrivals=race.arrivals,
        )
        if block.number < self.last_number:
            return  # a newer block already landed; this one is stale
        self.last_number, self.last_hash, self.last_wall = block.number, block.hash, block.wall_head
        self.on_block(block)

    # ── helpers used by other components (all raw, no SCALE library) ─────

    async def call(self, method: str, params: list, timeout: float = 5.0):
        """Call on the fastest healthy node, falling back to the next on failure."""
        last: Exception | None = None
        for c in self.healthy()[:3]:
            try:
                return await c.call(method, params, timeout=timeout)
            except RpcError as e:
                last = e
        raise RpcError(f"all nodes failed: {last}")

    async def prices_at(self, block_hash: str | None) -> dict[int, int]:
        """All prices at a block (None = node's best)."""
        params = [PRICE_ALL, "0x"] + ([block_hash] if block_hash else [])
        return decode_prices(bytes.fromhex((await self.call("state_call", params))[2:]))

    async def sim(self, buy: bool, netuid: int, amount_rao: int, at: str | None = None) -> int:
        return await simulate(self.call, buy, netuid, amount_rao, at)


async def simulate(call, buy: bool, netuid: int, amount_rao: int, at: str | None = None) -> int:
    """Amount out (rao) for swapping `amount_rao` TAO→alpha (buy) or alpha→TAO (sell), using the
    runtime's own swap simulator. `call` is any `async (method, params) -> result`."""
    params = [SIM_BUY if buy else SIM_SELL, "0x" + struct.pack("<HQ", netuid, amount_rao).hex()] + ([at] if at else [])
    res = await call("state_call", params)
    tao_amount, alpha_amount, *_ = struct.unpack("<6Q", bytes.fromhex(res[2:])[-48:])
    return alpha_amount if buy else tao_amount


async def fetch_history(rpcs: list[Rpc], first: int, last: int, per_node: int = 4) -> list[tuple[int, dict[int, int]]]:
    """Prices for blocks first..last, spread round-robin over several nodes (archive nodes
    rate-limit history; pruned nodes keep only the last few hundred blocks). Each block falls
    over to the next node on error."""
    rpcs = [r for r in rpcs if r is not None]
    sems = {r.name: asyncio.Semaphore(per_node) for r in rpcs}

    async def one(b: int):
        for attempt in range(len(rpcs) * 2):
            r = rpcs[(b + attempt) % len(rpcs)]
            try:
                async with sems[r.name]:
                    h = await r.call("chain_getBlockHash", [b], timeout=20)
                    raw = await r.call("state_call", [PRICE_ALL, "0x", h], timeout=20)
                return b, decode_prices(bytes.fromhex(raw[2:]))
            except RpcError as e:
                if "rate limit" in str(e):
                    await asyncio.sleep(1.0 + attempt * 0.5)
        raise RpcError(f"block {b}: every node failed")

    rows = await asyncio.gather(*(one(b) for b in range(first, last + 1)), return_exceptions=True)
    return sorted(r for r in rows if not isinstance(r, BaseException))
