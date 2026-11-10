"""The price to register a subnet, as the chain computes it (subtensor `get_network_lock_cost`), its history, and its chart.

    cost(block) = max(min_lock, 2 × L − L / I × (block − B))

L, B: the last registration's lock and block (each registration doubles the price); I: the reduction interval
(NetworkLockReductionInterval × the block emission), read at the block the price is asked at: 28,800 blocks (4 days)
until the halving at #9471021 (9 Nov ~21:26 CET) — the price fell from ~296 τ to ~87 τ in that one block — and 14,400
(2 days) since. Checked 10 Nov: SN78 waw locked 85.94 τ at #9471098, 77 blocks after the halving; at #9472391 the chain
said 164.17 τ = 2 × 85.94 − 85.94 / 14400 × 1293. So past and coming days are exact from the registrations and the
interval's changes; the live cost (runtime API) only checks the interval (a later halving is added when it shows).
"""
from __future__ import annotations

import asyncio
import io
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

from PIL import Image, ImageDraw

from .chart import BG, GRID, TEXT, TEXT_HI, UP, _font

BLOCK_S = 12
HALVING_BLOCK = 9471021   # the reduction interval went 28,800 → 14,400 (found on chain, 10 Nov)
AMBER = (245, 158, 11)
VIOLET = (167, 139, 250)


@dataclass
class Registration:
    block: int          # the block it was queued in (the price's "last lock block")
    tao: float          # what it locked
    ts: float = 0.0     # that block's time
    netuid: int | None = None
    name: str = ""


class RegCost:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.regs: list[Registration] = []
        # blocks for the price to fall by one lock, from a block on: [[from block, interval], …] (the halving halved it)
        self.intervals: list[list[int]] = [[0, 28800], [HALVING_BLOCK, 14400]]
        self.min_lock = 1.0
        self.rate_limit = 14400        # blocks between two registrations (NetworkRateLimit)
        try:
            d = json.loads(path.read_text())
            self.regs = [Registration(**r) for r in d.get("regs", [])]
            self.intervals = [[int(b), int(i)] for b, i in d.get("intervals") or self.intervals]
            self.min_lock = float(d.get("min_lock") or self.min_lock)
            self.rate_limit = int(d.get("rate_limit") or self.rate_limit)
        except (OSError, ValueError, TypeError):
            pass

    def save(self) -> None:
        try:
            self.path.write_text(json.dumps({"regs": [asdict(r) for r in self.regs], "intervals": self.intervals,
                                             "min_lock": self.min_lock, "rate_limit": self.rate_limit}))
        except OSError:
            pass

    def add(self, block: int, tao: float, ts: float, netuid: int | None = None, name: str = "") -> Registration:
        r = next((r for r in self.regs if r.block == block), None)
        if r is None:
            r = Registration(block, tao, ts, netuid, name)
            self.regs.append(r)
            self.regs.sort(key=lambda r: r.block)
        else:
            r.tao, r.ts = tao or r.tao, ts or r.ts
            r.netuid, r.name = netuid if netuid is not None else r.netuid, name or r.name
        self.save()
        return r

    def named(self, netuid: int, name: str, registered_at: int) -> None:
        """A new subnet came in: the registration queued just before it (≤ 2 h) gets its netuid and name."""
        cands = [r for r in self.regs if r.block <= registered_at and registered_at - r.block <= 600]
        if cands:
            r = cands[-1]
            r.netuid, r.name = netuid, name or r.name
            self.save()

    def last(self, before: int | None = None) -> Registration | None:
        regs = [r for r in self.regs if before is None or r.block < before]
        return regs[-1] if regs else None

    def interval_at(self, block: int) -> int:
        return next(i for b, i in reversed(self.intervals) if b <= block) if self.intervals else 14400

    def cost_at(self, block: int) -> float | None:
        r = self.last(block + 1)
        if r is None:
            return None
        return max(self.min_lock, 2 * r.tao - r.tao / self.interval_at(block) * (block - r.block))

    def calibrate(self, head: int, cost_now: float) -> None:
        """Check the interval with the live cost (2L − cost = L / I × (head − B)); a new one (a halving) is kept from now."""
        r = self.last(head + 1)
        if r is None or head - r.block < 300 or cost_now <= self.min_lock + 0.01:
            return
        fall = 2 * r.tao - cost_now
        if fall > 0:
            i = round(r.tao * (head - r.block) / fall / 100) * 100
            cur = self.interval_at(head)
            if 1000 <= i <= 200000 and abs(i - cur) > cur * 0.05:
                self.intervals.append([head, i])
                self.save()

    def floor_block(self) -> int | None:
        """When the price reaches the minimum if no one registers."""
        r = self.last()
        if r is None:
            return None
        return r.block + math.ceil((2 * r.tao - self.min_lock) / (r.tao / self.interval_at(r.block + 1)))

    def next_allowed(self) -> int | None:
        r = self.last()
        return r.block + self.rate_limit if r else None


def render(rc: RegCost, head: int, now: float, days_back: float = 10, days_ahead: float = 2,
           names: dict[int, str] | None = None) -> bytes:
    """The price over the last `days_back` days and the next `days_ahead` (if no one registers): a solid line, then
    dashed; a dot on each registration with what it paid; the price now."""
    W, H, S = 1000, 420, 2
    L, R, T, B = 70, 110, 58, 44
    img = Image.new("RGB", (W * S, H * S), BG)
    d = ImageDraw.Draw(img)
    f_t, f_s, f_b = _font(17, True), _font(12), _font(13, True)
    b0, b1 = head - int(days_back * 86400 / BLOCK_S), head + int(days_ahead * 86400 / BLOCK_S)
    step = max(1, (b1 - b0) // 600)
    marks = {b for r in rc.regs for b in (r.block - 1, r.block)} | {b for b, _ in rc.intervals for b in (b - 1, b)}
    blocks = sorted(set(range(b0, b1 + 1, step)) | {b for b in marks if b0 <= b <= b1} | {head})
    pts = [(b, rc.cost_at(b)) for b in blocks]
    pts = [(b, c) for b, c in pts if c is not None]
    if not pts:
        d.text((L * S, T * S), "no registration known yet", font=f_t, fill=TEXT)
        return _png(img, W, H)
    regs = [r for r in rc.regs if b0 <= r.block <= head]
    hi = max([c for _, c in pts] + [2 * r.tao for r in regs] + [10.0]) * 1.08
    tick = next(t for t in (5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 5000) if hi / t <= 6)
    top = math.ceil(hi / tick) * tick
    X = lambda b: (L + (b - b0) / (b1 - b0) * (W - L - R)) * S
    Y = lambda c: (T + (1 - c / top) * (H - T - B)) * S
    for k in range(int(top / tick) + 1):                         # grid: τ
        c = k * tick
        d.line([(L * S, Y(c)), ((W - R) * S, Y(c))], fill=GRID, width=S)
        d.text(((L - 8) * S, Y(c)), f"{c:,.0f} τ", font=f_s, fill=TEXT, anchor="rm")
    for k in range(int(days_back) + int(days_ahead) + 1):        # grid: days
        b = head - int((days_back - k) * 86400 / BLOCK_S)
        if b0 <= b <= b1:
            d.line([(X(b), T * S), (X(b), (H - B) * S)], fill=GRID, width=S)
            dd = k - days_back
            label = "now" if dd == 0 else f"{dd:+g}d"
            d.text((X(b), (H - B + 8) * S), label, font=f_s, fill=TEXT_HI if dd == 0 else TEXT, anchor="mt")
    past = [(X(b), Y(c)) for b, c in pts if b <= head]
    d.line(past, fill=AMBER, width=3 * S, joint="curve")
    fut = [(X(b), Y(c)) for b, c in pts if b >= head]
    for i, ((x0, y0), (x1, y1)) in enumerate(zip(fut, fut[1:])):  # the coming days: dashed
        if i % 3 != 2:
            d.line([(x0, y0), (x1, y1)], fill=AMBER, width=2 * S)
    for b, i in rc.intervals:                                    # a halving: the price falls twice as fast from there
        if b0 <= b <= head and b:
            d.line([(X(b), T * S), (X(b), (H - B) * S)], fill=TEXT, width=S)
            d.text((X(b) - 6 * S, (T + 4) * S), f"halving\nfalls in {i * BLOCK_S / 86400:g} days", font=f_s, fill=TEXT,
                   anchor="ra", spacing=2 * S, align="right")
    for r in regs:                                               # each registration: what it paid
        x, y = X(r.block), Y(r.tao)
        d.ellipse([x - 6 * S, y - 6 * S, x + 6 * S, y + 6 * S], fill=VIOLET)
        who = (names or {}).get(r.netuid) or r.name
        tag = f"SN{r.netuid} {who}".strip() if r.netuid is not None else (who or "registered")
        d.text((x + 9 * S, y + 2 * S), f"{tag}\n{r.tao:,.2f} τ", font=f_s, fill=VIOLET, anchor="la", spacing=2 * S)
    c_now = rc.cost_at(head)
    if c_now is not None:
        x, y = X(head), Y(c_now)
        d.ellipse([x - 7 * S, y - 7 * S, x + 7 * S, y + 7 * S], fill=UP)
        d.text(((W - R + 10) * S, y), f"{c_now:,.2f} τ\nnow", font=f_b, fill=UP, anchor="lm", spacing=2 * S)
    d.text((L * S, 18 * S), "Price to register a subnet", font=f_t, fill=TEXT_HI)
    d.text(((W - R) * S, 22 * S), "● paid at registration   ━ price   ┅ if no one registers", font=f_s, fill=TEXT,
           anchor="ra")
    return _png(img, W, H)


def _png(img: Image.Image, w: int, h: int) -> bytes:
    out = io.BytesIO()
    img.resize((w, h), Image.LANCZOS).save(out, "PNG", optimize=True)
    return out.getvalue()


async def seed(rc: RegCost, url: str, days: float = 15, most: int = 12) -> int:
    """Fill the history from the chain (once, when it is empty): walk back registration by registration — the last
    lock (NetworkLastLockCost) and its block (LastRateLimitedBlock[NetworkLastRegistered]) as they were just before it.
    Its own connection to an archive node: old state, and nothing in the way of the monitor's block reads."""
    from async_substrate_interface import AsyncSubstrateInterface
    added = 0
    async with AsyncSubstrateInterface(url, ss58_format=42, chain_name="Bittensor") as s:
        head = (await s.get_block_header())["header"]["number"]
        b, stop = head, head - int(days * 86400 / BLOCK_S)
        for _ in range(most):
            h = await s.get_block_hash(b)
            lock = (await s.query("SubtensorModule", "NetworkLastLockCost", [], block_hash=h)).value
            at = (await s.query("SubtensorModule", "LastRateLimitedBlock", ["NetworkLastRegistered"], block_hash=h)).value
            if not at or not lock or at < stop:
                break
            ts = (await s.query("Timestamp", "Now", [], block_hash=await s.get_block_hash(at))).value / 1000
            rc.add(int(at), int(lock) / 1e9, ts)
            added += 1
            b = int(at) - 1
            await asyncio.sleep(0.5)
        mn = await s.query("SubtensorModule", "NetworkMinLockCost", [])
        rl = await s.query("SubtensorModule", "NetworkRateLimit", [])
        rc.min_lock = int(getattr(mn, "value", mn) or 1e9) / 1e9
        rc.rate_limit = int(getattr(rl, "value", rl) or rc.rate_limit)
        rc.save()
    return added
