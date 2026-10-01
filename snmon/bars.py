"""5-minute OHLC bars for every subnet: live from every block, backfilled from archive nodes,
persisted in SQLite so a restart never re-downloads history.

Bars are keyed by block number (bucket = block // 25, i.e. 25 blocks = 5 minutes) so they line
up exactly with the chain, not with a wall clock. Data lives in numpy ring buffers shaped
[time, netuid], which lets the trend detector fit every subnet in one vectorised pass.

Every (bar, subnet) cell knows whether it is *exact* — built from every block — or *sampled*
(one on-chain price at the bar's last block, from backfill). Live bars are exact; `exactify()`
rebuilds a subnet's history from every block on demand (used for alert charts).
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
import warnings
from pathlib import Path

import numpy as np

from .chain import PRICE_ALL, decode_prices
from .rpc import Rpc, RpcError

log = logging.getLogger("snmon.bars")

BUCKET = 25            # blocks per bar (5 minutes)
CAPACITY = 8 * 288     # 8 days of 5-minute bars
NMAX = 512             # max netuid + 1 (float32 → ~19 MB for 8 days)


class Bars:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.o = np.full((CAPACITY, NMAX), np.nan, dtype=np.float32)
        self.h = np.full((CAPACITY, NMAX), np.nan, dtype=np.float32)
        self.l = np.full((CAPACITY, NMAX), np.nan, dtype=np.float32)
        self.c = np.full((CAPACITY, NMAX), np.nan, dtype=np.float32)
        self.bucket_of = np.full(CAPACITY, -1, dtype=np.int64)  # which bucket each row holds
        self.live = np.zeros(CAPACITY, dtype=bool)               # row is being built from live blocks
        self.partial = np.zeros(CAPACITY, dtype=bool)            # live row that started mid-bar (restart)
        self.exact = np.zeros((CAPACITY, NMAX), dtype=bool)      # cell holds every block's price
        self.last_bucket = -1
        self.ncols = 1  # 1 + highest netuid seen; series are sliced to this
        self.coverage = np.zeros(0)  # set by closes()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS bars (bucket INTEGER, netuid INTEGER, o REAL, h REAL, "
                         "l REAL, c REAL, live INTEGER, PRIMARY KEY (bucket, netuid))")

    # ── rows ─────────────────────────────────────────────────────────────

    def _row(self, bucket: int) -> int:
        i = bucket % CAPACITY
        if self.bucket_of[i] != bucket:
            self.o[i] = self.h[i] = self.l[i] = self.c[i] = np.nan
            self.bucket_of[i] = bucket
            self.live[i] = False
            self.partial[i] = False
            self.exact[i] = False
        return i

    def has(self, bucket: int) -> bool:
        return self.bucket_of[bucket % CAPACITY] == bucket

    def update(self, block: int, prices: dict[int, int]) -> None:
        """Live: fold one block's prices into its 5-minute bar."""
        bucket = block // BUCKET
        if bucket > self.last_bucket >= 0:
            self._persist(self.last_bucket)
        fresh = not self.has(bucket) or not self.live[bucket % CAPACITY]
        i = self._row(bucket)
        idx = np.fromiter(prices.keys(), dtype=np.int64, count=len(prices))
        val = np.fromiter(prices.values(), dtype=np.float64, count=len(prices)) / 1e9
        keep = (idx < NMAX) & (val > 0)
        idx, val = idx[keep], val[keep]
        if len(idx):
            self.ncols = max(self.ncols, int(idx.max()) + 1)
        if fresh:
            self.o[i, idx] = self.h[i, idx] = self.l[i, idx] = val
            self.live[i] = True
            self.partial[i] = block % BUCKET != 0  # we joined mid-bar: earlier blocks are missing
            self.exact[i, idx] = not self.partial[i]
        else:
            new = np.isnan(self.o[i, idx])
            self.o[i, idx[new]] = val[new]
            self.exact[i, idx[new]] = False
            self.h[i, idx] = np.fmax(self.h[i, idx], val)
            self.l[i, idx] = np.fmin(self.l[i, idx], val)
        self.c[i, idx] = val
        self.last_bucket = max(self.last_bucket, bucket)

    def put_sample(self, block: int, prices: dict[int, int]) -> None:
        """Backfill: one historical price snapshot (close only). Never overwrites a live bar."""
        bucket = block // BUCKET
        if self.has(bucket) and self.live[bucket % CAPACITY]:
            return
        if self.last_bucket >= 0 and bucket <= self.last_bucket - CAPACITY + 1:
            return  # older than the ring holds
        i = self._row(bucket)
        for n, p in prices.items():
            if 0 < n < NMAX and p > 0:
                self.ncols = max(self.ncols, n + 1)
                if self.exact[i, n]:
                    continue  # never replace every-block data with a single sample
                v = p / 1e9
                self.o[i, n] = self.h[i, n] = self.l[i, n] = self.c[i, n] = v

    def reset(self, netuid: int) -> None:
        self.o[:, netuid] = self.h[:, netuid] = self.l[:, netuid] = self.c[:, netuid] = np.nan
        self.exact[:, netuid] = False
        self._db.execute("DELETE FROM bars WHERE netuid = ?", (netuid,))
        self._db.commit()

    # ── series ───────────────────────────────────────────────────────────

    def closes(self, end_bucket: int, points: int, agg: int) -> np.ndarray:
        """[points, ncols] closes of `agg`-bucket bars ending at end_bucket (inclusive), forward-filled."""
        n, w = points * agg, self.ncols
        buckets = np.arange(end_bucket - n + 1, end_bucket + 1)
        rows = buckets % CAPACITY
        valid = self.bucket_of[rows] == buckets
        c = np.where(valid[:, None], self.c[rows, :w], np.nan)
        c = c.reshape(points, agg, w)
        # last non-NaN close inside each group
        out = np.full((points, w), np.nan)
        for k in range(agg):
            col = c[:, k, :]
            out = np.where(np.isnan(col), out, col)
        self.coverage = (~np.isnan(out)).mean(0)  # share of points with real data, per subnet
        out = _ffill(out)
        return _ffill(out[::-1])[::-1]  # a leading gap takes the first real value

    def candles(self, end_bucket: int, points: int, agg: int) -> tuple[np.ndarray, ...]:
        """OHLC of `agg`-bucket candles for one chart: each [points, ncols]. Backfilled-only candles get
        open = previous close so the chart stays continuous."""
        n, w = points * agg, self.ncols
        buckets = np.arange(end_bucket - n + 1, end_bucket + 1)
        rows = buckets % CAPACITY
        valid = (self.bucket_of[rows] == buckets)[:, None]
        o = np.where(valid, self.o[rows, :w], np.nan).reshape(points, agg, w)
        h = np.where(valid, self.h[rows, :w], np.nan).reshape(points, agg, w)
        lo = np.where(valid, self.l[rows, :w], np.nan).reshape(points, agg, w)
        c = np.where(valid, self.c[rows, :w], np.nan).reshape(points, agg, w)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN groups are expected for gaps
            hi = np.nanmax(h, axis=1)
            low = np.nanmin(lo, axis=1)
        close = np.full((points, w), np.nan)
        opn = np.full((points, w), np.nan)
        for k in range(agg):
            close = np.where(np.isnan(c[:, k, :]), close, c[:, k, :])
            opn = np.where(np.isnan(opn), o[:, k, :], opn)
        close = _ffill(close)
        prev = np.vstack([np.full((1, w), np.nan), close[:-1]])
        opn = np.where(np.isnan(prev), opn, prev)
        opn = np.where(np.isnan(opn), close, opn)
        hi = np.fmax(np.fmax(hi, opn), close)
        low = np.fmin(np.fmin(low, opn), close)
        return opn, hi, low, close

    def exact_mask(self, end_bucket: int, points: int, agg: int, netuid: int) -> np.ndarray:
        """Per candle: True if every 5-minute bar in it was built from every block."""
        buckets = np.arange(end_bucket - points * agg + 1, end_bucket + 1)
        rows = buckets % CAPACITY
        ok = (self.bucket_of[rows] == buckets) & self.exact[rows, netuid]
        return ok.reshape(points, agg).all(1)

    # ── persistence ──────────────────────────────────────────────────────

    def _persist(self, bucket: int) -> None:
        if not self.has(bucket):
            return
        i = bucket % CAPACITY
        cols = np.nonzero(~np.isnan(self.c[i]))[0]
        rows = [(bucket, int(n), float(self.o[i, n]), float(self.h[i, n]), float(self.l[i, n]),
                 float(self.c[i, n]), int(self.exact[i, n])) for n in cols]
        try:
            self._db.executemany("INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?)", rows)
            self._db.execute("DELETE FROM bars WHERE bucket < ?", (bucket - CAPACITY,))
            self._db.commit()
        except sqlite3.Error as e:
            log.warning("bar persist failed: %s", e)

    def persist_samples(self, buckets: list[int]) -> None:
        for b in buckets:
            self._persist(b)

    def load(self, head_block: int) -> int:
        """Load stored bars for the last CAPACITY buckets. Returns how many buckets were loaded."""
        lo = head_block // BUCKET - CAPACITY + 1
        cur = self._db.execute("SELECT bucket, netuid, o, h, l, c, live FROM bars WHERE bucket >= ? ORDER BY bucket", (lo,))
        seen = set()
        for bucket, n, o, h, l, c, live in cur:
            if n >= NMAX:
                continue
            i = self._row(bucket)
            self.o[i, n], self.h[i, n], self.l[i, n], self.c[i, n] = o, h, l, c
            self.ncols = max(self.ncols, n + 1)
            self.exact[i, n] = bool(live)
            seen.add(bucket)
        if seen:
            self.last_bucket = max(self.last_bucket, max(seen))
        return len(seen)

    # ── backfill ─────────────────────────────────────────────────────────

    def missing_plan(self, head_block: int) -> list[int]:
        """Buckets to sample, newest first: every 5-minute bar back to 3 days, one per 15 min back to
        7 days. Each sample is the exact price at the bar's last block, so candle closes are exact.
        Groups are aligned to absolute bucket numbers, so a restart only fills real gaps."""
        head_bucket = head_block // BUCKET
        plan: list[int] = []
        planned: set[int] = set()
        for span_h, step in ((72, 1), (7 * 24, 3)):
            first_group = (head_bucket - span_h * 12) // step
            for g in range((head_bucket - 1) // step, first_group, -1):
                group = range(g * step, g * step + step)
                if any(self.has(b) or b in planned for b in group):
                    continue
                b = min(g * step + step - 1, head_bucket - 1)  # group close
                plan.append(b)
                planned.add(b)
        return plan

    async def backfill(self, rpcs: list[Rpc], head_block: int, on_progress=None) -> int:
        """Sample the missing buckets from history-keeping nodes. Newest first, so the short
        timeframes are ready within seconds and the 3-day one fills in behind them."""
        plan = self.missing_plan(head_block)
        if not plan:
            return 0
        rpcs = [r for r in rpcs if r is not None]
        sems = {r.name: asyncio.Semaphore(4) for r in rpcs}
        done = 0

        async def one(i: int, bucket: int):
            nonlocal done
            block = bucket * BUCKET + BUCKET - 1  # the bar's last block = its close
            for attempt in range(len(rpcs) * 3):
                r = rpcs[(i + attempt) % len(rpcs)]
                try:
                    async with sems[r.name]:
                        h = await r.call("chain_getBlockHash", [block], timeout=20)
                        raw = await r.call("state_call", [PRICE_ALL, "0x", h], timeout=20)
                    self.put_sample(block, decode_prices(bytes.fromhex(raw[2:])))
                    done += 1
                    if on_progress and done % 50 == 0:
                        on_progress(done, len(plan))
                    return bucket
                except RpcError as e:
                    await asyncio.sleep(1.0 + attempt * 0.5 if "limit" in str(e) else 0.2)
            return None

        t0 = time.perf_counter()
        got = [b for b in await asyncio.gather(*(one(i, b) for i, b in enumerate(plan))) if b is not None]
        self.persist_samples(got)
        log.debug("backfilled %d/%d bars in %.1fs", len(got), len(plan), time.perf_counter() - t0)
        return len(got)


    # ── exact history (every block) ──────────────────────────────────────

    async def exactify(self, rpc: Rpc, keys: tuple[str, str, str, str], netuid: int,
                       first_bucket: int, last_bucket: int, chunk: int = 1000) -> int:
        """Rebuild one subnet's 5-minute bars in [first_bucket, last_bucket] from *every* block.

        Uses state_queryStorage (one call returns every block's change to the pool reserves),
        then price = TAO reserve × (1 − w) / (alpha reserve × w) for the pool weight w. The rebuilt
        prices are checked against the chain's own price API; on any mismatch nothing is written.
        Returns the number of bars made exact, or -1 if validation failed."""
        n = netuid
        need = [b for b in range(first_bucket, last_bucket + 1)
                if not (self.has(b) and self.exact[b % CAPACITY, n])]
        if not need:
            return 0
        k_num, k_tao, k_alpha, k_w = keys
        ranges, start, prev = [], need[0], need[0]
        for b in need[1:]:
            if b != prev + 1:
                ranges.append((start, prev))
                start = b
            prev = b
        ranges.append((start, prev))

        bars: dict[int, list[float]] = {}
        check_block, check_price = None, None
        for b0, b1 in ranges:
            tao = alpha = w = None  # the first change set of a query carries every key's full value
            first_blk, last_blk = b0 * BUCKET, b1 * BUCKET + BUCKET - 1
            for s in range(first_blk, last_blk + 1, chunk):
                e = min(s + chunk - 1, last_blk)
                h0 = await rpc.call("chain_getBlockHash", [s], timeout=20)
                h1 = await rpc.call("chain_getBlockHash", [e], timeout=20)
                sets = await rpc.call("state_queryStorage", [[k_num, k_tao, k_alpha, k_w], h0, h1], timeout=120)
                for cs in sets:
                    num = None
                    for k, v in cs["changes"]:
                        if v is None:
                            continue
                        x = int.from_bytes(bytes.fromhex(v[2:]), "little")
                        if k == k_num:
                            num = x
                        elif k == k_tao:
                            tao = x
                        elif k == k_alpha:
                            alpha = x
                        elif k == k_w:
                            w = x / 1e18
                    if num is None or not tao or not alpha or not w:
                        continue
                    price = tao * (1 - w) / (alpha * w)  # both reserves in rao → TAO per alpha
                    bar = bars.get(num // BUCKET)
                    if bar is None:
                        bars[num // BUCKET] = [price, price, price, price]
                    else:
                        bar[1], bar[2], bar[3] = max(bar[1], price), min(bar[2], price), price
                    check_block, check_price = num, price
        if not bars or check_block is None:
            return 0

        # validate: against stored samples (exact runtime prices at each bar's last block) …
        worst = 0.0
        for b, (_, _, _, c) in bars.items():
            i = b % CAPACITY
            if self.has(b) and not self.exact[i, n] and not np.isnan(self.c[i, n]):
                worst = max(worst, abs(c / float(self.c[i, n]) - 1))
        # … and against the runtime price at the last rebuilt block
        h = await rpc.call("chain_getBlockHash", [check_block], timeout=20)
        real = decode_prices(bytes.fromhex((await rpc.call("state_call", [PRICE_ALL, "0x", h], timeout=20))[2:]))
        if n in real:
            worst = max(worst, abs(check_price / (real[n] / 1e9) - 1))
        if worst > 1e-4:
            log.warning("exact history for SN%d rejected: rebuilt prices differ from the chain by %.4f%%", n, worst * 100)
            return -1

        rows = []
        for b, (o, h_, l_, c) in bars.items():
            if self.last_bucket >= 0 and b <= self.last_bucket - CAPACITY + 1:
                continue
            i = self._row(b)
            self.o[i, n], self.h[i, n], self.l[i, n], self.c[i, n] = o, h_, l_, c
            self.exact[i, n] = True
            rows.append((b, n, float(o), float(h_), float(l_), float(c), 1))
        try:
            self._db.executemany("INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?)", rows)
            self._db.commit()
        except sqlite3.Error as e:
            log.warning("exact bar persist failed: %s", e)
        return len(rows)


def _ffill(a: np.ndarray) -> np.ndarray:
    """Forward-fill NaNs down axis 0."""
    mask = np.isnan(a)
    idx = np.where(~mask, np.arange(a.shape[0])[:, None], 0)
    np.maximum.accumulate(idx, axis=0, out=idx)
    out = a[idx, np.arange(a.shape[1])[None, :]]
    return out
