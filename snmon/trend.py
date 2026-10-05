"""Trend monitor: sustained up/down moves over hours to days, not single pumps.

For each timeframe (default 12h / 24h / 3d) every subnet's closes over the window are fitted
with a log-linear regression on every block (one vectorised numpy pass for all subnets):

    trend %   = the fitted line's change across the window (robust to single spikes)
    strength  = R² of the fit (how cleanly price followed the line)
    shape     = the window's three thirds must step the same way (rejects V / Λ shapes)

A timeframe enters UP/DOWN when trend % ≥ its threshold with enough strength, and leaves only
when the trend % halves or R² collapses (hysteresis), so it doesn't flicker at the boundary.
Re-entering the same direction on the same timeframe waits half a window.

Alerts are grouped into stories — one card per subnet and direction. When a longer timeframe
confirms a fresh story, that card is updated in place instead of posting again. A trend in the
opposite direction is posted as a reversal.

Calibrated on 4–7 days of history for all subnets (2026-10-01): ~20–25 cards/day across ~129
subnets; SN80's late-September slide shows as a 24h (−12%, R² 0.82) and 3d (−13%, R² 0.75)
downtrend, and its recovery as a 24h uptrend (+11%, R² 0.70).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np

from . import alerts, chart, fmt
from .bars import BUCKET, NMAX, Bars
from .config import BLOCK_SECONDS
from .rpc import Rpc

log = logging.getLogger("snmon.trend")

DEFAULT_TIMEFRAMES = "1h:2:0.85:4:0.9,3h:3.5:0.8:5:0.85,6h:6:0.8,12h:8:0.7,24h:10:0.65,3d:12:0.6"
EXIT_FRACTION = 0.5     # leave a trend when its % falls below half the entry threshold …
EXIT_R2 = 0.4           # … or the fit's R² drops below this
UP_COLOR, DOWN_COLOR = 0x16A34A, 0xDC2626


@dataclass(frozen=True)
class Timeframe:
    name: str
    hours: int
    enter: float   # minimum fitted % move to be ON (board)
    r2: float      # minimum R² to be ON (board)
    card_enter: float = 0.0  # minimum fitted % move to post a card (0 = same as the board bar)
    card_r2: float = 0.0

    def __post_init__(self) -> None:
        if not self.card_enter:
            object.__setattr__(self, "card_enter", self.enter)
        if not self.card_r2:
            object.__setattr__(self, "card_r2", self.r2)

    @property
    def split(self) -> bool:
        """True if cards need more than the board (short windows)."""
        return (self.card_enter, self.card_r2) != (self.enter, self.r2)

    def describe(self) -> str:
        board = f"{self.name} ≥{self.enter:g}% R²≥{self.r2:g}"
        return board + (f" (card ≥{self.card_enter:g}% R²≥{self.card_r2:g})" if self.split else "")

    @property
    def agg(self) -> int:
        """Buckets (5 min) per fitted point: 5-min points up to 3h (1h → 12 points, so its steadiness
        bar is high), 10-min for 6h, 30-min up to a day, hourly beyond."""
        if self.hours <= 3:
            return 1
        if self.hours <= 6:
            return 2
        return 6 if self.hours <= 24 else 12

    @property
    def points(self) -> int:
        return self.hours * 12 // self.agg

    @property
    def blocks(self) -> int:
        return self.hours * 3600 // BLOCK_SECONDS

    @property
    def point_label(self) -> str:
        return "1-hour" if self.agg == 12 else f"{self.agg * 5}-min"

    def chart_shape(self) -> tuple[int, int]:
        """(buckets per candle, candles) for the alert chart: ~3 windows of context, ≤6 days."""
        buckets = min(3 * self.hours, 144) * 12
        for agg in (1, 2, 3, 6, 12, 24, 48):
            if buckets // agg <= 96:
                return agg, buckets // agg
        return 48, buckets // 48


def parse_timeframes(raw: str) -> list[Timeframe]:
    out = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        # name:board%:boardR²[:card%:cardR²] — short windows can show on the board before they post a card
        name, enter, r2, *card = part.split(":")
        hours = int(name[:-1]) * (24 if name.endswith("d") else 1)
        ce, cr = (float(card[0]), float(card[1])) if len(card) == 2 else (0.0, 0.0)
        out.append(Timeframe(name, hours, float(enter), float(r2), ce, cr))
    return sorted(out, key=lambda t: t.hours)


@dataclass
class Fit:
    ok: np.ndarray       # enough data
    change: np.ndarray   # fitted % change across the window
    r2: np.ndarray
    shape: np.ndarray    # +1 thirds rising, -1 falling, 0 neither
    start: np.ndarray    # trend-line value at window start (TAO)
    end: np.ndarray      # trend-line value now
    first: np.ndarray    # actual close at window start
    last: np.ndarray     # actual close now
    hi: np.ndarray
    lo: np.ndarray


MIN_COVERAGE = 0.75  # a window needs real data in ≥75% of its points


def fit_all(closes: np.ndarray, coverage: np.ndarray) -> Fit:
    ok = (coverage >= MIN_COVERAGE) & ~np.isnan(closes).any(0) & (np.nan_to_num(closes) > 0).all(0)
    safe = np.where(ok, closes, 1.0).astype(np.float64)
    y = np.log(safe)
    n = y.shape[0]
    x = np.arange(n) - (n - 1) / 2
    ym = y.mean(0)
    dy = y - ym
    sxx = (x * x).sum()
    sxy = (x[:, None] * dy).sum(0)
    syy = (dy * dy).sum(0)
    slope = sxy / sxx
    with np.errstate(divide="ignore", invalid="ignore"):
        r2 = np.where(syy > 0, sxy * sxy / (sxx * syy), 0.0)
    k = n // 3
    m1, m2, m3 = y[:k].mean(0), y[k:2 * k].mean(0), y[2 * k:].mean(0)
    shape = np.where((m1 < m2) & (m2 < m3), 1, np.where((m1 > m2) & (m2 > m3), -1, 0))
    half = slope * (n - 1) / 2
    return Fit(ok, np.expm1(slope * (n - 1)) * 100, r2, shape, np.exp(ym - half), np.exp(ym + half),
               safe[0], safe[-1], safe.max(0), safe.min(0))


@dataclass
class Story:
    netuid: int
    direction: int
    kind: str                     # "new" | "reversal" | "continues"
    posted_block: int
    last_active: int
    tfs: list[str]                # timeframes confirming, in the order they triggered
    msg_id: str | None = None
    posted: asyncio.Event = field(default_factory=asyncio.Event, repr=False)


class TrendMonitor:
    def __init__(self, timeframes: list[Timeframe], bars: Bars, meta, discord, watched,
                 story_hours: float = 12.0) -> None:
        self.tfs = timeframes
        self.bars = bars
        self.meta = meta
        self.discord = discord
        self.watched = watched
        self.story_blocks = int(story_hours * 3600 / BLOCK_SECONDS)
        self.state = {tf.name: np.zeros(NMAX, dtype=np.int8) for tf in timeframes}
        # direction already announced with a card in the current trend episode (0 = not yet)
        self.carded = {tf.name: np.zeros(NMAX, dtype=np.int8) for tf in timeframes}
        self.last_enter = {(tf.name, d): np.full(NMAX, -10**9, dtype=np.int64) for tf in timeframes for d in (1, -1)}
        self.fits: dict[str, Fit] = {}
        self.stories: dict[int, Story] = {}
        self.ready = False
        self.alerts = 0
        self.dry_run = False
        self.history: Rpc | None = None   # a node allowing state_queryStorage, for exact charts
        self.board: TrendBoard | None = None
        self.flips: list[tuple[int, str, int]] = []
        self.signals = None  # set by the app: reversal signals shown on the board
        self._last_block = 0
        self._exact_lock = asyncio.Lock()

    # ── evaluation (every block) ─────────────────────────────────────────

    def evaluate(self, block: int, silent: bool = False) -> list[tuple[int, Timeframe, int]]:
        """Refit every timeframe for every subnet; returns (netuid, timeframe, direction) entries."""
        end = block // BUCKET
        self._last_block = block
        self.flips = []  # (netuid, window, direction) for every window that just turned ▲/▼ on the board
        events = []
        for tf in self.tfs:
            closes = self.bars.closes(end, tf.points, tf.agg)
            f = fit_all(closes, self.bars.coverage)
            self.fits[tf.name] = f
            w = len(f.change)
            st = self.state[tf.name][:w]
            up = f.ok & (f.change >= tf.enter) & (f.r2 >= tf.r2) & (f.shape == 1)
            dn = f.ok & (f.change <= -tf.enter) & (f.r2 >= tf.r2) & (f.shape == -1)
            target = np.where(up, 1, np.where(dn, -1, 0)).astype(np.int8)
            leave = (st != 0) & (~f.ok | (st * f.change < tf.enter * EXIT_FRACTION) | (f.r2 < EXIT_R2))
            st[leave] = 0
            flip = (target != 0) & (target != st)
            st[flip] = target[flip]
            self.flips += [(int(n), tf.name, int(target[n])) for n in np.nonzero(flip)[0]]
            # A card goes out once per trend episode, when the trend also clears the card bar (for long
            # windows that's the same moment it turns on; short windows show on the board first).
            carded = self.carded[tf.name][:w]
            carded[carded != st] = 0
            card = (st != 0) & f.ok & (st * f.change >= tf.card_enter) & (f.r2 >= tf.card_r2) & (f.shape == st)
            for n in np.nonzero(card & (carded == 0))[0]:
                d = int(st[n])
                carded[n] = d
                le = self.last_enter[(tf.name, d)]
                cooled = block - le[n] >= tf.blocks // 2
                le[n] = block
                if not silent and cooled and self.watched(int(n)):
                    events.append((int(n), tf, d))
        # keep stories alive while any timeframe still trends their way
        for n, s in self.stories.items():
            if any(self.state[tf.name][n] == s.direction for tf in self.tfs):
                s.last_active = block
        return events

    def reset(self, netuid: int) -> None:
        for tf in self.tfs:
            self.state[tf.name][netuid] = 0
            self.carded[tf.name][netuid] = 0
        self.stories.pop(netuid, None)

    # ── stories → Discord ────────────────────────────────────────────────

    def on_events(self, block: int, events: list[tuple[int, Timeframe, int]]) -> None:
        for n, tf, d in events:
            s = self.stories.get(n)
            if s and s.direction == d and block - s.posted_block <= self.story_blocks:
                if tf.name not in s.tfs:
                    s.tfs.append(tf.name)
                    s.last_active = block
                    print(fmt.dim(f"{_now()}    ↳ trend SN{n} also {tf.name} {self._pct(n, tf)}"))
                    asyncio.create_task(self._update(s, block))
                continue
            if s and block - s.last_active <= self.story_blocks:
                kind = "reversal" if s.direction != d else "continues"
            else:
                kind = "new"
            s = Story(n, d, kind, block, block, [tf.name])
            self.stories[n] = s
            self.alerts += 1
            print(f"{_now()} {self.console_line(s, tf)}")
            asyncio.create_task(self._post(s, block))

    async def _post(self, s: Story, block: int) -> None:
        try:
            payload, png = await self._build(s, block)
            if self.dry_run:
                return
            s.msg_id = await self.discord.send(payload, files={"trend.png": png})
            if self.board is not None:
                self.board.stale = True
        except Exception:
            log.exception("trend alert failed")
        finally:
            s.posted.set()
        await self._refine(s)

    async def _update(self, s: Story, block: int) -> None:
        await s.posted.wait()
        if not s.msg_id:
            return
        try:
            payload, png = await self._build(s, block)
            payload.pop("allowed_mentions", None)
            self.discord.edit_later(s.msg_id, payload, files={"trend.png": png})
        except Exception:
            log.exception("trend update failed")
        await self._refine(s)

    async def _refine(self, s: Story) -> None:
        """Swap the card's chart for one built from every block: rebuild this subnet's history for
        the chart span (validated against the chain), then edit the card."""
        if self.history is None or not s.msg_id:
            return
        tf = self._main_tf(s)
        agg, count = tf.chart_shape()
        async with self._exact_lock:  # one at a time — be gentle with the history node
            end = self.bars.last_bucket
            t0 = time.perf_counter()
            try:
                keys = await self.meta.storage_keys(s.netuid)
                made = await self.bars.exactify(self.history, keys, s.netuid, end - count * agg + 1, end - 1)
            except Exception as e:
                log.warning("exact chart for SN%d failed: %s", s.netuid, e)
                return
        if made > 0:
            block = end * BUCKET + BUCKET - 1
            payload, png = await self._build(s, max(block, self._last_block))
            payload.pop("allowed_mentions", None)
            self.discord.edit_later(s.msg_id, payload, files={"trend.png": png})
            print(fmt.dim(f"{_now()}    ↳ trend SN{s.netuid}: chart rebuilt from every block "
                          f"({made} bars, {time.perf_counter() - t0:.0f}s)"))

    def _pct(self, n: int, tf: Timeframe) -> str:
        f = self.fits[tf.name]
        move = fmt.move(float(f.first[n]) * 1e9, float(f.last[n]) * 1e9)[2]
        return f"{fmt.pct(move)} (R² {f.r2[n]:.2f})"

    def _main_tf(self, s: Story) -> Timeframe:
        """The longest timeframe in the story — the headline."""
        names = set(s.tfs)
        return [tf for tf in self.tfs if tf.name in names][-1]

    async def _build(self, s: Story, block: int) -> tuple[dict, bytes]:
        """Every "A → B (X%)" on the card uses real prices (window start → now), with X computed from
        the numbers as shown. The fitted line is only shown where it's labelled as the trend line."""
        tf = self._main_tf(s)
        n = s.netuid
        f = self.fits[tf.name]
        info = self.meta.info(n)
        up = s.direction > 0
        word = "UPTREND" if up else "DOWNTREND"
        emoji = "📈" if up else "📉"
        fit, r2 = float(f.change[n]), float(f.r2[n])
        a, b, move = fmt.move(float(f.first[n]) * 1e9, float(f.last[n]) * 1e9)
        tfs_label = " + ".join(s.tfs) if len(s.tfs) > 1 else tf.name

        # A short-term trend against a longer one still running is a pullback / bounce, not a reversal.
        against = next((t for t in reversed(self.tfs)
                        if t.hours > tf.hours and self.state[t.name][n] == -s.direction), None)
        if against is not None:
            g = self.fits[against.name]
            big = fmt.move(float(g.first[n]) * 1e9, float(g.last[n]) * 1e9)[2]
            other = "downtrend" if up else "uptrend"  # the longer trend runs the other way
            prefix = "↘️ PULLBACK · " if not up else "↗️ BOUNCE · "
            context = f" in a {against.name} {other.upper()}"
            tail = f" · still {fmt.pct(big)} over {against.name}"
        else:
            prefix = {"reversal": "🔄 REVERSAL → ", "continues": ""}.get(s.kind, "")
            context, tail = "", ""
        suffix = " CONTINUES" if s.kind == "continues" else ""
        title = f"{prefix}{emoji} {tfs_label} {word}{suffix}{context}  ·  {fmt.pct(move)}"
        name = f"SN{n} {info.name}".strip()
        headline = (f"{prefix}{emoji} **{name}** {tf.name} {word}{suffix} **{fmt.pct(move)}**{context.lower()} · "
                    f"{a} → {b} τ{tail}")

        per_hour = ((1 + fit / 100) ** (1 / tf.hours) - 1) * 100
        strength = "very strong" if r2 >= 0.85 else "strong" if r2 >= 0.75 else "clear" if r2 >= 0.6 else "weak"
        sign = "+" if up else "-"
        rows = [f"{'window':>6}  {'change':>8}  {'steady':>6}  trend"]
        for t in self.tfs:
            g = self.fits.get(t.name)
            if g is None or not g.ok[n]:
                rows.append(f"{t.name:>6}  not enough history yet")
                continue
            st = self.state[t.name][n]
            mv = fmt.move(float(g.first[n]) * 1e9, float(g.last[n]) * 1e9)[2]
            mark = ("▲ up" if st > 0 else "▼ down") if st else "—"
            new = " ← new" if t.name == s.tfs[-1] else ""
            rows.append(f"{t.name:>6}  {fmt.pct(mv):>8}  {g.r2[n]:>6.2f}  {mark}{new}")
        legend = ("change = real price move over that window (ago → now)\n"
                  "steady = how straight the move was: 1.00 a clean line, under 0.40 choppy")

        embed = {
            "author": alerts._author(info),
            "title": title,
            "url": alerts.subnet_url(n),
            "description": f"```diff\n{sign} {a} τ  →  {b} τ   ({fmt.pct(move)} in {tf.name})\n```",
            "color": UP_COLOR if up else DOWN_COLOR,
            "fields": [
                {"name": "Trend line (fit)", "value": f"**{fmt.pct(fit)}** over {tf.name}\n{fmt.pct(per_hour)} per hour",
                 "inline": True},
                {"name": "Strength", "value": f"**{strength}**\nsteady {r2:.2f}", "inline": True},
                {"name": "Pool liquidity", "value": f"**{fmt.tao_compact(info.pool_tao)}**" if info.pool_tao else "—",
                 "inline": True},
                {"name": f"Range {tf.name}", "value": f"high {fmt.price(float(f.hi[n]) * 1e9)}\n"
                                                    f"low {fmt.price(float(f.lo[n]) * 1e9)}", "inline": True},
                {"name": "Timeframes", "value": "```\n" + "\n".join(rows) + "\n```\n" + legend, "inline": False},
            ],
            "image": {"url": "attachment://trend.png"},
            "footer": {"text": f"Trend line = best straight-line fit of {tf.point_label} closes over {tf.name} · "
                               f"checked every block · block #{block}"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        png = await asyncio.to_thread(self._chart, s, tf, info, move, block)
        return {"content": alerts.content([headline]), "embeds": [embed], "allowed_mentions": {"parse": []}}, png

    def _chart(self, s: Story, tf: Timeframe, info, change: float, block: int) -> bytes:
        n = s.netuid
        agg, count = tf.chart_shape()
        o, h, lo, c = (a[:, n] for a in self.bars.candles(block // BUCKET, count, agg))
        f = self.fits[tf.name]
        m = max(2, round(tf.hours * 12 / agg))  # candles inside the trend window
        fit = np.full(count, np.nan)
        ls, le = np.log(float(f.start[n])), np.log(float(f.end[n]))
        fit[-m:] = np.exp(ls + (le - ls) * np.linspace(0, 1, m))
        span_h = count * agg / 12
        labels = []
        for k in (3, 2, 1, 0):
            hrs = span_h * k / 3
            i = count - 1 - round(hrs * 12 / agg)
            text = "now" if k == 0 else (f"-{hrs / 24:g}d" if hrs >= 48 and hrs % 24 == 0 else f"-{hrs:g}h")
            labels.append((max(0, i), text))
        word = "UPTREND" if s.direction > 0 else "DOWNTREND"
        title = f"SN{n} · {info.name}" if info.name else f"SN{n}"
        minutes = agg * 5
        interval = f"{minutes}m" if minutes < 60 else f"{minutes // 60}h"
        exact = self.bars.exact_mask(block // BUCKET, count, agg, n)
        exact[-1] = True  # the forming candle is live by definition
        return chart.render(o, h, lo, c, fit, title=title, badge=f"{tf.name} {word} {fmt.pct(change)}",
                            direction=s.direction, x_labels=labels, interval=interval, exact=exact)

    def console_line(self, s: Story, tf: Timeframe) -> str:
        n = s.netuid
        f = self.fits[tf.name]
        info = self.meta.info(n)
        color = fmt.bgreen if s.direction > 0 else fmt.bred
        word = ("UPTREND" if s.direction > 0 else "DOWNTREND") + {"reversal": " ↺", "continues": " +"}.get(s.kind, "")
        a, b, move = fmt.move(float(f.first[n]) * 1e9, float(f.last[n]) * 1e9)
        return color(f"{'📈' if s.direction > 0 else '📉'} {word:<11} SN{n:<3} {info.name[:16]:<16} {tf.name:>3} "
                     f"{fmt.pct(move):>8}  {a} → {b} τ  (fit {fmt.pct(float(f.change[n]))}, R² {f.r2[n]:.2f})")


BOARD_FILE = "trend_board.json"
BOARD_ROWS = 30
SILENT = 4096  # message flag: deliver without a push notification


class TrendBoard:
    """One live message listing every subnet that is trending right now, across all timeframes.

    Refreshed every minute by editing it in place. Whenever trend cards have been posted below
    it, it is re-posted (silently) so it always sits at the bottom of the channel."""

    def __init__(self, monitor: "TrendMonitor", path, every: float = 60.0) -> None:
        self.m = monitor
        self.path = path
        self.every = every
        self.msg_id: str | None = None
        self.stale = False  # a card was posted after the board
        self.first_seen: dict[tuple[int, int], float] = {}  # (netuid, direction) → when it joined the board
        self._initial = True  # what's on the board at startup isn't "new"

    def payload(self) -> dict:
        """Grouped and sorted by the MOST RECENT trend: a subnet's group (up/down) is the direction of
        its shortest window in a trend — what the price is doing now; longer windows show the bigger
        picture in the strip. Rows: aligned monospace span (arrows + the latest trend's real move), then
        the subnet name linking to taomarketcap. 🆕 = joined in the last hour.
        ⏳ Building: no window in a trend yet, but a steady move ≥70% of a window's bar."""
        m, tfs = self.m, self.m.tfs
        now = time.time()
        names = " ".join(t.name for t in tfs)
        groups: dict[int, list[tuple]] = {1: [], -1: []}
        building: list[tuple] = []
        on_board = set()
        for n in range(1, len(m.state[tfs[0].name])):
            if not m.watched(n):
                continue
            states = [int(m.state[t.name][n]) for t in tfs]
            name = m.meta.info(n).name or ""
            link = f"[SN{n} · {name}]({alerts.subnet_url(n)})" if name else f"[SN{n}]({alerts.subnet_url(n)})"
            if any(states):
                latest = min((i for i, st in enumerate(states) if st), key=lambda i: tfs[i].hours)
                d, t = states[latest], tfs[latest]
                g = m.fits.get(t.name)
                if g is None or n >= len(g.ok) or not g.ok[n]:
                    continue
                mv = fmt.move(float(g.first[n]) * 1e9, float(g.last[n]) * 1e9)[2]
                strip = " ".join(("▲" if st > 0 else "▼" if st < 0 else "·").ljust(len(tf.name))
                                 for st, tf in zip(states, tfs))
                on_board.add((n, d))
                seen = self.first_seen.setdefault((n, d), 0.0 if self._initial else now)
                new = "🆕 " if seen and now - seen < 3600 else ""
                groups[d].append((t.hours, -abs(mv), f"`{strip}  {fmt.pct(mv):>8} {t.name:<3}` {new}{link}"))
                continue
            for i, t in enumerate(tfs):  # building: shortest window that's ≥70% of the way, steady, right shape
                f = m.fits.get(t.name)
                if f is None or n >= len(f.ok) or not f.ok[n]:
                    continue
                ch = float(f.change[n])
                progress = abs(ch) / t.enter
                if progress >= 0.7 and f.r2[n] >= t.r2 and int(f.shape[n]) == (1 if ch > 0 else -1):
                    mv = fmt.move(float(f.first[n]) * 1e9, float(f.last[n]) * 1e9)[2]
                    heading = "↗ UP" if ch > 0 else "↘ DOWN"
                    building.append((0 if ch > 0 else 1, t.hours, -progress,
                                     f"`{heading:<6}  {fmt.pct(mv):>8} {t.name:<3} {progress * 100:>4.0f}%` {link}"))
                    break
        self.first_seen = {k: v for k, v in self.first_seen.items() if k in on_board}
        self._initial = False

        def section(rows: list[tuple], header: str, budget: int) -> str:
            lines, used = [header], len(header)
            ordered = [r[-1] for r in sorted(rows, key=lambda r: r[:-1])]
            for r in ordered:  # Discord caps all embeds in a message at 6000 chars
                if used + len(r) + 1 > budget:
                    break
                lines.append(r)
                used += len(r) + 1
            if len(lines) - 1 < len(ordered):
                lines.append(f"… +{len(ordered) - (len(lines) - 1)} more")
            return "\n".join(lines)

        trend_head = f"`{names}  latest trend`"
        embeds = []
        if m.signals is not None:
            embeds.append(m.signals.board_section(m._last_block // BUCKET, 600))
        for d, title, color in ((1, "📈 Uptrends", UP_COLOR), (-1, "📉 Downtrends", DOWN_COLOR)):
            embeds.append({"title": f"{title} now ({len(groups[d])})", "color": color,
                           "description": section(groups[d], trend_head, 1250) if groups[d] else "none right now"})
        embeds.append({"title": f"⏳ Almost trending ({len(building)}) — not a trend yet, but close",
                       "color": 0x6B7280,
                       "description": section(building, f"`{'heading':<6}  {'move':>8} {'in':<3} {'ready':>5}`", 700)
                       if building else "nothing close to a trend right now"})
        ts = datetime.now(timezone.utc)
        embeds[-1]["footer"] = {"text": "Up/down: sorted by the most recent window in a trend · latest trend = that "
                                        "window's real price change · 🆕 new in the last hour · ready = how close to "
                                        "a trend · signals = 1h turns up after a ≥5% dump · tap a name for taomarketcap"}
        embeds[-1]["timestamp"] = ts.isoformat()
        up, down = len(groups[1]), len(groups[-1])
        sigs = f"**{len(m.signals.active)} reversal signals in 24h** · " if m.signals is not None else ""
        return {"content": f"\u200b\n📊 **Trend board** — {sigs}trending right now: **{up} up · {down} down** · "
                           f"{len(building)} almost · updated <t:{int(ts.timestamp())}:R> · refreshes every minute",
                "embeds": embeds, "allowed_mentions": {"parse": []}}

    async def run(self) -> None:
        try:
            self.msg_id = json.loads(self.path.read_text()).get("msg_id")
        except (OSError, ValueError):
            self.msg_id = None
        while True:
            try:
                if self.m.ready:
                    await self._refresh()
            except Exception:
                log.exception("trend board refresh failed")
            await asyncio.sleep(self.every)

    async def _refresh(self) -> None:
        d = self.m.discord
        payload = self.payload()
        if self.msg_id and not self.stale and await d.edit(self.msg_id, payload):
            return
        if self.msg_id:  # buried under new cards (or deleted): move it back to the bottom
            await d.delete(self.msg_id)
        self.msg_id = await d.send({**payload, "flags": SILENT})
        self.stale = False
        try:
            self.path.write_text(json.dumps({"msg_id": self.msg_id}))
        except OSError:
            pass


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]
