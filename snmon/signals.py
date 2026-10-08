"""Reversal signals — "dump → pump starting".

Rule:

    price turns up over the trigger window        (default 15m: the last three 5-minute bars rise
                                                   ≥ 2% in total, each one higher than the one before)
    AND price is ≥ 5% below its 24h high          (it is coming out of a dump)
    at most once per subnet every 12 hours

The trigger window is `TREND_SIGNAL_WINDOW` (window:min %:steadiness — "15m:2:0.8", "30m:2:0.85",
"1h:2:0.85"). It is evaluated on every block, with the bar in progress as the last point, so a signal
fires within seconds of the turn rather than at a bar's close.

What the window changes, replayed on 8 days of 5-minute prices for every subnet (2026-10-07):

    window     signals/day   higher after 24h   median after 24h   best gain within 24h (median)
    1h  ≥2%        4.3            54%               +1.50%               +7.4%
    30m ≥2%        5.1            52%               +1.22%               +4.9%
    15m ≥2%        7.3            52%               +0.33%               +5.6%
    5m  ≥1%       18.3            44%               −0.83%               +3.8%      (one candle: a pump alert)
    random buys     —             38%               −0.13%               +0.6%

A shorter window fires earlier and more often; each signal is a little weaker. Waiting longer than
1h (3h confirmation) was negative — the edge is in being early.

That is an edge, not a guarantee: about half the signals lose. So every signal carries its
invalidation level (the dump's low), is tracked for 24 hours, and the card is updated with what
actually happened. The track record shown on cards is recomputed from the stored price history with
the rule in use, and the live record from the signals this monitor actually posted.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from . import alerts, chart, fmt
from .bars import BUCKET, NMAX, Bars
from .trend import EXIT_FRACTION, EXIT_R2, fit_all

log = logging.getLogger("snmon.signals")

DEFAULT_TRIGGER = "15m:2:0.8"
LOOKBACK = 288          # buckets (24h) for the high the dump is measured from
COOLDOWN = 144          # buckets (12h) between signals on one subnet: backtested 66% up vs 59% at 3h,
                        # and it stops re-signalling a subnet that keeps failing to bounce
TRACK = 288             # buckets (24h) a signal is tracked
CHECKPOINTS = ((12, "1h"), (72, "6h"), (288, "24h"))
SIZES = (10, 50, 100)   # TAO, for the slippage line
COLOR, WIN, LOSS = 0x06B6D4, 0x16A34A, 0xDC2626
GREY, RED = (148, 155, 164), (239, 68, 68)


@dataclass(frozen=True)
class Trigger:
    """The window whose turn up fires a signal."""
    name: str       # "15m"
    points: int     # 5-minute bars in the window
    enter: float    # % the fitted line must rise across the window
    r2: float       # how cleanly price must follow that line (1 = a straight climb)

    def describe(self) -> str:
        return f"{self.name} ≥{self.enter:g}%"


def parse_trigger(spec: str) -> Trigger:
    """"15m:2:0.8" → Trigger. Window in minutes or hours (multiples of 5 minutes, at least 10m)."""
    parts = (spec or DEFAULT_TRIGGER).replace(" ", "").split(":")
    name = parts[0].lower()
    minutes = int(name[:-1]) * (60 if name.endswith("h") else 1)
    if name[-1] not in "mh" or minutes % 5 or minutes < 10:
        raise ValueError(f"TREND_SIGNAL_WINDOW: '{spec}' — use e.g. 15m, 30m or 1h")
    points = minutes // 5
    return Trigger(name, points, float(parts[1]) if len(parts) > 1 else 2.0,
                   float(parts[2]) if len(parts) > 2 else (0.8 if points <= 4 else 0.85))


@dataclass
class Sig:
    netuid: int
    bucket: int               # 5-minute bucket the signal fired in
    block: int
    ts: float
    entry: float              # price at the signal (TAO per alpha)
    high: float               # the 24h high the dump started from
    low: float                # the dump's low = invalidation level
    msg_id: str | None = None
    marks: dict = field(default_factory=dict)  # "1h"/"6h"/"24h" → % since entry
    best: float = 0.0
    worst: float = 0.0
    last: float = 0.0         # % since entry at the latest look
    invalidated: int | None = None             # bucket of the first 5-min close below `low`
    done: bool = False
    embed: dict | None = None                  # the posted card, for later edits

    def age(self, bucket: int) -> int:
        return bucket - self.bucket


class Signals:
    def __init__(self, monitor, bars: Bars, meta, discord, watched, path: Path, dump_pct: float = 5.0,
                 sim=None, flow=None, dry_run: bool = False, trigger: Trigger | None = None) -> None:
        self.m = monitor
        self.trigger = trigger or parse_trigger(DEFAULT_TRIGGER)
        self.fit = None                     # latest fit of the trigger window, every subnet
        self._st = np.zeros(NMAX, dtype=np.int8)   # 1 = the trigger window is up for this subnet
        self._armed = False                 # the first look only records what is already up
        self.bars = bars
        self.meta = meta
        self.discord = discord
        self.watched = watched
        self.path = path
        self.dump_pct = dump_pct
        self.sim = sim      # async (netuid, tao_rao) → alpha out (rao)
        self.flow = flow    # (netuid) → (net_tao, buys, sells) over the last hour, or None
        self.dry_run = dry_run
        self.active: list[Sig] = []
        self.history: list[Sig] = []
        self.stats: dict | None = None
        self.posted = 0
        self._last: dict[int, int] = {}
        self._load()

    # ── detection (every block) ──────────────────────────────────────────

    def levels(self, bucket: int, n: int) -> tuple[float, float, float] | None:
        """(price now, 24h high, low since that high) — the dump leg the bounce comes out of."""
        closes = self.bars.closes(bucket, LOOKBACK + 1, 1)[:, n].astype(np.float64)
        if np.isnan(closes).any():
            return None
        i_hi = int(closes.argmax())
        return float(closes[-1]), float(closes[i_hi]), float(closes[i_hi:].min())

    def _turned_up(self, bucket: int) -> list[int]:
        """Refit the trigger window (the bar in progress is its last point) → subnets that just turned up."""
        tg = self.trigger
        f = fit_all(self.bars.closes(bucket, tg.points, 1), self.bars.coverage)
        self.fit = f
        st = self._st[:len(f.change)]
        up = f.ok & (f.change >= tg.enter) & (f.r2 >= tg.r2) & (f.shape == 1)
        st[(st != 0) & (~f.ok | (f.change < tg.enter * EXIT_FRACTION) | (f.r2 < EXIT_R2))] = 0
        flip = up & (st != 1)
        st[flip] = 1
        return [int(n) for n in np.nonzero(flip)[0]]

    def detect(self, block: int) -> list[Sig]:
        bucket = block // BUCKET
        turned = self._turned_up(bucket)
        if not self._armed:   # just started: what is already rising is not a fresh turn
            self._armed = True
            return []
        out = []
        for n in turned:
            if n == 0 or not self.watched(n):
                continue
            if bucket - self._last.get(n, -10**9) < COOLDOWN:
                continue
            lv = self.levels(bucket, n)
            if lv is None:
                continue
            price, high, low = lv
            if (price / high - 1) * 100 > -self.dump_pct:
                continue  # not coming out of a dump
            sig = Sig(n, bucket, block, time.time(), price, high, low)
            self._last[n] = bucket
            self.active.append(sig)
            out.append(sig)
            self.posted += 1
            info = self.meta.info(n)
            print(fmt.bgreen(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} 🎯 SIGNAL   SN{n:<3} {info.name[:16]:<16} "
                             f"dump {fmt.pct((low / high - 1) * 100)} → {self.trigger.name} turn up · entry {fmt.price(price * 1e9)} τ"))
            asyncio.create_task(self._post(sig))
        if out:
            self._save()
        return out

    # ── tracking (every few blocks) ──────────────────────────────────────

    def update(self, block: int) -> None:
        bucket = block // BUCKET
        changed_any = False
        for sig in list(self.active):
            age = sig.age(bucket)
            if age <= 0:
                continue
            closes = self.bars.closes(bucket, min(age, TRACK) + 1, 1)[:, sig.netuid].astype(np.float64)
            if np.isnan(closes).any():
                continue
            path = (closes[1:] / sig.entry - 1) * 100
            sig.best, sig.worst, sig.last = float(max(path.max(), 0)), float(min(path.min(), 0)), float(path[-1])
            changed = False
            if sig.invalidated is None:
                below = np.nonzero(closes[1:] < sig.low)[0]
                if len(below):
                    sig.invalidated = bucket - (len(path) - 1 - int(below[0]))
                    changed = True
            for b, label in CHECKPOINTS:
                if age >= b and label not in sig.marks:
                    at = self.bars.closes(sig.bucket + b, 1, 1)[0, sig.netuid]
                    if not np.isnan(at):
                        sig.marks[label] = float((at / sig.entry - 1) * 100)
                        changed = True
            if age >= TRACK:
                sig.done = True
                self.active.remove(sig)
                self.history.append(sig)
                changed = True
            if changed:
                changed_any = True
                self._edit(sig)
        if changed_any:
            self._save()

    def record(self) -> tuple[int, int]:
        """(signals with a 24h result, how many were up) — the live record of posted signals."""
        done = [s for s in self.history if "24h" in s.marks and s.msg_id]  # only signals posted as cards
        return len(done), sum(1 for s in done if s.marks["24h"] > 0)

    # ── cards ────────────────────────────────────────────────────────────

    def track_record(self) -> str:
        st = self.stats
        parts = []
        if st and st["n"]:
            parts.append(f"Backtest, last {st['days']:.0f} days: **{st['n']} signals** like this — **{st['win24']:.0f}%** were "
                         f"higher after 24h, median **{fmt.pct(st['med24'])}** (after 6h {fmt.pct(st['med6'])}). "
                         f"Typical best along the way {fmt.pct(st['best'])}, typical worst dip {fmt.pct(st['worst'])}.")
        n, up = self.record()
        if n:
            parts.append(f"Live, signals posted here: {up}/{n} up after 24h.")
        parts.append("_An edge, not a guarantee — about half lose. Not financial advice._")
        return "\n".join(parts)

    def _result(self, sig: Sig, bucket: int | None = None) -> str:
        bits = [f"now **{fmt.pct(sig.last)}**"] if not sig.done else []
        bits += [f"+{k} **{fmt.pct(v)}**" for k, v in sig.marks.items()]
        bits += [f"best {fmt.pct(sig.best)}", f"worst {fmt.pct(sig.worst)}"]
        text = " · ".join(bits)
        if sig.invalidated is not None:
            after = fmt.duration((sig.invalidated - sig.bucket) * BUCKET)
            text += f"\n❌ Closed below the invalidation level {after} after the signal."
        return text

    async def _card(self, sig: Sig) -> tuple[dict, bytes]:
        n = sig.netuid
        info = self.meta.info(n)
        name = f"SN{n} {info.name}".strip()
        hi, lo, entry = sig.high * 1e9, sig.low * 1e9, sig.entry * 1e9
        d_a, d_b, d_pc = fmt.move(hi, lo)
        b_a, b_b, b_pc = fmt.move(lo, entry)
        _, _, to_stop = fmt.move(entry, lo)
        _, _, to_high = fmt.move(entry, hi)
        desc = (f"```diff\n- dump    {d_a} τ → {d_b} τ  ({fmt.pct(d_pc)})\n"
                f"+ bounce  {b_a} τ → {b_b} τ  ({fmt.pct(b_pc)})  ← turned up ({self.trigger.name})\n```")
        fields = [
            {"name": "Entry (now)", "value": f"**{fmt.price(entry)} τ**", "inline": True},
            {"name": "Invalidation", "value": f"**{fmt.price(lo)} τ** ({fmt.pct(to_stop)})\nclose below the dump's low",
             "inline": True},
            {"name": "Previous high", "value": f"**{fmt.price(hi)} τ** ({fmt.pct(to_high)})", "inline": True},
        ]
        slip = await self._slippage(n, sig.entry)
        if slip:
            fields.append({"name": "Cost to buy (slippage + fee)", "value": slip, "inline": True})
        fields.append({"name": "Pool liquidity", "value": f"**{fmt.tao_compact(info.pool_tao)}**" if info.pool_tao else "—",
                       "inline": True})
        fl = self.flow(n) if self.flow else None
        if fl is not None:
            net, buys, sells = fl
            sign = "+" if net >= 0 else "-"
            fields.append({"name": "Flow, last hour", "inline": True,
                           "value": (f"net **{sign}{fmt.tao(abs(net))} τ**\n{buys} buys · {sells} sells"
                                     if buys or sells else "no trades")})
        age_days = (sig.block - info.registered_at) * 12 / 86400 if info.registered_at else None
        if age_days is not None and 0 <= age_days < 7:
            fields.insert(0, {"name": "⚠️ New subnet", "inline": False,
                              "value": f"Registered **{age_days:.1f} days ago**. A launch moves far more than an established "
                                       f"subnet and has little history — the track record below is mostly older subnets."})
        tfs = self.m.tfs
        strip = " ".join(("▲" if self.m.state[t.name][n] > 0 else "▼" if self.m.state[t.name][n] < 0 else "·")
                         .ljust(len(t.name)) for t in tfs)
        fields.append({"name": "Trend windows", "value": f"`{' '.join(t.name for t in tfs)}`\n`{strip}`", "inline": False})
        fields.append({"name": "Track record", "value": self.track_record()[:1024], "inline": False})
        embed = {
            "author": alerts._author(info),
            "title": "🎯 REVERSAL SIGNAL · dump → pump starting",
            "url": alerts.subnet_url(n),
            "description": desc,
            "color": COLOR,
            "fields": fields,
            "image": {"url": "attachment://signal.png"},
            "footer": {"text": f"Signal = price turns up (≥{self.trigger.enter:g}% over {self.trigger.name}) after a ≥{self.dump_pct:g}% drop from the 24h high · "
                               f"tracked for 24h, this card updates with the result · block #{sig.block}"},
            "timestamp": datetime.fromtimestamp(sig.ts, timezone.utc).isoformat(),
        }
        headline = (f"🎯 **{name}** REVERSAL SIGNAL — dump {fmt.pct(d_pc)}, now turning up {fmt.pct(b_pc)} · "
                    f"entry {fmt.price(entry)} τ · invalidation {fmt.price(lo)} τ")
        png = await asyncio.to_thread(self._chart, sig, info)
        return {"content": alerts.content([headline]), "embeds": [embed], "allowed_mentions": {"parse": []}}, png

    def _chart(self, sig: Sig, info) -> bytes:
        n = sig.netuid
        o, h, lo, c = (a[:, n] for a in self.bars.candles(sig.bucket, 96, 3))  # 24h of 15-minute candles
        exact = self.bars.exact_mask(sig.bucket, 96, 3, n)
        exact[-1] = True
        title = f"SN{n} · {info.name}" if info.name else f"SN{n}"
        return chart.render(o, h, lo, c, np.full(96, np.nan), title=title, badge="REVERSAL SIGNAL", direction=1,
                            x_labels=[(0, "-24h"), (32, "-16h"), (64, "-8h"), (95, "now")], interval="15m", exact=exact,
                            levels=[(sig.high, "24h high", GREY), (sig.low, "invalidation", RED)], mark=95)

    async def _slippage(self, n: int, entry: float) -> str | None:
        """What it costs to actually buy: average fill vs the quoted price, from the chain's swap simulator."""
        if self.sim is None:
            return None
        try:
            outs = await asyncio.wait_for(asyncio.gather(*(self.sim(n, s * 10**9) for s in SIZES)), 4)
        except Exception as e:
            log.debug("slippage sim failed: %s", e)
            return None
        parts = []
        for size, out in zip(SIZES, outs):
            if out:
                parts.append(f"{size} τ `{fmt.pct((size * 1e9 / out / entry - 1) * 100)}`")
        return " · ".join(parts) or None

    async def _post(self, sig: Sig) -> None:
        try:
            payload, png = await self._card(sig)
            sig.embed = payload["embeds"][0]
            if not self.dry_run:
                sig.msg_id = await self.discord.send(payload, files={"signal.png": png})
                if self.m.board is not None:
                    self.m.board.stale = True
            self._save()
        except Exception:
            log.exception("signal card failed")

    def _edit(self, sig: Sig) -> None:
        if not sig.msg_id or not sig.embed or self.dry_run:
            return
        emb = sig.embed
        emb["fields"] = [f for f in emb["fields"] if f["name"] != "Result"] + [
            {"name": "Result", "value": self._result(sig), "inline": False}]
        if sig.done and "24h" in sig.marks:
            r = sig.marks["24h"]
            emb["title"] = f"{'✅' if r > 0 else '❌'} REVERSAL SIGNAL · {fmt.pct(r)} after 24h"
            emb["color"] = WIN if r > 0 else LOSS
        elif sig.invalidated is not None:
            emb["title"] = "⚠️ REVERSAL SIGNAL · invalidated (closed below the dump's low)"
            emb["color"] = LOSS
        self.discord.edit_later(sig.msg_id, {"embeds": [emb]})

    # ── board ────────────────────────────────────────────────────────────

    def watching(self, bucket: int) -> list[tuple[float, float, str]]:
        """Subnets that could signal next: ≥ dump_pct below their 24h high, not turned up yet, and not
        signalled in the last 12h. → (ready, dump %, board row), closest to a signal first. `ready` is how
        far the trigger window is toward turning up (100% = the signal fires)."""
        tg, f = self.trigger, self.fit
        if f is None:
            return []
        closes = self.bars.closes(bucket, LOOKBACK + 1, 1).astype(np.float64)
        with np.errstate(invalid="ignore"):
            dd = (closes[-1] / closes.max(axis=0) - 1) * 100
        out = []
        for n in range(1, min(closes.shape[1], len(f.ok))):
            if np.isnan(dd[n]) or dd[n] > -self.dump_pct or not f.ok[n] or not self.watched(n):
                continue
            if bucket - self._last.get(n, -10**9) < COOLDOWN or self._st[n] == 1:
                continue
            # a signal needs the line to move enough AND steadily AND to rise bar after bar:
            # readiness is the weakest of those, so 100% really means "fires now"
            ready = min(max(float(f.change[n]), 0.0) / tg.enter, float(f.r2[n]) / tg.r2, 0.99)
            if int(f.shape[n]) != 1:
                ready = min(ready, 0.5)
            move = (float(f.last[n]) / float(f.first[n]) - 1) * 100
            info = self.meta.info(n)
            link = f"[SN{n} · {info.name}]({alerts.subnet_url(n)})"
            out.append((ready, float(dd[n]),
                        f"`{fmt.pct(float(dd[n])):>8}  {fmt.pct(move):>8}  {ready * 100:>4.0f}%` {link}"))
        return sorted(out, key=lambda r: (-r[0], r[1]))

    def board_section(self, bucket: int, budget: int = 800) -> dict:
        rows = []
        for sig in sorted(self.active, key=lambda s: -s.bucket):
            info = self.meta.info(sig.netuid)
            link = f"[SN{sig.netuid} · {info.name}]({alerts.subnet_url(sig.netuid)})"
            # one icon per row: failed (fell below the dump's low) / up / down since the signal
            flag = "❌ " if sig.invalidated is not None else ("🟢 " if sig.last > 0 else "🔴 ")
            if sig.age(bucket) < 12:
                flag += "🆕 "
            ago = fmt.duration(max(sig.age(bucket), 1) * 25)
            rows.append(f"`{fmt.pct(sig.last):>8}  {ago:>7} ago  entry {fmt.price(sig.entry * 1e9)}` {flag}{link}")
        lines, used = [], 0
        for r in rows:
            if used + len(r) + 1 > budget:
                lines.append(f"… +{len(rows) - len(lines)} more")
                break
            lines.append(r)
            used += len(r) + 1
        st = self.stats
        n, up = self.record()
        rec = []
        if st and st["n"]:
            rec.append(f"backtest {st['days']:.0f}d: {st['win24']:.0f}% up after 24h, median {fmt.pct(st['med24'])}")
        if n:
            rec.append(f"live: {up}/{n} up")
        body = "\n".join(lines) if lines else "no signal in the last 24h"
        if lines:
            body += ("\n*% = price change since the signal · 🟢 up · 🔴 down · "
                     "❌ failed: price fell below the dump's low · 🆕 under 1h old*")
        if rec:
            body += "\n*Track record — " + " · ".join(rec) + "*"
        watch = self.watching(bucket)
        if watch:
            turning = sum(1 for r in watch if r[0] > 0)
            body += (f"\n\n**👀 Watching — dumped ≥{self.dump_pct:g}%, not turned up yet ({len(watch)})**\n"
                     f"`{'off high':>8}  {'last ' + self.trigger.name:>8}  {'ready':>5}`")
            used, shown = 0, 0
            for _, _, r in watch:
                if used + len(r) + 1 > 720 or shown >= 8:
                    break
                body += "\n" + r
                used += len(r) + 1
                shown += 1
            if shown < len(watch):
                body += f"\n… +{len(watch) - shown} more, further from turning"
            body += (f"\n*off high = below its 24h high · ready = how close the last {self.trigger.name} is to a "
                     f"{self.trigger.enter:g}% steady rise (100% = a signal fires) · {turning} rising now*")
        count = f"{len(rows)} signal{'s' * (len(rows) != 1)} in the last 24h"
        return {"title": f"🎯 Reversal signals — dump → pump starting · {count}", "color": COLOR,
                "description": body}

    # ── backtest (keeps the track record current) ────────────────────────

    async def _triggers(self, closes: np.ndarray) -> list[tuple[int, int, float, float, float]]:
        """Replay this exact rule over a [time, subnet] matrix of 5-minute closes (the first LOOKBACK rows
        are context). → (row, netuid, price, 24h high, dump low) per signal. Yields to the event loop."""
        tf = self.trigger
        total, w = closes.shape
        ones = np.ones(w)
        st = np.zeros(w, dtype=np.int8)
        last: dict[int, int] = {}
        out = []
        for i in range(LOOKBACK, total):
            f = fit_all(closes[i - tf.points + 1:i + 1], ones)
            up = f.ok & (f.change >= tf.enter) & (f.r2 >= tf.r2) & (f.shape == 1)
            dn = f.ok & (f.change <= -tf.enter) & (f.r2 >= tf.r2) & (f.shape == -1)
            target = np.where(up, 1, np.where(dn, -1, 0)).astype(np.int8)
            st[(st != 0) & (~f.ok | (st * f.change < tf.enter * EXIT_FRACTION) | (f.r2 < EXIT_R2))] = 0
            flip = (target != 0) & (target != st)
            ups = np.nonzero(flip & (target == 1))[0]
            st[flip] = target[flip]
            for n in ups:
                n = int(n)
                if n == 0 or not self.watched(n) or i - last.get(n, -10**9) < COOLDOWN:
                    continue
                win = closes[i - LOOKBACK:i + 1, n]
                if np.isnan(win).any() or (win[-1] / win.max() - 1) * 100 > -self.dump_pct:
                    continue
                last[n] = i
                i_hi = int(win.argmax())
                out.append((i, n, float(win[-1]), float(win[i_hi]), float(win[i_hi:].min())))
            if i % 25 == 0:
                await asyncio.sleep(0)
        return out

    async def backtest(self, end_bucket: int, days: int = 7) -> dict | None:
        """The rule's track record over the stored history: what happened 6h and 24h after each signal."""
        closes = self.bars.closes(end_bucket, days * 288 + LOOKBACK, 1).astype(np.float64)
        total = closes.shape[0]
        r6, r24, best, worst = [], [], [], []
        for i, n, price, _, _ in await self._triggers(closes):
            if i + 72 < total:
                r6.append((closes[i + 72, n] / price - 1) * 100)
            if i + TRACK < total:
                seg = closes[i:i + TRACK + 1, n] / price * 100 - 100
                r24.append(seg[-1])
                best.append(seg.max())
                worst.append(seg.min())
        if not r24:
            return None
        self.stats = {"days": days, "n": len(r24), "win24": float(np.mean(np.array(r24) > 0) * 100),
                      "med24": float(np.median(r24)), "mean24": float(np.mean(r24)),
                      "med6": float(np.median(r6)) if r6 else 0.0, "best": float(np.mean(best)),
                      "worst": float(np.mean(worst))}
        return self.stats

    async def seed(self, end_bucket: int) -> int:
        """At startup: rebuild the last 24h of signals from price history (no cards), so the board lists
        recent signals with their real outcomes straight away and nothing is lost over a restart."""
        warm = 3 * 288  # let the 1h state machine and the 12h spacing settle before the day we keep
        closes = self.bars.closes(end_bucket, warm + TRACK + LOOKBACK, 1).astype(np.float64)
        total = closes.shape[0]
        added = 0
        for i, n, price, high, low in await self._triggers(closes):
            bucket = end_bucket - (total - 1 - i)
            if end_bucket - bucket >= TRACK:
                continue
            self._last[n] = max(self._last.get(n, 0), bucket)
            if any(s.netuid == n and abs(s.bucket - bucket) <= 2 for s in self.active + self.history):
                continue
            self.active.append(Sig(n, bucket, bucket * BUCKET + BUCKET - 1, time.time() - (end_bucket - bucket) * 300,
                                   price, high, low))
            added += 1
        if added:
            self.update(end_bucket * BUCKET + BUCKET - 1)
            self._save()
        return added

    # ── persistence ──────────────────────────────────────────────────────

    def _save(self) -> None:
        keep = time.time() - 14 * 86400
        self.history = [s for s in self.history if s.ts >= keep]
        try:
            self.path.write_text(json.dumps([asdict(s) for s in self.history + self.active]))
        except OSError as e:
            log.debug("signals save failed: %s", e)

    def _load(self) -> None:
        try:
            rows = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return
        for r in rows:
            try:
                sig = Sig(**r)
            except TypeError:
                continue
            (self.history if sig.done else self.active).append(sig)
            self._last[sig.netuid] = max(self._last.get(sig.netuid, 0), sig.bucket)
