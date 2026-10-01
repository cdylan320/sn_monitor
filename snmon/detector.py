"""Pump / dump detection.

For every subnet and every window W (in blocks) we keep the sliding min and max price over
the last W blocks (monotonic deques → O(1) per block). A PUMP fires when the price is
>= threshold above the window's low; a DUMP when it's >= threshold below the window's high.
So a move is caught on the very first block where it crosses the line, whatever its speed.

Anti-spam "episodes": once a subnet alerts, it only alerts again when the price is a further
REALERT_STEP_PCT away from the last alerted price — "continues" if same direction, "reversal"
if it turned. An episode ends after EPISODE_TTL with no fresh trigger.
"""
from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import dataclass


@dataclass
class Signal:
    netuid: int
    direction: int          # +1 pump, -1 dump
    kind: str               # "new" | "more" | "reversal"
    from_price: int         # rao; the window low (pump) / high (dump)
    to_price: int
    from_block: int
    to_block: int
    pct: float              # move from from_price → to_price
    window: int             # the window (blocks) whose threshold tripped
    threshold: float
    anchor_price: int       # where this episode started
    anchor_block: int
    prev_alert_price: int | None  # for "more": price at the previous alert

    @property
    def total_pct(self) -> float:
        return (self.to_price / self.anchor_price - 1) * 100

    @property
    def span_blocks(self) -> int:
        return self.to_block - self.from_block


@dataclass
class _Episode:
    direction: int
    anchor_price: int
    anchor_block: int
    last_price: int
    last_block: int
    until: int


class _Window:
    __slots__ = ("size", "threshold", "lo", "hi")

    def __init__(self, size: int, threshold: float) -> None:
        self.size = size
        self.threshold = threshold
        self.lo: deque[tuple[int, int]] = deque()  # increasing prices
        self.hi: deque[tuple[int, int]] = deque()  # decreasing prices

    def push(self, block: int, price: int) -> None:
        lo, hi = self.lo, self.hi
        while lo and lo[-1][1] >= price:
            lo.pop()
        lo.append((block, price))
        while hi and hi[-1][1] <= price:
            hi.pop()
        hi.append((block, price))
        edge = block - self.size
        while lo[0][0] < edge:
            lo.popleft()
        while hi[0][0] < edge:
            hi.popleft()


class Detector:
    def __init__(self, windows: list[tuple[int, float]], realert_step_pct: float, episode_ttl_blocks: int,
                 keep_blocks: int = 0) -> None:
        self.window_spec = windows
        self.realert_step = realert_step_pct
        self.ttl = episode_ttl_blocks
        self.keep = max(keep_blocks, max(w for w, _ in windows)) + 2
        self.history: OrderedDict[int, dict[int, int]] = OrderedDict()
        self._win: dict[int, list[_Window]] = {}
        self._episodes: dict[int, _Episode] = {}
        self.last_block = 0

    # ── public ───────────────────────────────────────────────────────────

    def update(self, block: int, prices: dict[int, int], watched=lambda n: n != 0, silent: bool = False) -> list[Signal]:
        if block <= self.last_block:
            # Re-org (same height, different block) or late block: rewrite history and rebuild.
            self.history[block] = prices
            self._rebuild()
            return []
        self.history[block] = prices
        while len(self.history) > self.keep:
            self.history.popitem(last=False)
        self.last_block = block

        for n in [n for n in self._win if n not in prices]:
            self.reset(n)  # subnet vanished (deregistered)

        signals: list[Signal] = []
        for n, p in prices.items():
            if p <= 0 or not watched(n):
                continue
            wins = self._win.get(n)
            if wins is None:
                wins = self._win[n] = [_Window(w, t) for w, t in self.window_spec]
            for w in wins:
                w.push(block, p)
            sig = self._check(n, block, p, wins)
            if sig and not silent:
                signals.append(sig)
        signals.sort(key=lambda s: -abs(s.pct))
        return signals

    def reset(self, netuid: int) -> None:
        """Forget a subnet (e.g. netuid was deregistered and re-registered)."""
        self._win.pop(netuid, None)
        self._episodes.pop(netuid, None)
        for prices in self.history.values():
            prices.pop(netuid, None)

    def price_ago(self, netuid: int, blocks: int) -> int | None:
        """Price `blocks` ago (nearest earlier block we have, within 2 blocks)."""
        target = self.last_block - blocks
        for b in range(target, target - 3, -1):
            p = self.history.get(b, {}).get(netuid)
            if p:
                return p
        return None

    def change(self, netuid: int, blocks: int) -> float | None:
        now = self.history.get(self.last_block, {}).get(netuid)
        then = self.price_ago(netuid, blocks)
        if not now or not then:
            return None
        return (now / then - 1) * 100

    # ── internals ────────────────────────────────────────────────────────

    def _rebuild(self) -> None:
        self._win.clear()
        for b, prices in self.history.items():
            for n, p in prices.items():
                if p <= 0:
                    continue
                wins = self._win.setdefault(n, [_Window(w, t) for w, t in self.window_spec])
                for w in wins:
                    w.push(b, p)

    def _check(self, n: int, block: int, p: int, wins: list[_Window]):
        best_up = best_dn = None  # (severity, window, extreme_block, extreme_price, pct)
        for w in wins:
            lb, lp = w.lo[0]
            up = (p / lp - 1) * 100
            if up >= w.threshold:
                sev = up / w.threshold
                if best_up is None or sev > best_up[0]:
                    best_up = (sev, w, lb, lp, up)
            hb, hp = w.hi[0]
            dn = (p / hp - 1) * 100
            if -dn >= w.threshold:
                sev = -dn / w.threshold
                if best_dn is None or sev > best_dn[0]:
                    best_dn = (sev, w, hb, hp, dn)

        ep = self._episodes.get(n)
        if ep is not None and block > ep.until:
            del self._episodes[n]
            ep = None

        if best_up and best_dn:
            # V or Λ inside the window: the more recent pivot is the move happening now.
            pick = best_up if (best_up[2], best_up[0]) > (best_dn[2], best_dn[0]) else best_dn
        else:
            pick = best_up or best_dn
        if pick is None:
            return None
        sev, w, xb, xp, pct = pick
        d = 1 if pick is best_up else -1

        if ep is not None:
            # While an episode runs, stay quiet until the price is a full step away from what
            # we last reported — in either direction. Stops ping-pong on thin pools.
            ep.until = block + self.ttl
            if (p / ep.last_price - 1) * 100 * d < self.realert_step:
                return None

        if ep is None or ep.direction != d:
            kind = "new" if ep is None else "reversal"
            self._episodes[n] = _Episode(d, xp, xb, p, block, block + self.ttl)
            return Signal(n, d, kind, xp, p, xb, block, pct, w.size, w.threshold, xp, xb, None)

        # Same direction, a full step further: the move continues.
        prev = ep.last_price
        prev_block = ep.last_block
        ep.last_price, ep.last_block = p, block
        return Signal(n, d, "more", prev, p, prev_block, block, (p / prev - 1) * 100, w.size, w.threshold,
                      ep.anchor_price, ep.anchor_block, prev)
