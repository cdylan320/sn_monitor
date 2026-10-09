"""Run: .venv/bin/python tests/test_signals.py"""
import asyncio
import numpy as np
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.bars import BUCKET, Bars  # noqa: E402
from snmon.signals import Signals, parse_trigger  # noqa: E402
from snmon.trend import TrendMonitor, parse_timeframes  # noqa: E402

TFS = "1h:2:0.85:4:0.9,3h:3.5:0.8:5:0.85,6h:6:0.8,12h:8:0.7,24h:10:0.65,3d:12:0.6"
P = 10_000_000  # 0.01 TAO
SLOW, FAST = "1h:2:0.85", "15m:2:0.8"   # trigger windows: the tracking tests use the slow one, the default is FAST


class _Meta:
    def info(self, n):
        from snmon.meta import SubnetInfo
        return SubnetInfo(n, "test", "", 5000.0, 1, None, None)


class World:
    """A tiny market: subnet 1 follows the script, subnet 2 stays flat."""

    def __init__(self, trigger=SLOW, bounce_pct=0.0, sharp_pct=0.0, tiny_pct=1.0, flow_tao=0.0, flow_all=None):   # new triggers off unless a test asks
        tmp = Path(tempfile.mkdtemp())
        self.bars = Bars(tmp / "b.db")
        self.tm = TrendMonitor(parse_timeframes(TFS), self.bars, _Meta(), None, lambda n: True)
        self.sig = Signals(self.tm, self.bars, _Meta(), None, lambda n: True, tmp / "s.json", dry_run=True,
                           trigger=parse_trigger(trigger), bounce_pct=bounce_pct, sharp_pct=sharp_pct,
                           tiny_pct=tiny_pct, flow_tao=flow_tao, flow_all=flow_all)
        self.b = 400_000
        self.fired = []

    def step(self, price, live=True):
        block = self.b * BUCKET + 24
        self.bars.put_sample(block, {1: int(price), 2: P})
        self.tm.evaluate(block, silent=not live)
        if live:
            self.fired += self.sig.detect(block)
            self.sig.update(block)
        self.b += 1

    def run(self, prices, live=True):
        for p in prices:
            self.step(p, live)


def ramp(a, b, n):
    return [a + (b - a) * (i + 1) / n for i in range(n)]


async def _dump_then_bounce():
    w = World()
    w.run([P] * 600, live=False)                      # 2 days flat (warm-up)
    w.run(ramp(P, P * 0.90, 72))                      # -10% over 6h: the dump
    w.run([P * 0.90] * 24)                            # 2h at the low
    assert w.fired == [], "no signal while dumping or flat"
    w.run(ramp(P * 0.90, P * 0.93, 12))               # +3.3% in an hour, steadily: the bounce
    assert len(w.fired) == 1, f"expected one signal, got {len(w.fired)}"
    s = w.fired[0]
    for _ in range(100):                              # the card (with its chart) is built in the background
        if s.embed:
            break
        await asyncio.sleep(0.05)
    assert s.netuid == 1 and abs(s.high / (P / 1e9) - 1) < 1e-6 and abs(s.low / (P * 0.90 / 1e9) - 1) < 1e-6
    assert (s.entry / s.high - 1) * 100 <= -5, "entry must be ≥5% under the 24h high"
    assert s.embed and s.embed["title"].startswith("🎯 REVERSAL SIGNAL")
    names = [f["name"] for f in s.embed["fields"]]
    assert names[:3] == ["Entry (now)", "Invalidation", "Previous high"] and "Track record" in names
    return w, s


def test_signal_fires_once_at_the_bounce_after_a_dump():
    asyncio.run(_dump_then_bounce())


def test_no_signal_without_a_dump():
    async def go():
        w = World()
        w.run([P] * 600, live=False)
        w.run(ramp(P, P * 1.04, 12))                  # a 1h uptrend from flat: momentum, not a reversal
        await asyncio.sleep(0.05)
        assert w.tm.state["1h"][1] == 1 and w.fired == []
    asyncio.run(go())


def test_signal_is_tracked_and_invalidated():
    async def go():
        w, s = await _dump_then_bounce()
        assert s.bucket < w.b - 1, "the signal fires early in the bounce, not at its end"
        w.run([P * 0.94] * 12)                                   # holds its gains for the next hour
        want = (P * 0.94 / 1e9 / s.entry - 1) * 100              # +2.45% vs the entry an hour earlier
        assert abs(s.marks["1h"] - want) < 0.01 and s.best >= want - 0.01 and s.invalidated is None
        w.run(ramp(P * 0.94, P * 0.88, 24))                      # then it rolls over, below the dump's low
        assert s.invalidated is not None and s.worst < -3
        assert len(w.fired) == 1, "no new signal while still falling"
    asyncio.run(go())


def test_backtest_finds_the_same_signal():
    async def go():
        w, s = await _dump_then_bounce()
        w.run([s.entry * 1e9 * 1.03] * 300)                       # 25h later, +3%
        st = await w.sig.backtest(w.b - 1, days=2)
        assert st and st["n"] == 1 and abs(st["med24"] - 3.0) < 0.2 and st["win24"] == 100
    asyncio.run(go())


def test_watch_list_shows_dumped_subnets_until_they_signal():
    async def go():
        w = World()
        w.run([P] * 600, live=False)
        w.run(ramp(P, P * 0.90, 72))                               # the dump
        w.run([P * 0.90] * 24)
        bucket = w.b - 1
        watch = w.sig.watching(bucket)
        assert len(watch) == 1 and watch[0][0] == 0 and abs(watch[0][1] + 10) < 0.1   # -10% off high, not rising
        w.run(ramp(P * 0.90, P * 0.9075, 6))                       # starts to lift: +0.8% in 30 minutes
        ready = w.sig.watching(w.b - 1)[0][0]
        assert 0 < ready < 1 and w.fired == []                     # closer, but no signal yet
        w.run(ramp(P * 0.9075, P * 0.93, 9))
        assert len(w.fired) == 1 and w.sig.watching(w.b - 1) == []  # signalled → off the watch list
        section = w.sig.board_section(w.b - 1)
        assert "1 signal in the last 24h" in section["title"]
    asyncio.run(go())


def test_seed_rebuilds_recent_signals_without_duplicates():
    async def go():
        w, s = await _dump_then_bounce()
        w.run([P * 0.94] * 30)
        fresh = Signals(w.tm, w.bars, _Meta(), None, lambda n: True, Path(tempfile.mkdtemp()) / "s.json", dry_run=True,
                        trigger=parse_trigger(SLOW))
        assert await fresh.seed(w.b - 1) == 1 and len(fresh.active) == 1
        got = fresh.active[0]
        assert got.netuid == 1 and abs(got.bucket - s.bucket) <= 1 and abs(got.entry / s.entry - 1) < 0.005
        assert got.msg_id is None and got.last > 0          # tracked, but no card was posted for it
        assert await fresh.seed(w.b - 1) == 0               # running it again adds nothing
    asyncio.run(go())


def _dumped(trigger, bounce_pct=0.0):
    w = World(trigger, bounce_pct=bounce_pct)
    w.run([P] * 600, live=False)
    w.run(ramp(P, P * 0.90, 72))                       # -10% over 6h
    w.run([P * 0.90] * 24)                             # 2h at the low
    return w


def test_15m_trigger_fires_on_the_second_rising_bar():
    async def go():
        assert Signals.__init__.__defaults__ is not None and parse_trigger("").name == "15m"   # 15m is the default
        fast, slow = _dumped(FAST), _dumped(SLOW)
        lift = [P * 0.90 * 1.011, P * 0.90 * 1.022]    # two 5-minute bars, +1.1% each
        fast.run(lift[:1]); slow.run(lift[:1])
        assert fast.fired == [], "+1.1% over the window is not a turn yet"
        fast.run(lift[1:]); slow.run(lift[1:])
        assert len(fast.fired) == 1 and slow.fired == []   # 10 minutes into the bounce; the 1h window is nowhere near
        s = fast.fired[0]
        assert abs(s.entry / (P * 0.90 * 1.022 / 1e9) - 1) < 1e-6 and (s.entry / s.high - 1) * 100 <= -5
        await asyncio.sleep(0.3)
        assert "(15m)" in s.embed["description"] and "over 15m" in s.embed["footer"]["text"]
        fast.run([P * 0.90 * 1.022 * (1.01 ** i) for i in range(1, 12)])
        assert len(fast.fired) == 1, "once per subnet per 12 hours, however long it keeps rising"
    asyncio.run(go())


def test_15m_trigger_ignores_a_single_candle():
    async def go():
        w = _dumped(FAST)
        w.run([P * 0.90 * 1.03])                       # one bar jumps 3%…
        assert w.fired == [], "one candle is a pump, not a turn"
        w.run([P * 0.90 * 1.03] * 4)                   # …and it just sits there
        assert w.fired == []
        assert w.sig.watching(w.b - 1), "still on the watch list"
    asyncio.run(go())


def test_what_is_already_rising_at_start_is_not_a_signal():
    async def go():
        w = World(FAST)
        w.run([P] * 600, live=False)
        w.run(ramp(P, P * 0.90, 72), live=False)       # dumped and already bouncing while the monitor was down
        w.run([P * 0.90 * 1.011, P * 0.90 * 1.022], live=False)
        w.run([P * 0.90 * 1.033])                      # first block after the restart: the rise is under way
        assert w.fired == []
    asyncio.run(go())


def test_bounce_fires_on_a_sharp_v(tmp_path=None):
    async def go():
        w = World(FAST, bounce_pct=3.0)
        w.run([P] * 600, live=False)
        w.run(ramp(P, P * 0.88, 48), live=False)               # a 12% dump (context)
        w.run([P * 0.88], live=True)                           # arm at the low
        w.run([P * 0.88 * 1.04])                               # +4% in one candle off the low → bounce fires
        assert len(w.fired) == 1 and w.fired[0].netuid == 1
    asyncio.run(go())


def test_bounce_off_is_window_only(tmp_path=None):
    async def go():
        w = World(FAST, bounce_pct=0.0)
        w.run([P] * 600, live=False)
        w.run(ramp(P, P * 0.88, 48), live=False)
        w.run([P * 0.88], live=True)
        w.run([P * 0.88 * 1.05])                               # single candle, bounce off → no signal
        assert w.fired == []
    asyncio.run(go())


def _sharp(**kw):
    w = World(FAST, **kw)
    w.run([P] * 600, live=False)
    w.run([P * 0.98, P * 0.95, P * 0.92, P * 0.90], live=False)       # −10% in 20 minutes, history
    w.run([P * 0.90], live=True)                                      # arm at the low
    return w


def test_tiny_uptick_after_a_sharp_dump_fires_on_the_first_small_pump():
    async def go():
        w = _sharp(sharp_pct=8.0, tiny_pct=1.0)
        assert w.fired == []
        w.run([P * 0.90 * 1.004])
        assert w.fired == [], "+0.4% is below the 1% bar"
        w.run([P * 0.90 * 1.012])
        assert len(w.fired) == 1 and w.fired[0].why == "tiny", "fires on the first +1% — no 15m window"
        await asyncio.sleep(0.3)
        assert "first uptick after a sharp dump" in w.fired[0].embed["description"]
    asyncio.run(go())


def test_tiny_pump_ignores_slow_bleeds_and_small_dumps():
    async def go():
        slow = World(FAST, sharp_pct=8.0, tiny_pct=1.0)
        slow.run([P] * 600, live=False)
        slow.run(ramp(P, P * 0.88, 72), live=False)
        slow.run([P * 0.88], live=True)
        slow.run([P * 0.88 * 1.02])
        assert slow.fired == [] and not slow.sig._dumps               # −12% over 6h is not a sharp dump
        small = World(FAST, sharp_pct=8.0, tiny_pct=1.0)
        small.run([P] * 600, live=False)
        small.run([P * 0.98, P * 0.96, P * 0.94], live=False)         # −6%: under the bar
        small.run([P * 0.94], live=True)
        small.run([P * 0.94 * 1.02])
        assert small.fired == []
    asyncio.run(go())


def test_big_net_selloff_is_a_signal_only_when_switched_on_and_the_price_fell():
    async def go():
        net = {}
        on = World(FAST, flow_tao=500.0, flow_all=lambda: dict(net))
        on.run([P] * 600, live=False)
        on.run([P * 0.97, P * 0.95], live=False)                      # SN1 −5% (a real move); SN2 flat
        on.run([P * 0.95], live=True)                                 # the first live look only arms the detector
        assert on.fired == []
        net.update({1: -700.0, 2: -900.0})                            # SN1 sells 700 τ net, SN2 900 τ — now, live
        on.run([P * 0.95])
        assert [(x.netuid, x.why) for x in on.fired] == [(1, "flow")]  # SN2's 900 τ moved nothing: no signal
        on.run([P * 0.95])
        assert len(on.fired) == 1                                    # and not twice for the same sell-off
        off = World(FAST, flow_tao=0.0, flow_all=lambda: dict(net))
        off.run([P] * 600, live=False); off.run([P * 0.95], live=True); off.run([P * 0.95])
        assert off.fired == []                                       # default: off
    asyncio.run(go())


def test_backtest_replay_finds_the_tiny_pump_too():
    async def go():
        w = World(FAST, sharp_pct=8.0, tiny_pct=1.0)
        w.run([P] * 600, live=False)
        w.run([P * 0.98, P * 0.95, P * 0.92, P * 0.90, P * 0.90 * 1.012], live=False)
        w.run([P * 0.90 * 1.012] * 300, live=False)
        closes = w.bars.closes(w.b - 1, 3 * 288 + 288, 1).astype(np.float64)
        hits = await w.sig._triggers(closes)
        assert len(hits) == 1 and abs(hits[0][2] / (P * 0.90 * 1.012 / 1e9) - 1) < 1e-6   # same entry as live
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} signal tests passed")
