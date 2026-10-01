"""Run: .venv/bin/python -m pytest tests -q   (or just: .venv/bin/python tests/test_detector.py)"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.detector import Detector  # noqa: E402

WINDOWS = [(1, 2.0), (5, 3.0), (25, 4.0), (300, 7.0)]
P = 10_000_000  # 0.01 TAO


def det():
    return Detector(WINDOWS, realert_step_pct=3.0, episode_ttl_blocks=75)


def feed(d, start, prices, n=1):
    out = []
    for i, p in enumerate(prices):
        out.append(d.update(start + i, {0: 10**9, n: int(p)}))
    return out


def test_flat_no_alerts():
    d = det()
    assert not any(feed(d, 1, [P] * 400))


def test_single_block_pump_fires_immediately():
    d = det()
    feed(d, 1, [P] * 10)
    sigs = d.update(11, {1: int(P * 1.025)})
    assert len(sigs) == 1
    s = sigs[0]
    assert s.direction == 1 and s.kind == "new" and s.span_blocks == 1
    assert abs(s.pct - 2.5) < 1e-6 and s.from_price == P


def test_below_threshold_silent():
    d = det()
    feed(d, 1, [P] * 10)
    assert d.update(11, {1: int(P * 1.019)}) == []


def test_slow_grind_caught_by_longer_window():
    d = det()
    # +0.65% per block: never 2% in one block, but 1.0065^5 = +3.29% within 5 blocks
    prices = [P * (1.0065 ** i) for i in range(12)]
    res = feed(d, 1, prices)
    fired = [(i, s) for i, sigs in enumerate(res) for s in sigs]
    assert fired, "grind should alert"
    i, s = fired[0]
    assert s.window == 5 and s.pct >= 3.0 and i == 5  # first block where the 5-block rise >= 3%
    assert s.span_blocks == 5


def test_episode_suppresses_then_steps():
    d = det()
    feed(d, 1, [P] * 5)
    first = d.update(6, {1: int(P * 1.03)})
    assert first and first[0].kind == "new"
    # small further rise: suppressed
    assert d.update(7, {1: int(P * 1.04)}) == []
    # another +3% beyond the last alert price → "more"
    more = d.update(8, {1: int(P * 1.03 * 1.031)})
    assert more and more[0].kind == "more" and more[0].direction == 1
    assert abs(more[0].total_pct - (1.03 * 1.031 - 1) * 100) < 0.01


def test_reversal_alerts_immediately():
    d = det()
    feed(d, 1, [P] * 5)
    assert d.update(6, {1: int(P * 1.05)})[0].direction == 1
    rev = d.update(7, {1: int(P * 1.05 * 0.965)})
    assert rev and rev[0].direction == -1 and rev[0].kind == "reversal"
    assert rev[0].from_price == int(P * 1.05)  # from the high


def test_ping_pong_on_thin_pool_is_suppressed():
    d = det()
    feed(d, 1, [P] * 5)
    assert d.update(6, {1: int(P * 1.03)})  # pump alert at 1.03
    # bounces of ±2.5% around the alerted price: real per-block moves, but nothing new to say
    quiet = feed(d, 7, [P * 1.03 * 0.975, P * 1.03, P * 1.03 * 0.975, P * 1.03 * 1.02])
    assert not any(quiet)
    # a genuine break 3%+ below the alerted price does alert, as a reversal
    rev = d.update(11, {1: int(P * 1.03 * 0.965)})
    assert rev and rev[0].kind == "reversal"


def test_episode_expires():
    d = Detector(WINDOWS, 3.0, episode_ttl_blocks=10)
    feed(d, 1, [P] * 5)
    assert d.update(6, {1: int(P * 1.03)})
    feed(d, 7, [P * 1.03] * 320)  # long quiet → windows and episode roll off
    again = d.update(327, {1: int(P * 1.03 * 1.03)})
    assert again and again[0].kind == "new"


def test_silent_backfill_sets_episode_without_alert():
    d = det()
    for b in range(1, 6):
        d.update(b, {1: P}, silent=True)
    assert d.update(6, {1: int(P * 1.05)}, silent=True) == []
    # live: same pump continuing slightly is not re-alerted
    assert d.update(7, {1: int(P * 1.055)}) == []


def test_dump_and_v_shape_picks_latest_move():
    d = det()
    feed(d, 1, [P] * 5)
    assert d.update(6, {1: int(P * 0.96)})[0].direction == -1
    # sharp recovery: price is now >2% above the recent low → pump (reversal), not another dump
    s = d.update(7, {1: int(P * 0.99)})
    assert s and s[0].direction == 1 and s[0].kind == "reversal"


def test_root_and_unwatched_ignored():
    d = det()
    for b in range(1, 5):
        d.update(b, {0: 10**9, 5: P})
    assert d.update(5, {0: 2 * 10**9, 5: int(P * 1.5)}, watched=lambda n: n not in (0, 5)) == []


def test_subnet_vanishes_and_reappears_no_false_alert():
    d = det()
    feed(d, 1, [P] * 5)
    d.update(6, {0: 10**9})  # deregistered
    assert d.update(7, {1: P * 5}) == []  # new subnet in same slot: fresh history


def test_reorg_same_height_rebuilds_without_alert():
    d = det()
    feed(d, 1, [P] * 5)
    assert d.update(5, {1: int(P * 1.1)}) == []  # same height, different block
    assert d.history[5][1] == int(P * 1.1)


def test_trend_change():
    d = det()
    feed(d, 1, [P] * 25 + [P * 1.1])
    assert abs(d.change(1, 25) - 10.0) < 1e-6


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} detector tests passed")
