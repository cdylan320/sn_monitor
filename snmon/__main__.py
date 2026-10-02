"""python -m snmon            run the monitor
   python -m snmon test       post sample alerts to Discord (see exactly what alerts look like)
   python -m snmon replay N   dry-run detection over the last N blocks of real history (no Discord)
   python -m snmon trend-test [netuid]   post a sample trend card (strongest live trend, or that subnet)
   python -m snmon news-test [netuid] [--post]   check the Discord login, show what counts as news in that
                                                 subnet's channel (default 78); --post sends the newest as TEST
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time

from . import alerts, config, fmt
from .chain import PRICE_ALL, Block, decode_prices, fetch_history
from .detector import Detector, Signal
from .rpc import Rpc


def _logging() -> None:
    sys.stdout.reconfigure(line_buffering=True)  # pm2/file output: every line immediately
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")


async def _run() -> None:
    from .app import App
    app = App(config.load())
    try:
        await app.run()
    finally:
        await app.close()


async def _test() -> None:
    """Send one message showing a pump, a dump and a pending alert built from live prices."""
    from .discord import Discord
    from .meta import Meta, Trade

    cfg = config.load()
    meta = Meta(cfg.endpoints[0])
    await meta.refresh()
    rpc = Rpc(cfg.endpoints[0])
    rpc.start()
    head = await rpc.call("chain_getHeader", [], timeout=15)
    number = int(head["number"], 16)
    h = await rpc.call("chain_getBlockHash", [number])
    prices = decode_prices(bytes.fromhex((await rpc.call("state_call", [PRICE_ALL, "0x", h]))[2:]))
    top = sorted((s for s in meta.subnets.values() if s.netuid in prices and s.netuid), key=lambda s: -s.pool_tao)
    a, b = top[0], top[1]
    now = time.time()
    blk = Block(number, h, prices, "lite", "lite", time.perf_counter() - 0.064, time.perf_counter(), now, now - 2.9)
    pa, pb = prices[a.netuid], prices[b.netuid]
    pump = Signal(a.netuid, 1, "new", int(pa / 1.0523), pa, number - 2, number, 5.23, 5, 3.0, int(pa / 1.0523), number - 2, None)
    dump = Signal(b.netuid, -1, "new", int(pb / 0.9587), pb, number - 1, number, -4.13, 1, 2.0, int(pb / 0.9587), number - 1, None)
    ctx = alerts.Context(trend={"1m": 5.23, "5m": 6.10, "1h": 4.02, "24h": 11.7}, tao_usd=None)
    ctx2 = alerts.Context(trend={"1m": -4.13, "5m": -4.40, "1h": -2.2, "24h": 1.3}, tao_usd=None)
    e1 = alerts.confirmed_embed(pump, a, blk, ctx)
    e2 = alerts.confirmed_embed(dump, b, blk, ctx2)
    # a bot burst: the same 75 τ limit order fired 6× from two wallets → one card
    wallets = ("5H1ESpU5vXHf7hQyDP6dREDCq3zxCvR4Mc8UaVZJEDHnApQX", "5EkPFt2wfvR5Ga8wNgjpXrWxsA7S6hKa3Wn6r8C6uYmCPfX")
    burst = [alerts.Pending(f"0x{i:064x}", a.netuid, True, 75.0, None, wallets[i % 2], pa, int(pa * 1.0281),
                            number, 3400 + i, "add_stake_limit", limit=int(pa * 1.06)) for i in range(6)]
    g = alerts.PendingGroup(a.netuid, burst[0], deadline=number + 10)
    for t in burst[1:]:
        g.trades[t.tx_hash] = t
    g.price_after, g.priced_n = int(pa * 1.06), len(burst)
    e3 = alerts.pending_embed(g, a)
    content = alerts.content(["🧪 **TEST — sample alerts, not real market moves**", alerts.headline(pump, a),
                              alerts.headline(dump, b), alerts.pending_headline(g, a)])
    d = Discord(cfg.webhook_url)
    await d.start()
    t0 = time.perf_counter()
    mid = await d.send({"content": content, "embeds": [e1, e2, e3], "allowed_mentions": {"parse": []}})
    print(f"sent test message {mid} in {(time.perf_counter() - t0) * 1000:.0f} ms")
    # exercise the edit path the way a real alert gets its trades + landed status
    e1["fields"].append(alerts.trades_field([
        Trade(number - 1, a.netuid, True, 212.5, 0, "5GW9Nb89AR9SVvAuQW2b62KZkTYAcbJ2yAW4UGWx8bxrVoPD"),
        Trade(number, a.netuid, True, 100.0, 0, "5E2LP6EnZ54m3wS8s1yPvD5c3xo71kQroBw7aUVK32TKeZ5u"),
        Trade(number, a.netuid, False, 4.2, 0, "5CQBet4zbuSbnewGBewuK7HA6HeCFYYyWcnYHMgnmd6WLAPA"),
    ], pump))
    g.landed[burst[0].tx_hash] = number + 1
    g.moves[number + 1] = 2.79
    g.finalize(10)
    e3 = alerts.pending_embed(g, a)
    d.edit_later(mid, {"embeds": [e1, e2, e3]})
    await asyncio.sleep(2)
    print("edited test message (trades + landed status)")
    await d.close()
    await rpc.stop()


async def _trend_test(netuid: int | None) -> None:
    """Post one sample trend card to the trend webhook, built from live data."""
    import numpy as np
    from .bars import Bars
    from .discord import Discord
    from .meta import Meta
    from .trend import Story, TrendMonitor, parse_timeframes

    cfg = config.load()
    if not cfg.trend_webhook_url:
        raise SystemExit("TREND_WEB_HOOK_URL is not set")
    meta = Meta(cfg.endpoints[0])
    await meta.refresh()
    hist = [Rpc(u) for u in dict.fromkeys([cfg.archive_endpoint, *cfg.mempool_endpoints])]
    for r in hist:
        r.start()
    await asyncio.gather(*(r.wait_up(10) for r in hist))
    head = int((await hist[0].call("chain_getHeader", [], timeout=15))["number"], 16)
    bars = Bars(config.ROOT / "data" / "bars.db")
    bars.load(head)
    print(f"backfilled {await bars.backfill(hist, head)} bars")
    bars.put_sample(head, decode_prices(bytes.fromhex(
        (await hist[0].call("state_call", [PRICE_ALL, "0x", await hist[0].call("chain_getBlockHash", [head])]))[2:])))
    d = Discord(cfg.trend_webhook_url)
    await d.start()
    tm = TrendMonitor(parse_timeframes(cfg.trend_timeframes), bars, meta, d, cfg.watched, cfg.trend_story_hours)
    tm.evaluate(head, silent=True)
    if netuid is None:  # strongest qualifying trend right now, on the longest timeframe that has one
        best = None
        for tf in tm.tfs:
            f = tm.fits[tf.name]
            score = np.where(f.ok & (f.r2 >= tf.r2) & (np.abs(f.change) >= tf.enter) & (f.shape != 0),
                             np.abs(f.change) / tf.enter, 0)
            score[0] = 0
            if score.max() > 0:
                best = (tf, int(score.argmax()))
        if best is None:
            raise SystemExit("no subnet is trending right now")
        tf, netuid = best
    else:
        f24 = {tf.name: abs(tm.fits[tf.name].change[netuid]) / tf.enter for tf in tm.tfs}
        tf = next(t for t in tm.tfs if t.name == max(f24, key=f24.get))
    direction = 1 if tm.fits[tf.name].change[netuid] > 0 else -1
    s = Story(netuid, direction, "new", head, head, [tf.name])
    payload, png = await tm._build(s, head)
    payload["content"] = payload["content"].replace("\u200b\n", "\u200b\n🧪 **TEST — sample trend card (live data)**\n", 1)
    mid = await d.send(payload, files={"trend.png": png})
    print(f"posted trend card for SN{netuid} ({tf.name}) → message {mid}")
    (config.ROOT / "data" / "trend_test.png").write_bytes(png)
    # then swap in the chart rebuilt from every block, as the live monitor does
    tm.history = Rpc(cfg.trend_history_endpoint)
    tm.history.start()
    await tm.history.wait_up(10)
    s.msg_id = mid
    s.posted.set()
    t0 = time.perf_counter()
    await tm._refine(s)
    await asyncio.sleep(3)  # let the edit go out
    print(f"refined with every-block history in {time.perf_counter() - t0:.0f}s")
    await tm.history.stop()
    await d.close()
    for r in hist:
        await r.stop()


async def _capture(sent: list, payload: dict):
    sent.append(payload)
    return None


async def _news_test(netuid: int, post: bool) -> None:
    from .discord import Discord
    from .meta import Meta
    from .news import NewsMonitor

    cfg = config.load()
    if not cfg.news_discord_token:
        raise SystemExit("NEWS_DISCORD_TOKEN is not set in .env")
    meta = Meta(cfg.endpoints[0])
    await meta.refresh()
    d = Discord(cfg.news_webhook_url)
    await d.start()
    rpc = Rpc(cfg.endpoints[0])
    rpc.start()
    await rpc.wait_up(10)
    prices = decode_prices(bytes.fromhex((await rpc.call("state_call", [PRICE_ALL, "0x"]))[2:]))
    await rpc.stop()
    nm = NewsMonitor(cfg.news_discord_token, d, meta, price_now=prices.get, change=lambda n, b: None,
                     guild_hint=cfg.news_guild, min_team_chars=cfg.news_min_team_chars)
    await nm.client.start()
    from datetime import datetime, timezone
    created = datetime.fromtimestamp(((int(nm.client.user["id"]) >> 22) + 1420070400000) / 1000, timezone.utc)
    print(f"logged in as {nm.client.user.get('username')} ✓ (account created {created:%Y-%m-%d})")
    await nm.load_structure()
    subnet_chans = [c for c in nm.channels.values() if c.netuid is not None]
    print(f"server: {nm.guild_name} · {len(subnet_chans)} subnet channels · "
          f"{sum(1 for c in subnet_chans if c.team_users or c.team_roles)} with a team detected")
    print("staff roles:", ", ".join(sorted(nm.roles[r] for r in nm.staff_roles)) or "none")
    ch = next((c for c in subnet_chans if c.netuid == netuid), None)
    if ch is None:
        raise SystemExit(f"no channel found for SN{netuid}")
    print(f"\n#{ch.name}: team users {len(ch.team_users)}, team roles "
          f"{[nm.roles.get(r, r) for r in ch.team_roles]}")
    msgs = await nm.client.get(f"/channels/{ch.id}/messages", limit=100)
    news = []
    for m in sorted(msgs, key=lambda m: int(m["id"])):
        m.setdefault("guild_id", nm.guild_id)
        await nm._ensure_roles(m)
        v = nm.classify(m, ch)
        nm._remember(m, ch)
        if v:
            news.append((m, v))
            who = m["author"].get("global_name") or m["author"]["username"]
            text = (m.get("content") or "").replace("\n", " ")[:90]
            print(f"  NEWS {v[0]:<12} {m['timestamp'][:16]}  {who} ({v[1] or 'community'}): {text}")
    print(f"→ {len(news)} of the last {len(msgs)} messages would be posted"
          + ("" if not nm._member_lookup_off else " (member roles via gateway only)"))
    if post and news:
        newest = {}
        for m, (kind, who) in news:
            newest[kind] = (m, who)
        sent = []
        nm.discord = type("Capture", (), {"send": staticmethod(lambda p: _capture(sent, p))})()
        for kind, (m, who) in newest.items():  # newest of each kind, through the real posting path
            if "--only-x" in sys.argv and kind != "x_post":
                continue
            await nm._post(m, ch, kind, who)
        for payload in sent:
            payload["content"] = payload["content"].replace("\u200b\n", "\u200b\n🧪 **TEST — sample news card**\n", 1)
            print("posted", await d.send(payload))
    await nm.client.close()
    await d.close()


async def _replay(n: int) -> None:
    """Run the detector over real history and print what would have alerted."""
    from .meta import Meta

    cfg = config.load()
    meta = Meta(cfg.endpoints[0])
    meta_task = asyncio.ensure_future(meta.refresh())
    nodes = [Rpc(u) for u in dict.fromkeys([cfg.archive_endpoint, *cfg.endpoints])]
    for r in nodes:
        r.start()
    await asyncio.gather(*(r.wait_up(10) for r in nodes))
    nodes = [r for r in nodes if r.up]
    head = int((await nodes[0].call("chain_getHeader", [], timeout=15))["number"], 16)
    t0 = time.perf_counter()
    # pruned nodes only keep recent state; long replays lean on archive-style nodes
    deep = [r for r in nodes if r.name in ("archive", "onfinality")] if n > 250 else nodes
    rows = await fetch_history(deep or nodes, head - n, head)
    await meta_task
    print(fmt.dim(f"fetched {len(rows)} blocks (#{rows[0][0]}–#{rows[-1][0]}) in {time.perf_counter() - t0:.1f}s"))
    det = Detector(cfg.windows, cfg.realert_step_pct, cfg.episode_ttl_blocks, keep_blocks=300)
    total = 0
    t_det = 0.0
    for b, prices in rows:
        t = time.perf_counter()
        sigs = det.update(b, prices, cfg.watched)
        t_det += time.perf_counter() - t
        for s in sigs:
            total += 1
            print(f"#{b} {alerts.console_alert(s, meta.info(s.netuid))}")
    print(fmt.bold(f"\n{total} alerts over {len(rows)} blocks (~{len(rows) * 12 / 3600:.1f}h) · "
                   f"detector {t_det / len(rows) * 1e6:.0f} µs/block"))
    for r in nodes:
        await r.stop()


def _go(coro) -> None:
    try:
        import uvloop
        asyncio.run(coro, loop_factory=uvloop.new_event_loop)
    except ImportError:
        asyncio.run(coro)


def main() -> None:
    _logging()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    try:
        if cmd == "run":
            _go(_run())
        elif cmd == "test":
            _go(_test())
        elif cmd == "trend-test":
            _go(_trend_test(int(sys.argv[2]) if len(sys.argv) > 2 else None))
        elif cmd == "news-test":
            args = [a for a in sys.argv[2:] if not a.startswith("--")]
            _go(_news_test(int(args[0]) if args else 78, "--post" in sys.argv))
        elif cmd == "replay":
            _go(_replay(int(sys.argv[2]) if len(sys.argv) > 2 else 900))
        else:
            print(__doc__)
            sys.exit(2)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
