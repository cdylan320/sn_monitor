"""Wires everything together.

Hot path (per block, all in the main event loop, no SCALE library):
    header arrives on N nodes ─▶ first copy wins ─▶ state_call price_all on that node
    (hedged to a 2nd node) ─▶ decode ─▶ detector ─▶ Discord POST on a pre-warmed connection

Slow path (worker thread): names/liquidity refresh, per-block trade log for attribution,
mempool extrinsic decoding. Attribution and pending→landed status arrive as message edits,
so they never delay an alert.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import aiohttp
import orjson

from . import alerts, fmt
from .chain import PRICE_ALL, Block, Feed, blake2_256, decode_prices, fetch_history
from .config import BLOCK_SECONDS, ROOT, Config
from .detector import Detector, Signal
from .discord import Discord
from .lifecycle import SubnetWatch
from .mempool import Mempool
from .bars import BUCKET, Bars
from .meta import Meta, Trade
from .news import NewsMonitor
from .rpc import Rpc
from .signals import Signals, parse_trigger
from .trend import TrendBoard, TrendMonitor, parse_timeframes

log = logging.getLogger("snmon")

TREND = (("1m", 5), ("5m", 25), ("1h", 300))
DAY_BLOCKS = 24 * 3600 // BLOCK_SECONDS
TRADE_LOG_BLOCKS = 320
PENDING_TTL_BLOCKS = 10


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class App:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._hooks: dict[str, Discord] = {}
        self.discord = self._hook(cfg.price_webhook_url)   # PRICE_HOOK_URL: pump/dump + pending trades
        self.meta = Meta(cfg.endpoints[0])
        self.feed = Feed(cfg.endpoints, self.on_block)
        self.det = Detector(cfg.windows, cfg.realert_step_pct, cfg.episode_ttl_blocks, keep_blocks=300)
        self.tao_usd: float | None = None
        self.ref24: dict[int, int] = {}
        self.trades: dict[int, list[Trade]] = {}
        self.trades_ready: dict[int, asyncio.Event] = {}
        self.trade_log_start: int | None = None
        self.trades_failed: set[int] = set()
        self.pending_join: dict[int, alerts.PendingGroup] = {}  # netuid → card still taking new trades
        self.pending_open: list[alerts.PendingGroup] = []        # cards waiting for their txs to land
        self.ready = False
        self._early: list[Block] = []
        self.alert_count = 0
        self.pending_count = 0
        self.mempool: Mempool | None = None
        self._lat: list[float] = []
        self._archive: Rpc | None = None
        self._alerts_file = ROOT / "data" / "alerts.jsonl"
        self._alerts_file.parent.mkdir(exist_ok=True)
        self.subnets: SubnetWatch | None = None
        if cfg.subnet_alerts:
            self.subnet_discord = self._hook(cfg.subnet_webhook_url)   # the news channel unless given its own
            self.subnets = SubnetWatch(self.meta, self.subnet_discord, self._refresh_meta, ROOT / "data" / "subnets.json",
                                       dry_run=cfg.dry_run)
        self.news: NewsMonitor | None = None
        if cfg.news_webhook_url and cfg.news_discord_token:
            self.news_discord = self._hook(cfg.news_webhook_url)
            self.news = NewsMonitor(
                cfg.news_discord_token, self.news_discord, self.meta,
                price_now=lambda n: self.det.history.get(self.det.last_block, {}).get(n),
                change=self.det.change, guild_hint=cfg.news_guild,
                min_team_chars=cfg.news_min_team_chars, dry_run=cfg.dry_run)
        self.trend: TrendMonitor | None = None
        if cfg.trend_webhook_url:
            self.bars = Bars(ROOT / "data" / "bars.db")
            self.trend_discord = self._hook(cfg.trend_webhook_url)
            self.trend = TrendMonitor(parse_timeframes(cfg.trend_timeframes), self.bars, self.meta,
                                      self.trend_discord, self.watched, cfg.trend_story_hours)
            self.trend.dry_run = cfg.dry_run
            if cfg.trend_signals:
                self.trend.signals = Signals(
                    self.trend, self.bars, self.meta, self.trend_discord, self.watched, ROOT / "data" / "signals.json",
                    dump_pct=cfg.trend_signal_dump_pct, sim=lambda n, rao: self.feed.sim(True, n, rao),
                    flow=self._flow, dry_run=cfg.dry_run, trigger=parse_trigger(cfg.trend_signal_window),
                    bounce_pct=cfg.trend_signal_bounce_pct, sharp_pct=cfg.trend_signal_sharp_pct,
                    tiny_pct=cfg.trend_signal_tiny_pct, flow_tao=cfg.trend_signal_flow_tao,
                    flow_min_drop=cfg.trend_signal_flow_min_drop, flow_all=self._flow_all,
                    cooldown_hours=cfg.trend_signal_cooldown_hours)

    # ── lifecycle ────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._banner()
        for d in self._hooks.values():
            await d.start()
        try:
            hook = await self.discord.check()
            print(f"  {fmt.dim('discord')}    webhook “{hook.get('name')}” ✓")
        except Exception as e:
            log.error("Discord webhook check failed (%s) — monitoring anyway, alerts will retry", e)

        try:
            await asyncio.wait_for(self.meta.refresh(), 30)
            print(f"  {fmt.dim('subnets')}    {len(self.meta.subnets)} loaded (names, logos, liquidity)")
        except Exception as e:
            log.warning("subnet metadata not loaded yet (%s) — will retry", e)

        self.feed.start()
        self._archive = next((c for c in self.feed.clients if c.url == self.cfg.archive_endpoint), None)
        if self._archive is None:
            self._archive = Rpc(self.cfg.archive_endpoint)
            self._archive.start()

        await self._backfill()
        print(fmt.dim("  " + "─" * 66))
        self.ready = True
        for b in sorted(self._early, key=lambda b: b.number):
            self.on_block(b)
        self._early.clear()

        for coro in (self._meta_loop(), self._usd_loop(), self._ref24_loop(), self._health_loop(), self._summary_loop()):
            asyncio.create_task(coro)
        if self.trend:
            asyncio.create_task(self._trend_start())
        if self.news:
            asyncio.create_task(self._news_start())
        elif self.cfg.news_webhook_url:
            log.warning("news monitor is off: add NEWS_DISCORD_TOKEN (the alt account's token) to .env")
        if self.cfg.mempool:
            self.mempool = mp = Mempool(self.feed, self.meta, self.cfg.mempool_endpoints,
                                        self.cfg.mempool_min_pct, self.cfg.mempool_poll_ms,
                         price_now=lambda n: self.det.history.get(self.det.last_block, {}).get(n),
                         watched=self.watched, on_pending=self.on_pending)
            asyncio.create_task(mp.run())
        if self.cfg.startup_message and not self.cfg.dry_run:
            asyncio.create_task(self._startup_message())
        await asyncio.Event().wait()

    def _hook(self, url: str) -> Discord:
        """One Discord client per distinct webhook, so things posting to the same channel share its rate limit."""
        key = url.split("?")[0].rstrip("/")
        if key not in self._hooks:
            self._hooks[key] = Discord(url, dry_run=self.cfg.dry_run)
        return self._hooks[key]

    async def close(self) -> None:
        """Clean shutdown (pm2 restart): close HTTP sessions so nothing is left dangling."""
        if self.news:
            await self.news.client.close()
        for d in self._hooks.values():
            if d.session is not None:
                await d.session.close()

    def watched(self, netuid: int) -> bool:
        if not self.cfg.watched(netuid):
            return False
        if self.cfg.min_pool_tao > 0:
            info = self.meta.subnets.get(netuid)
            return info is None or info.pool_tao >= self.cfg.min_pool_tao
        return True

    # ── hot path ─────────────────────────────────────────────────────────

    def on_block(self, block: Block) -> None:
        if not self.ready:
            self._early.append(block)
            return
        prev = self.det.history.get(block.number - 1)
        if self.subnets:
            self.subnets.note(block.prices)
        signals = self.det.update(block.number, block.prices, self.watched)
        if signals:
            asyncio.create_task(self._dispatch(block, signals))
        self._status(block, prev, signals)
        if self.trend:  # queued behind the price alert, so it never delays it
            asyncio.create_task(self._trend_block(block))
        asyncio.create_task(self._after_block(block))

    async def _dispatch(self, block: Block, signals: list[Signal]) -> None:
        # ≤5 embeds per message keeps us under Discord's 6000-char cap even after trades are added
        for i in range(0, len(signals), 5):
            chunk = signals[i:i + 5]
            infos = [self.meta.info(s.netuid) for s in chunk]
            embeds = [alerts.confirmed_embed(s, inf, block, self._context(s.netuid)) for s, inf in zip(chunk, infos)]
            lines = [alerts.headline(s, inf) for s, inf in zip(chunk, infos)]
            mention = self._mention(max(abs(s.pct) for s in chunk))
            payload = {
                "content": alerts.content(lines, mention),
                "embeds": embeds,
                "allowed_mentions": {"parse": ["everyone", "roles", "users"] if mention else []},
            }
            t0 = time.perf_counter()
            msg_id = await self.discord.send(payload)
            sent_ms = (time.perf_counter() - block.t_head) * 1000
            if msg_id:
                print(fmt.dim(f"{now_str()}   ↳ Discord ✓ {len(chunk)} alert(s) · {sent_ms:.0f} ms from block arrival "
                              f"(post {(time.perf_counter() - t0) * 1000:.0f} ms)"))
            elif not self.cfg.dry_run:
                print(fmt.red(f"{now_str()}   ↳ Discord ✗ send failed"))
            if not self.cfg.dry_run:
                self._record(block, chunk, infos)
            if msg_id:
                asyncio.create_task(self._attribute(msg_id, embeds, chunk))

    def _context(self, netuid: int) -> alerts.Context:
        trend = {label: self.det.change(netuid, blocks) for label, blocks in TREND}
        ref, now = self.ref24.get(netuid), self.det.history.get(self.det.last_block, {}).get(netuid)
        young = self.meta.info(netuid).registered_at > self.det.last_block - DAY_BLOCKS
        trend["24h"] = (now / ref - 1) * 100 if ref and now and not young else None
        return alerts.Context(trend=trend, tao_usd=self.tao_usd)

    def _mention(self, pct: float) -> str:
        if self.cfg.mention and self.cfg.mention_pct > 0 and pct >= self.cfg.mention_pct:
            return self.cfg.mention
        return ""

    # ── follow-ups (edits) ───────────────────────────────────────────────

    async def _attribute(self, msg_id: str, embeds: list[dict], signals: list[Signal]) -> None:
        """Add a 'Trades' field (who bought/sold) to each embed once the block's events are decoded."""
        last = max(s.to_block for s in signals)
        ev = self.trades_ready.setdefault(last, asyncio.Event())
        try:
            await asyncio.wait_for(ev.wait(), 20)
        except asyncio.TimeoutError:
            return
        for emb, s in zip(embeds, signals):
            lo = (s.anchor_block if s.kind == "more" else s.from_block) + 1  # same span as the headline
            trades = [t for b in range(lo, s.to_block + 1) for t in self.trades.get(b, ()) if t.netuid == s.netuid]
            field = alerts.trades_field(trades, s)
            notes = []
            if self.trade_log_start and lo < self.trade_log_start:
                notes.append(f"trades indexed from #{self.trade_log_start}")
            missing = sum(1 for b in range(lo, s.to_block + 1) if b in self.trades_failed)
            if missing:
                notes.append(f"{missing} block(s) could not be read")
            if notes:
                if not trades:
                    field["value"] = "No stake/unstake found on this subnet."
                field["value"] = (field["value"] + f"\n_({'; '.join(notes)})_")[:1024]
            emb["fields"].append(field)
        self.discord.edit_later(msg_id, {"embeds": embeds})

    async def _after_block(self, block: Block) -> None:
        try:
            tr, life = await asyncio.wait_for(self.meta.trades(block.number, block.hash), 15)
            self.trades_failed.discard(block.number)
        except Exception as e:
            log.debug("trade log failed for #%d: %s", block.number, e)
            tr, life = [], []
            self.trades_failed.add(block.number)
        if life and self.subnets:
            asyncio.create_task(self.subnets.on_events(block.number, life))
        self.trades[block.number] = tr
        if self.trade_log_start is None:
            self.trade_log_start = block.number
        self.trades_ready.setdefault(block.number, asyncio.Event()).set()
        if self.trend and self.trend.ready and self.trend.signals:
            try:   # a big sell-off is judged now, with this block's trades in the log — same block, no lag
                self.trend.signals.detect_flow(block.number)
            except Exception:
                log.exception("flow signal check failed")
        for b in [b for b in self.trades if b < block.number - TRADE_LOG_BLOCKS]:
            self.trades.pop(b, None)
            self.trades_ready.pop(b, None)
            self.trades_failed.discard(b)
        if self.pending_open:
            await self._resolve_pending(block)

    async def _resolve_pending(self, block: Block) -> None:
        try:
            body = await self.feed.call("chain_getBlock", [block.hash])
            included = {blake2_256(bytes.fromhex(x[2:])) for x in body["block"]["extrinsics"]}
        except Exception as e:
            log.debug("block body fetch failed: %s", e)
            return
        prev = self.det.history.get(block.number - 1, {})
        for g in list(self.pending_open):
            new = [tx for tx in g.trades if tx in included and tx not in g.landed]
            for tx in new:
                g.landed[tx] = block.number
            if new:
                before, after = prev.get(g.netuid), block.prices.get(g.netuid)
                if before and after:
                    g.moves[block.number] = (after / before - 1) * 100
            done = len(g.landed) == g.n
            if done or block.number >= g.deadline:
                g.finalize(PENDING_TTL_BLOCKS)
                self.pending_open.remove(g)
                if self.pending_join.get(g.netuid) is g:
                    del self.pending_join[g.netuid]
                self._refresh_card(g)
            elif new:
                self._refresh_card(g)

    # ── mempool ──────────────────────────────────────────────────────────

    def on_pending(self, p: alerts.Pending) -> None:
        """Trades on the same subnet within ~2 blocks share one card (bots often fire the same
        order many times). The first trade posts instantly; later ones update the card."""
        self.pending_count += 1
        info = self.meta.info(p.netuid)
        g = self.pending_join.get(p.netuid)
        if g is not None and not g.closed and p.seen_after_block <= g.first.seen_after_block + 1:
            if p.tx_hash in g.trades:
                return
            g.trades[p.tx_hash] = p
            g.deadline = max(g.deadline, p.seen_after_block + PENDING_TTL_BLOCKS)
            print(fmt.dim(f"{now_str()}    ↳ +1 pending on SN{p.netuid} (×{g.n} · "
                          f"{fmt.tao(sum(t.tao for t in g.trades.values()))} τ) · {fmt.short(p.signer)}"))
            self._refresh_card(g, recompute=True)
            return
        print(f"{now_str()} {alerts.console_pending(p, info)}")
        g = alerts.PendingGroup(p.netuid, p, deadline=p.seen_after_block + PENDING_TTL_BLOCKS)
        self.pending_join[p.netuid] = g
        self.pending_open.append(g)
        asyncio.create_task(self._post_card(g, info))

    async def _post_card(self, g: alerts.PendingGroup, info) -> None:
        mention = self._mention(abs(g.pct))
        g.msg_id = await self.discord.send({
            "content": alerts.content([alerts.pending_headline(g, info)], mention),
            "embeds": [alerts.pending_embed(g, info)],
            "allowed_mentions": {"parse": ["everyone", "roles", "users"] if mention else []},
        })
        g.posted.set()

    def _refresh_card(self, g: alerts.PendingGroup, recompute: bool = False) -> None:
        """Debounced edit: at most one edit per card every ~1.5s, however many trades pile in."""
        g.recompute = g.recompute or recompute
        g.dirty = True
        if g.flush is None:
            g.flush = asyncio.create_task(self._flush_card(g))

    async def _flush_card(self, g: alerts.PendingGroup) -> None:
        try:
            await g.posted.wait()
            if not g.closed:
                await asyncio.sleep(1.5)  # fold a burst of trades into one edit
            g.dirty = False
            if g.recompute and self.mempool:
                g.recompute = False
                trades = list(g.trades.values())
                try:
                    after = await self.mempool.combined_after(trades, g.price_now)
                    if after:
                        g.price_after, g.priced_n = after, len(trades)
                except Exception as e:
                    log.debug("combined prediction failed: %s", e)
            if g.msg_id:
                info = self.meta.info(g.netuid)
                self.discord.edit_later(g.msg_id, {
                    "content": alerts.content([alerts.pending_headline(g, info)]),
                    "embeds": [alerts.pending_embed(g, info)],
                })
        finally:
            g.flush = None
        if g.dirty:  # something changed while we were editing
            self._refresh_card(g)

    # ── trend monitor ────────────────────────────────────────────────────

    async def _trend_block(self, block: Block) -> None:
        try:
            self.bars.update(block.number, block.prices)
            if self.trend.ready:
                events = self.trend.evaluate(block.number)
                if self.trend.signals:
                    self.trend.signals.detect(block.number)      # reversal signals: first, they're time-sensitive
                    if block.number % 5 == 0:
                        self.trend.signals.update(block.number)  # outcomes of open signals, about once a minute
                if events:
                    self.trend.on_events(block.number, events)
        except Exception:
            log.exception("trend evaluation failed")

    async def _trend_start(self) -> None:
        """Load stored bars, backfill the gaps (newest first) on dedicated history connections,
        then arm the trend monitor. Trends already under way are noted silently — no stale alerts."""
        t0 = time.perf_counter()
        await self.trend_discord.start()
        head = self.feed.last_number or self.det.last_block
        loaded = self.bars.load(head)
        hist = [Rpc(u) for u in dict.fromkeys([self.cfg.archive_endpoint, *self.cfg.mempool_endpoints])]
        for r in hist:
            r.start()
        try:
            await asyncio.gather(*(r.wait_up(10) for r in hist))
            got = await asyncio.wait_for(self.bars.backfill([r for r in hist if r.up], head), 900)
        except Exception as e:
            log.warning("trend backfill incomplete: %s", e)
            got = 0
        finally:
            for r in hist:
                await r.stop()
        missing = len(self.bars.missing_plan(head))
        self.trend.history = Rpc(self.cfg.trend_history_endpoint)
        self.trend.history.start()
        await self._trend_warmup(self.feed.last_number or head)
        if self.trend.signals:
            asyncio.create_task(self._signal_stats_loop())
        self.trend.ready = True
        ok = {tf.name: int(self.trend.fits[tf.name].ok.sum()) for tf in self.trend.tfs}
        active = sum(int((self.trend.state[tf.name] != 0).sum()) for tf in self.trend.tfs)
        print(fmt.dim(f"{now_str()} ── trend monitor armed in {time.perf_counter() - t0:.0f}s · bars: {loaded} stored + "
                      f"{got} backfilled ({missing} gaps left) · subnets with history: "
                      + ", ".join(f"{k} {v}" for k, v in ok.items()) + f" · {active} trends already running"))
        asyncio.create_task(self._trend_gap_loop())
        if self.cfg.trend_board_seconds > 0 and not self.cfg.dry_run:
            self.trend.board = TrendBoard(self.trend, ROOT / "data" / "trend_board.json", self.cfg.trend_board_seconds)
            asyncio.create_task(self.trend.board.run())
        if self.cfg.startup_message and not self.cfg.dry_run and not self._recent_start("last_start_trend"):
            tfs = " · ".join(f"**{tf.name}** ≥{tf.enter:g}%" + (f" (card at ≥{tf.card_enter:g}%)" if tf.split else "")
                             for tf in self.trend.tfs)
            await self.trend_discord.send({"embeds": [{
                "title": "🟢 Trend monitor is live",
                "description": (f"Watching the trend of **{max(ok.values())} subnets**, refitted on every block. "
                                f"One card per trend; longer timeframes confirming it update the same card."),
                "color": 0x5865F2,
                "fields": [
                    {"name": "Trend starts when the fitted move over the window reaches", "value": tfs, "inline": False},
                    {"name": "Already trending right now", "value": f"{active} subnet-timeframes (not re-announced)",
                     "inline": False},
                ],
            }], "allowed_mentions": {"parse": []}})

    async def _news_start(self) -> None:
        try:
            await self.news_discord.start()
            await self.news.start()
        except Exception as e:
            log.error("news monitor could not start: %s", e)
            return
        if self.cfg.news_x_accounts:
            asyncio.create_task(self.news.x_loop(self.cfg.news_x_accounts, self.cfg.news_x_poll_seconds))
        n = sum(1 for c in self.news.channels.values() if c.netuid is not None)
        teams = sum(1 for c in self.news.channels.values() if c.team_users or c.team_roles)
        print(fmt.dim(f"{now_str()} ── news monitor live in {self.news.guild_name} as "
                      f"{(self.news.client.user or {}).get('username')} · {n} subnet channels "
                      f"({teams} with a team detected) · {len(self.news.staff_roles)} staff roles · X: "
                      + ", ".join("@" + h for h in self.cfg.news_x_accounts)))
        if self.cfg.startup_message and not self.cfg.dry_run and not self._recent_start("last_start_news"):
            await self.news_discord.send({"embeds": [{
                "title": "🟢 News monitor is live",
                "description": (f"Watching **{n} subnet channels** in **{self.news.guild_name}** in real time. "
                                "Posts here: 📢 @everyone announcements · 🐦 X posts · 📰 team updates "
                                "(posts with links or real content by each subnet's team). Every card shows the "
                                "price at the post and the market reaction after 5m, 15m and 1h."),
                "color": 0x5865F2,
            }], "allowed_mentions": {"parse": []}})

    async def _trend_gap_loop(self) -> None:
        """History nodes drop requests under load; retry any missing bars every 15 minutes."""
        while True:
            await asyncio.sleep(900)
            head = self.feed.last_number
            if not head or not self.bars.missing_plan(head):
                continue
            hist = [Rpc(u) for u in dict.fromkeys([self.cfg.archive_endpoint, *self.cfg.mempool_endpoints])]
            for r in hist:
                r.start()
            try:
                await asyncio.gather(*(r.wait_up(10) for r in hist))
                got = await asyncio.wait_for(self.bars.backfill([r for r in hist if r.up], head), 600)
                log.info("trend history: filled %d gaps (%d left)", got, len(self.bars.missing_plan(head)))
            except Exception as e:
                log.debug("gap fill failed: %s", e)
            finally:
                for r in hist:
                    await r.stop()

    def _flow_all(self, blocks: int = 300) -> dict[int, float]:
        """Net TAO staked (+) / unstaked (−) per subnet over the last hour, from the per-block trade log."""
        head = self.feed.last_number
        net: dict[int, float] = {}
        for b in range(head - blocks + 1, head + 1):
            for t in self.trades.get(b, ()):
                net[t.netuid] = net.get(t.netuid, 0.0) + (t.tao if t.buy else -t.tao)
        return net

    def _flow(self, netuid: int, blocks: int = 300):
        """(net TAO, buys, sells) on a subnet over the last hour, from the per-block trade log."""
        head = self.feed.last_number
        buys = sells = 0
        net = 0.0
        for b in range(head - blocks + 1, head + 1):
            for t in self.trades.get(b, ()):
                if t.netuid == netuid:
                    net += t.tao if t.buy else -t.tao
                    buys += t.buy
                    sells += not t.buy
        return net, buys, sells

    async def _signal_stats_loop(self) -> None:
        """Keep the signals' track record current: re-run the backtest on stored history every 6 hours."""
        try:
            n = await self.trend.signals.seed((self.feed.last_number or self.det.last_block) // BUCKET)
            print(fmt.dim(f"{now_str()} ── reversal signals: {len(self.trend.signals.active)} in the last 24h "
                          f"({n} rebuilt from price history)"))
        except Exception:
            log.exception("signal seed failed")
        while True:
            try:
                st = await self.trend.signals.backtest((self.feed.last_number or self.det.last_block) // BUCKET)
                if st:
                    print(fmt.dim(f"{now_str()} ── reversal signals: backtest {st['days']}d → {st['n']} signals, "
                                  f"{st['win24']:.0f}% up after 24h, median {st['med24']:+.2f}% (mean {st['mean24']:+.2f}%)"))
            except Exception:
                log.exception("signal backtest failed")
            await asyncio.sleep(6 * 3600)

    async def _trend_warmup(self, head: int, days: float = 3.0) -> None:
        """Replay the last few days of 5-minute bars through the trend logic (silently), so a restart
        remembers trends that are still running — not only those strong enough to start fresh now.
        A trend stays on until its move halves, so state depends on history, not just on the present."""
        end = head // BUCKET
        first = end - int(days * 288)
        for i, b in enumerate(range(first, end)):
            self.trend.evaluate(b * BUCKET + BUCKET - 1, silent=True)
            if i % 12 == 0:
                await asyncio.sleep(0)  # let the price hot path run between steps
        self.trend.evaluate(head, silent=True)

    def _recent_start(self, name: str) -> bool:
        """True if we started < 10 min ago (pm2 restart / crash loop) — don't repeat 'live' messages."""
        stamp = ROOT / "data" / name
        try:
            recent = time.time() - float(stamp.read_text()) < 600
        except (OSError, ValueError):
            recent = False
        stamp.write_text(str(time.time()))
        return recent

    # ── background loops ─────────────────────────────────────────────────

    async def _backfill(self) -> None:
        """Load the last hour of prices so long windows work from minute one. Moves that
        already happened set up episodes silently — no stale alerts on restart."""
        n = self.cfg.backfill_blocks
        if n <= 0:
            return
        t0 = time.perf_counter()
        for _ in range(50):  # give the nodes a moment to connect
            if len(self.feed.healthy()) >= min(2, len(self.feed.clients)):
                break
            await asyncio.sleep(0.2)
        nodes = self.feed.healthy()
        try:
            head = int((await nodes[0].call("chain_getHeader", [], timeout=15))["number"], 16)
            ok = await asyncio.wait_for(fetch_history(nodes, head - n, head), 120)
            for b, prices in ok:
                self.det.update(b, prices, self.watched, silent=True)
            print(f"  {fmt.dim('history')}    {len(ok)} blocks backfilled (#{ok[0][0]}–#{ok[-1][0]}) "
                  f"in {time.perf_counter() - t0:.1f}s")
        except Exception as e:
            log.warning("backfill failed (%s) — long windows warm up live", e)

    async def _refresh_meta(self) -> None:
        """Reload subnet metadata. A slot that was re-registered is a different subnet: forget its history."""
        for n in await asyncio.wait_for(self.meta.refresh(), 60):
            log.info("SN%d was re-registered — resetting its history", n)
            self.det.reset(n)
            self.ref24.pop(n, None)
            if self.trend:
                self.bars.reset(n)
                self.trend.reset(n)

    async def _meta_loop(self) -> None:
        started = False
        while True:
            try:
                if self.subnets and not started and self.meta.subnets:
                    await self.subnets.start()   # compares with the view saved before the last shutdown
                    started = True
                    print(fmt.dim(f"{now_str()} ── subnet watch on · {len(self.subnets.known) - 1} subnets · slots "
                                  f"{self.subnets.facts.get('subnets', '?')}/{self.subnets.facts.get('limit', '?')} · "
                                  f"next to be pruned SN{self.subnets.facts.get('prune')} · registering costs "
                                  f"{fmt.tao(self.subnets.facts.get('cost', 0))} τ"))
            except Exception:
                log.exception("subnet watch start failed")
            await asyncio.sleep(60)
            try:
                await self._refresh_meta()
                if self.subnets and started:
                    await self.subnets.reconcile()
            except Exception as e:
                log.debug("meta refresh failed: %s", e)

    async def _usd_loop(self) -> None:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bittensor&vs_currencies=usd"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            while True:
                try:
                    async with s.get(url) as r:
                        self.tao_usd = float((await r.json())["bittensor"]["usd"])
                except Exception as e:
                    log.debug("TAO/USD fetch failed: %s", e)
                await asyncio.sleep(60)

    async def _ref24_loop(self) -> None:
        """Prices ~24h ago for the trend row. Needs a node with old state: the archive, else
        any other node that keeps it (onfinality does)."""
        while True:
            nodes = [self._archive] + [c for c in self.feed.healthy() if c is not self._archive]
            for node in nodes:
                try:
                    head = int((await node.call("chain_getHeader", [], timeout=15))["number"], 16)
                    h = await node.call("chain_getBlockHash", [head - DAY_BLOCKS], timeout=15)
                    raw = await node.call("state_call", [PRICE_ALL, "0x", h], timeout=15)
                    self.ref24 = decode_prices(bytes.fromhex(raw[2:]))
                    break
                except Exception as e:
                    log.debug("24h reference via %s failed: %s", node.name, e)
            await asyncio.sleep(600 if self.ref24 else 30)

    async def _health_loop(self) -> None:
        stale_warned = False
        while True:
            await asyncio.sleep(10)
            age = time.time() - self.feed.last_wall if self.feed.last_wall else 0
            if age > 60 and not stale_warned:
                log.error("no new block for %.0fs — nodes: %s", age,
                          ", ".join(f"{c.name}={'up' if c.up else c.last_error}" for c in self.feed.clients))
                stale_warned = True
            elif age < 30:
                stale_warned = False

    async def _summary_loop(self) -> None:
        """One dim line every 5 minutes: speed, node race, mempool coverage, alert counts."""
        while True:
            await asyncio.sleep(300)
            lat = sorted(self._lat)
            self._lat.clear()
            if not lat:
                continue
            wins = ", ".join(f"{k} {v}" for k, v in sorted(self.feed.wins.items(), key=lambda kv: -kv[1]))
            for k in self.feed.wins:
                self.feed.wins[k] = 0
            mp = (f" · mempool {self.mempool.decoded} tx decoded, {self.mempool.stake_calls} trades checked"
                  if self.mempool else "")
            print(fmt.dim(f"{now_str()} ── 5m: {len(lat)} blocks · detect median {lat[len(lat) // 2]:.0f} ms, "
                          f"p95 {lat[int(len(lat) * 0.95)]:.0f} ms · first header: {wins}{mp} · "
                          f"alerts {self.alert_count} · pending {self.pending_count}"
                          + (f" · trends {self.trend.alerts}" if self.trend else "")
                          + (f" · news {self.news.posted} (gateway msgs {self.news.stats.get('MESSAGE_CREATE', 0)}, "
                             f"passive {self.news.stats.get('PASSIVE_UPDATE_V1', 0) + self.news.stats.get('PASSIVE_UPDATE_V2', 0)}"
                             f", sweep {self.news.stats.get('sweep', 0)}, X checks {self.news.stats.get('x_polls', 0)} ok / {self.news.stats.get('x_fail', 0)} failed)"
                             if self.news else "")))

    async def _startup_message(self) -> None:
        if self._recent_start("last_start"):
            return
        wins = " · ".join(f"**≥{t:g}%** in {fmt.duration(w)}" for w, t in self.cfg.windows)
        up = [c.name for c in self.feed.clients if c.up]
        watched = sum(1 for n in self.det.history.get(self.det.last_block, {}) if self.watched(n))
        mempool = (f"on · alerts when a pending trade would move price ≥{self.cfg.mempool_min_pct:g}%"
                   if self.cfg.mempool else "off")
        await self.discord.send({"embeds": [{
            "title": "🟢 Subnet price monitor is live",
            "description": f"Watching **{watched} subnets** on **{len(up)} nodes** in parallel. "
                           f"Every block is checked the moment the first node sees it.",
            "color": 0x5865F2,
            "fields": [
                {"name": "Alert when price moves", "value": wins, "inline": False},
                {"name": "Early warning (mempool)", "value": mempool, "inline": False},
                {"name": "No spam", "value": f"Same subnet re-alerts only after a further "
                                             f"{self.cfg.realert_step_pct:g}% move, or on a reversal.", "inline": False},
            ],
            "footer": {"text": "Nodes: " + ", ".join(up)},
        }], "allowed_mentions": {"parse": []}})

    # ── console ──────────────────────────────────────────────────────────

    def _banner(self) -> None:
        c = self.cfg
        print()
        print(fmt.bold("  ⚡ SN MONITOR") + fmt.dim("  ·  real-time Bittensor subnet pump/dump alerts"))
        print(fmt.dim("  " + "─" * 66))
        print(f"  {fmt.dim('nodes')}      " + ", ".join(Rpc(u).name for u in c.endpoints) + f"  ({len(c.endpoints)} raced)")
        print(f"  {fmt.dim('alerts')}     " + "  ·  ".join(f"≥{t:g}% in {fmt.duration(w)}" for w, t in c.windows))
        print(f"  {fmt.dim('re-alert')}   every further {c.realert_step_pct:g}% · episodes end after "
              f"{fmt.duration(c.episode_ttl_blocks)} quiet")
        print(f"  {fmt.dim('mempool')}    " + (f"on · ≥{c.mempool_min_pct:g}% predicted impact · poll {c.mempool_poll_ms} ms"
                                           if c.mempool else "off"))
        if c.trend_webhook_url:
            tfs = parse_timeframes(c.trend_timeframes)
            print(f"  {fmt.dim('trend')}      " + "  ·  ".join(t.describe() for t in tfs) + "  → trend webhook")
        if c.news_webhook_url:
            print(f"  {fmt.dim('news')}       " + ("Bittensor Discord subnet channels → news webhook"
                                                  if c.news_discord_token else "waiting for NEWS_DISCORD_TOKEN in .env"))
        if c.dry_run:
            print(f"  {fmt.dim('mode')}       " + fmt.byellow("DRY RUN — nothing is posted to Discord"))

    def _status(self, block: Block, prev: dict[int, int] | None, signals: list[Signal]) -> None:
        self._lat.append(block.price_ms)
        up = sum(c.up for c in self.feed.clients)
        prop = f" · produced {block.propagation_s:.1f}s ago" if block.propagation_s is not None else ""
        movers = ""
        if prev:
            moves = [((p / prev[n] - 1) * 100, n) for n, p in block.prices.items()
                     if n and prev.get(n) and self.watched(n)]
            if moves:
                hi, lo = max(moves), min(moves)
                if hi[0] >= 0.01:
                    movers += "  " + fmt.green(f"▲ SN{hi[1]} {fmt.pct(hi[0])}")
                if lo[0] <= -0.01:
                    movers += "  " + fmt.red(f"▼ SN{lo[1]} {fmt.pct(lo[0])}")
        print(f"{fmt.dim(now_str())} {fmt.cyan(f'#{block.number}')}  ⚡ {block.price_ms:3.0f} ms "
              f"{fmt.dim(f'via {block.source}{prop} · nodes {up}/{len(self.feed.clients)}')}{movers}")
        for s in signals:
            print(f"{now_str()} {alerts.console_alert(s, self.meta.info(s.netuid))}")

    def _record(self, block: Block, signals: list[Signal], infos) -> None:
        self.alert_count += len(signals)
        try:
            with self._alerts_file.open("ab") as f:
                for s, inf in zip(signals, infos):
                    f.write(orjson.dumps({
                        "ts": block.wall_head, "block": block.number, "netuid": s.netuid, "name": inf.name,
                        "kind": s.kind, "dir": "pump" if s.direction > 0 else "dump",
                        "from": s.from_price, "to": s.to_price, "pct": round(s.pct, 4),
                        "window": s.window, "span": s.span_blocks, "detect_ms": round(block.price_ms, 1),
                    }) + b"\n")
        except OSError as e:
            log.debug("alert log write failed: %s", e)
