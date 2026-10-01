"""Mempool early-warning: see big stakes/unstakes *before* they land in a block.

Polls `author_pendingExtrinsics` round-robin over dedicated connections (never the hot-path
ones: public nodes throttle this call per connection), so the combined pool is checked every
MEMPOOL_POLL_MS. Each new stake-type call is decoded and its price impact is computed exactly
with the runtime's swap simulator:

    impact = marginal_price(after trade) / marginal_price(now) - 1

using sim(ε), sim(X), sim(X+ε). Fees cancel out in the ratio, and no pool math is assumed.

Blind spot: transactions sent through MEV Shield (`submit_encrypted`) are encrypted until
they execute, so only plaintext trades can be seen early. Those still alert on landing.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from typing import Callable, Iterator

from .alerts import Pending
from .chain import RAO, Feed, blake2_256, simulate
from .meta import Meta
from .rpc import Rpc, RpcError

log = logging.getLogger("snmon.mempool")

BUY_CALLS = {"add_stake", "add_stake_limit", "add_stake_burn"}
SELL_CALLS = {"remove_stake", "remove_stake_limit", "remove_stake_full_limit", "unstake_all", "unstake_all_alpha"}
CROSS_CALLS = {"swap_stake", "swap_stake_limit", "move_stake", "move_stake_limit", "transfer_stake", "swap_basket"}


def _acct(v) -> str | None:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for k in ("Id", "id", "Address32"):
            if isinstance(v.get(k), str):
                return v[k]
    return None


def _walk(call: dict, signer: str | None) -> Iterator[tuple[str, dict, str | None]]:
    """Yield (function, args, effective_signer) for every SubtensorModule call, unwrapping proxies/batches."""
    if not isinstance(call, dict):
        return
    mod, fn = call.get("call_module"), call.get("call_function")
    args = {a["name"]: a["value"] for a in call.get("call_args") or []}
    if mod == "Proxy" and fn in ("proxy", "proxy_announced"):
        yield from _walk(args.get("call"), _acct(args.get("real")) or signer)
    elif mod == "Utility" and fn in ("batch", "batch_all", "force_batch"):
        for c in args.get("calls") or []:
            yield from _walk(c, signer)
    elif mod == "Utility" and fn in ("as_derivative", "with_weight", "dispatch_as"):
        yield from _walk(args.get("call"), signer)
    elif mod == "Multisig" and fn == "as_multi":
        yield from _walk(args.get("call"), None)
    elif mod == "SubtensorModule":
        yield fn, args, signer


class Mempool:
    def __init__(self, feed: Feed, meta: Meta, endpoints: list[str], min_pct: float, poll_ms: int,
                 price_now: Callable[[int], int | None], watched: Callable[[int], bool],
                 on_pending: Callable[[Pending], None]) -> None:
        self.feed, self.meta = feed, meta
        self.rpcs = [Rpc(u) for u in endpoints]
        self._cool: dict[str, float] = {}
        self.min_pct = min_pct
        self.poll_s = poll_ms / 1000
        self.price_now = price_now
        self.watched = watched
        self.on_pending = on_pending
        self._seen: OrderedDict[str, None] = OrderedDict()
        self.decoded = 0
        self.errors = 0
        self.stake_calls = 0
        self._runtime_reset_at = 0.0

    def _nodes(self) -> list[Rpc]:
        now = time.monotonic()
        return [r for r in self.rpcs if r.up and self._cool.get(r.name, 0) <= now]

    async def _call(self, method: str, params: list):
        for r in sorted(self._nodes(), key=lambda r: r.rtt_ms or 1e9):
            try:
                return await r.call(method, params, timeout=5)
            except RpcError:
                continue
        return await self.feed.call(method, params)

    async def _sim(self, buy: bool, netuid: int, amount: int) -> int:
        return await simulate(self._call, buy, netuid, amount)

    async def run(self) -> None:
        for r in self.rpcs:
            r.start()
        i = 0
        while True:
            nodes = self._nodes()
            if not nodes:
                await asyncio.sleep(1)
                continue
            node = nodes[i % len(nodes)]
            i += 1
            try:
                exts = await node.call("author_pendingExtrinsics", [], timeout=3)
            except Exception as e:
                if "limit" in str(e):
                    self._cool[node.name] = time.monotonic() + 15
                    log.warning("mempool: %s is throttling — resting it for 15s", node.name)
                else:
                    log.debug("pending poll failed on %s: %s", node.name, e)
                exts = []
            seen = self._seen
            for hx in exts or ():
                if hx in seen:
                    continue
                seen[hx] = None
                if len(seen) > 20000:
                    for _ in range(5000):
                        seen.popitem(last=False)
                asyncio.create_task(self._analyze(hx))
            await asyncio.sleep(self.poll_s / len(nodes))

    async def _analyze(self, hx: str) -> None:
        t_seen = time.time()
        try:
            ext = await self.meta.decode_extrinsic(hx)
            self.decoded += 1
        except Exception as e:
            self.errors += 1
            # Maybe a runtime upgrade: reload metadata, but at most every 10 minutes.
            if time.monotonic() - self._runtime_reset_at > 600:
                self._runtime_reset_at = time.monotonic()
                self.meta.runtime_changed()
            log.debug("decode failed: %s", e)
            return
        signer = _acct(ext.get("address"))
        tx_hash = blake2_256(bytes.fromhex(hx[2:]))
        for fn, args, who in _walk(ext.get("call"), signer):
            if fn in BUY_CALLS or fn in SELL_CALLS or fn in CROSS_CALLS:
                self.stake_calls += 1
            try:
                for leg in await self._legs(fn, args, who):
                    await self._evaluate(tx_hash, fn, who, t_seen, *leg)
            except Exception:
                log.debug("pending analysis failed for %s", fn, exc_info=True)

    async def _legs(self, fn: str, a: dict, who: str | None) -> list[tuple]:
        """→ [(netuid, buy, amount_rao, limit_price|None, allow_partial)]"""
        if fn in BUY_CALLS:
            amount = int(a.get("amount_staked") or a.get("amount") or 0)
            limit = a.get("limit_price", a.get("limit"))
            return [(int(a["netuid"]), True, amount, int(limit) if limit else None, bool(a.get("allow_partial", True)))]
        if fn in ("remove_stake", "remove_stake_limit"):
            limit = a.get("limit_price")
            return [(int(a["netuid"]), False, int(a["amount_unstaked"]), int(limit) if limit else None,
                     bool(a.get("allow_partial", True)))]
        if fn == "remove_stake_full_limit" and who:
            amount = await self.meta.stake_of(who, _acct(a["hotkey"]), int(a["netuid"]))
            limit = a.get("limit_price")
            return [(int(a["netuid"]), False, amount, int(limit) if limit else None, False)]
        if fn in ("unstake_all", "unstake_all_alpha") and who:
            stakes = await self.meta.stakes_on_hotkey(who, _acct(a["hotkey"]))
            return [(n, False, amt, None, True) for n, amt in stakes.items() if n != 0]
        if fn in CROSS_CALLS:
            o, d = int(a["origin_netuid"]), int(a["destination_netuid"])
            if o == d:
                return []
            alpha = int(a.get("alpha_amount") or a.get("amount") or 0)
            legs = [(o, False, alpha, None, True)] if o != 0 else []
            tao_out = alpha if o == 0 else await self._sim(False, o, alpha)
            if d != 0 and tao_out:
                legs.append((d, True, tao_out, None, True))
            return legs
        return []

    async def _ratio(self, netuid: int, buy: bool, amount: int) -> tuple[float, int] | None:
        """(spot-after / spot-now, amount out) for one swap — exact, from the runtime simulator."""
        eps = max(amount // 1000, 1_000_000)
        s_eps, s_x, s_x2 = await asyncio.gather(
            self._sim(buy, netuid, eps), self._sim(buy, netuid, amount), self._sim(buy, netuid, amount + eps)
        )
        if s_eps <= 0 or s_x2 <= s_x:
            return None
        if buy:   # TAO in → alpha out; marginal price = dTAO / dAlpha
            m0, mx = eps / s_eps, eps / (s_x2 - s_x)
        else:     # alpha in → TAO out
            m0, mx = s_eps / eps, (s_x2 - s_x) / eps
        return mx / m0, s_x

    async def combined_after(self, trades: list[Pending], p0: int) -> int | None:
        """Price if every one of these pending trades lands: net flow as one swap, then capped
        by the orders' limit prices (a limit order can't push price past its limit)."""
        buy_tao = sum(round(t.tao * RAO) for t in trades if t.buy)
        sell_tao = sum(round(t.alpha * p0) for t in trades if not t.buy and t.alpha)
        net = buy_tao - sell_tao
        if net == 0:
            return p0
        r = await self._ratio(trades[0].netuid, net > 0, net if net > 0 else net * RAO // -p0)
        if r is None:
            return None
        after = int(p0 * r[0])
        side = [t for t in trades if t.buy == (net > 0)]
        if side and all(t.limit for t in side):
            after = min(after, max(t.limit for t in side)) if net > 0 else max(after, min(t.limit for t in side))
        return after

    async def _evaluate(self, tx_hash: str, fn: str, who: str | None, t_seen: float,
                        netuid: int, buy: bool, amount: int, limit: int | None, allow_partial: bool) -> None:
        if amount <= 0 or not self.watched(netuid):
            return
        p0 = self.price_now(netuid)
        if not p0:
            return
        r = await self._ratio(netuid, buy, amount)
        if r is None:
            return
        ratio, out = r
        after = int(p0 * ratio)
        capped = False
        if limit:
            if buy and after > limit:
                if not allow_partial:
                    return  # would revert
                after, capped = limit, True
            elif not buy and after < limit:
                if not allow_partial:
                    return
                after, capped = limit, True
        impact = (after / p0 - 1) * 100
        if abs(impact) < self.min_pct:
            return
        feed = self.feed
        last_wall = feed.last_wall
        self.on_pending(Pending(
            tx_hash=tx_hash,
            netuid=netuid,
            buy=buy,
            tao=(amount if buy else out) / RAO,
            alpha=None if buy else amount / RAO,
            signer=who or "unknown",
            price_now=p0,
            price_after=after,
            seen_after_block=feed.last_number,
            seen_ms_after_block=((t_seen - last_wall) * 1000) if last_wall else 0.0,
            call=fn,
            limit=limit,
            limit_capped=capped,
        ))
