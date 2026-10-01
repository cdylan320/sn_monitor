"""What an alert looks like — in Discord and in the console.

Discord layout (one embed per subnet, up to 10 per message):

  content  → one line per alert; this is what phone push notifications show
             "🚀 SN64 Chutes PUMP +20.00% · 0.004300 → 0.005160 τ · 36s"
  embed    → author: subnet (logo, link to taostats)
             title:  direction + % + how fast
             diff block: old → new price, green for pumps / red for dumps
             fields: price ($), move window, liquidity, 1m/5m/1h/24h trend, trades (filled in by an edit)
             footer: block number + measured detection latency
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import fmt
from .chain import RAO, Block
from .detector import Signal
from .meta import SubnetInfo, Trade

GREEN, RED, AMBER = 0x22C55E, 0xEF4444, 0xF5B301
BRIGHT_GREEN, BRIGHT_RED = 0x00FF85, 0xFF2D55
BIG_MOVE = 10.0


# Blank line above every message: Discord stacks a webhook's messages tightly, so without it
# consecutive cards run into each other. A zero-width space survives Discord's whitespace trim.
SPACER = "\u200b\n"


def subnet_url(netuid: int) -> str:
    return f"https://taomarketcap.com/subnets/{netuid}"


def account_url(ss58: str) -> str:
    return f"https://ss-ims.space/account/{ss58}"


def content(lines: list[str], mention: str = "") -> str:
    """Message text above the embeds (what phone notifications show), with the spacer on top."""
    return SPACER + (mention + "\n" if mention else "") + "\n".join(lines)


@dataclass
class Context:
    trend: dict[str, float | None] = field(default_factory=dict)  # "1m" → % change
    tao_usd: float | None = None


def _emoji(direction: int, pct: float) -> str:
    big = abs(pct) >= BIG_MOVE
    if direction > 0:
        return "🚀" if big else "📈"
    return "💥" if big else "📉"


def _word(direction: int) -> str:
    return "PUMP" if direction > 0 else "DUMP"


def _author(info: SubnetInfo) -> dict:
    name = f"SN{info.netuid}"
    if info.name:
        name += f" · {info.name}"
    if info.symbol:
        name += f"  {info.symbol}"
    a = {"name": name, "url": subnet_url(info.netuid)}
    if info.logo:
        a["icon_url"] = info.logo
    return a


def _trend(ctx: Context) -> str:
    parts = []
    for label, v in ctx.trend.items():
        if v is None:
            parts.append(f"{label} —")
        else:
            arrow = "▲" if v > 0 else ("▼" if v < 0 else "•")
            parts.append(f"{label} {arrow}`{fmt.pct(v, signed=False)}`")
    return "  ·  ".join(parts)


def _ts(unix: float) -> str:
    return datetime.fromtimestamp(unix, timezone.utc).isoformat()


# ── confirmed (on-chain) alerts ──────────────────────────────────────────

def headline(sig: Signal, info: SubnetInfo) -> str:
    """Single line used for Discord content (push notification) — 'old → new  PUMP x%'.
    A "continues" alert leads with the whole move since it started; the latest leg is secondary."""
    name = f"SN{info.netuid} {info.name}".strip()
    if sig.kind == "more":
        a, b, tot = fmt.move(sig.anchor_price, sig.to_price)
        _, _, leg = fmt.move(sig.from_price, sig.to_price)
        return (f"{_emoji(sig.direction, tot)} **{name}** {_word(sig.direction)} continues **{fmt.pct(tot)}** · "
                f"{a} → {b} τ · {fmt.duration(sig.to_block - sig.anchor_block)} "
                f"(last leg {fmt.pct(leg)} in {fmt.duration(sig.span_blocks)})")
    tag = _word(sig.direction) + (" (reversal)" if sig.kind == "reversal" else "")
    a, b, pc = fmt.move(sig.from_price, sig.to_price)
    return f"{_emoji(sig.direction, pc)} **{name}** {tag} **{fmt.pct(pc)}** · {a} → {b} τ · {fmt.duration(sig.span_blocks)}"


def confirmed_embed(sig: Signal, info: SubnetInfo, block: Block, ctx: Context) -> dict:
    up = sig.direction > 0
    word = _word(sig.direction)
    when = fmt.duration(sig.span_blocks)

    if sig.kind == "more":
        # the headline number is the whole move; the last ≥3% leg is why this alert fired
        a, b, pc = fmt.move(sig.anchor_price, sig.to_price)
        leg_from, _, leg = fmt.move(sig.from_price, sig.to_price)
        started = fmt.duration(sig.to_block - sig.anchor_block)
        title = f"{_emoji(sig.direction, pc)} {word} CONTINUES  {fmt.pct(pc)}  ·  in {started}"
        move_value = (f"{fmt.pct(pc)} since it started ({started})\n"
                      f"last leg {fmt.pct(leg)} from {leg_from} τ ({when})")
    else:
        a, b, pc = fmt.move(sig.from_price, sig.to_price)  # % exactly as the shown prices give it
        tag = "  (reversal)" if sig.kind == "reversal" else ""
        title = f"{_emoji(sig.direction, pc)} {word}  {fmt.pct(pc)}  ·  in {when}{tag}"
        ref = "low" if up else "high"
        move_value = (f"{fmt.pct(pc)} in {fmt.window_label(sig.span_blocks)}\n"
                      f"from {ref} at #{sig.from_block}")
    big = abs(pc) >= BIG_MOVE
    color = (BRIGHT_GREEN if big else GREEN) if up else (BRIGHT_RED if big else RED)

    sign = "+" if up else "-"
    diff = f"```diff\n{sign} {a} τ  →  {b} τ   ({fmt.pct(pc)})\n```"

    price_value = f"**{fmt.price(sig.to_price)} τ**"
    if ctx.tao_usd:
        price_value += f"\n{fmt.usd(sig.to_price / RAO * ctx.tao_usd)}"
    liq_value = f"**{fmt.tao_compact(info.pool_tao)}**" if info.pool_tao else "—"

    fields = [
        {"name": "Price", "value": price_value, "inline": True},
        {"name": "Move", "value": move_value, "inline": True},
        {"name": "Pool liquidity", "value": liq_value, "inline": True},
    ]
    if ctx.trend:
        fields.append({"name": "Trend", "value": _trend(ctx), "inline": False})

    footer = f"Block #{block.number} · detected {block.price_ms:.0f} ms after the block reached us"
    if block.propagation_s is not None and 0 <= block.propagation_s < 60:
        footer += f" ({block.propagation_s:.1f}s after it was produced)"

    return {
        "author": _author(info),
        "title": title,
        "url": subnet_url(info.netuid),
        "description": diff,
        "color": color,
        "fields": fields,
        "footer": {"text": footer},
        "timestamp": _ts(block.wall_head),
    }


def trades_field(trades: list[Trade], sig: Signal) -> dict:
    """'What moved it' — filled in by an edit right after the alert goes out."""
    if not trades:
        return {"name": "Trades", "value": "No stake/unstake on this subnet in these blocks.", "inline": False}
    buys = [t for t in trades if t.buy]
    sells = [t for t in trades if not t.buy]
    net = sum(t.tao for t in buys) - sum(t.tao for t in sells)
    lines = []
    for t in sorted(trades, key=lambda t: -t.tao)[:5]:
        icon, verb = ("🟢", "Buy") if t.buy else ("🔴", "Sell")
        lines.append(f"{icon} **{verb} {fmt.tao(t.tao)} τ** · [{fmt.short(t.coldkey)}]({account_url(t.coldkey)}) · #{t.block}")
    if len(trades) > 5:
        lines.append(f"… +{len(trades) - 5} more")
    net_s = f"+{fmt.tao(net)}" if net >= 0 else f"-{fmt.tao(-net)}"
    lines.append(f"Net **{net_s} τ** · {len(buys)} buy{'s' * (len(buys) != 1)} · {len(sells)} sell{'s' * (len(sells) != 1)}")
    value = "\n".join(lines)
    return {"name": "Trades", "value": value[:1024], "inline": False}


# ── pending (mempool) alerts ─────────────────────────────────────────────

@dataclass
class Pending:
    tx_hash: str
    netuid: int
    buy: bool
    tao: float                 # TAO in (buy) or ≈TAO out (sell)
    alpha: float | None        # alpha sold (sell only)
    signer: str
    price_now: int
    price_after: int
    seen_after_block: int
    seen_ms_after_block: float
    call: str
    limit: int | None = None   # limit price (rao per alpha), if a limit order
    limit_capped: bool = False

    @property
    def pct(self) -> float:
        return (self.price_after / self.price_now - 1) * 100


@dataclass
class PendingGroup:
    """All pending trades on one subnet seen within ~2 blocks → one Discord card, edited in place.
    Bots often fire the same order many times (different nonces); this keeps that to one card."""
    netuid: int
    first: Pending
    trades: dict[str, Pending] = field(default_factory=dict)  # tx hash → trade
    price_after: int = 0                                       # if every trade lands
    deadline: int = 0
    landed: dict[str, int] = field(default_factory=dict)      # tx hash → block
    moves: dict[int, float] = field(default_factory=dict)     # block → actual % move in it
    msg_id: str | None = None
    closed: bool = False
    final_status: str | None = None
    # bookkeeping for the debounced Discord edits
    posted: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    flush: asyncio.Task | None = field(default=None, repr=False)
    recompute: bool = False
    dirty: bool = False
    priced_n: int = 1          # how many trades price_after accounts for

    def __post_init__(self) -> None:
        self.trades.setdefault(self.first.tx_hash, self.first)
        self.price_after = self.price_after or self.first.price_after

    @property
    def price_now(self) -> int:
        return self.first.price_now

    @property
    def pct(self) -> float:
        return (self.price_after / self.price_now - 1) * 100

    @property
    def n(self) -> int:
        return len(self.trades)

    @property
    def when(self) -> str:
        return "predicted" if self.priced_n == 1 else f"if all {self.priced_n} land"

    def side(self) -> str:
        buys = any(t.buy for t in self.trades.values())
        sells = any(not t.buy for t in self.trades.values())
        word = "BUY" if buys and not sells else "SELL" if sells and not buys else "TRADE"
        return word + ("S" if self.n > 1 else "")

    def _landed(self) -> str:
        moved = " · ".join(f"#{b} {fmt.pct(self.moves[b])}" for b in sorted(self.moves)[:3])
        return f"✅ {len(self.landed)} landed" + (f" (actual move {moved})" if moved else "")

    def status(self) -> str:
        if self.final_status:
            return self.final_status
        if not self.landed:
            return "🕓 In mempool — not in a block yet"
        waiting = self.n - len(self.landed)
        return self._landed() + (f" · 🕓 {waiting} still pending" if waiting else "")

    def finalize(self, ttl_blocks: int) -> None:
        """No more updates after this: everything landed, or the rest timed out."""
        self.closed = True
        waiting = self.n - len(self.landed)
        if not self.landed:
            self.final_status = f"⌛ Not included within {ttl_blocks} blocks (dropped, replaced or failed)"
        elif waiting:
            self.final_status = self._landed() + f" · ⌛ {waiting} never included"


def pending_headline(g: PendingGroup, info: SubnetInfo) -> str:
    name = f"SN{info.netuid} {info.name}".strip()
    ts = list(g.trades.values())
    size = f"{fmt.tao(sum(t.tao for t in ts))} τ"
    count = f" ×{g.n}" if g.n > 1 else ""
    a, b, pc = fmt.move(g.price_now, g.price_after)
    return f"⏳ **{name}** INCOMING {g.side()}{count} {size} → **{fmt.pct(pc)}** {g.when} · {a} → {b} τ"


def pending_embed(g: PendingGroup, info: SubnetInfo) -> dict:
    ts = list(g.trades.values())
    up = g.price_after >= g.price_now
    a, b, pc = fmt.move(g.price_now, g.price_after)
    diff = f"```diff\n{'+' if up else '-'} {a} τ  →  {b} τ   ({fmt.pct(pc)} {g.when})\n```"

    buys = [t for t in ts if t.buy]
    sells = [t for t in ts if not t.buy]
    lines = []
    if g.n > 1:
        lines.append(f"**{g.n} txs**")
    if buys:
        lines.append(f"Stake **{fmt.tao(sum(t.tao for t in buys))} τ**")
    if sells:
        lines.append(f"Unstake **{fmt.tao(sum(t.alpha or 0 for t in sells))} α** (≈ {fmt.tao(sum(t.tao for t in sells))} τ)")
    if g.n > 1:
        lines.append(f"largest {fmt.tao(max(t.tao for t in ts))} τ")
    limits = [t.limit for t in ts if t.limit]
    if limits:
        cap = max(limits) if up else min(limits)
        lines.append(f"limit {fmt.price(cap)} τ ({fmt.pct((cap / g.price_now - 1) * 100)})")

    wallets: dict[str, int] = {}
    for t in ts:
        wallets[t.signer] = wallets.get(t.signer, 0) + 1
    top = sorted(wallets.items(), key=lambda kv: -kv[1])
    wallet_lines = [f"[{fmt.short(w)}]({account_url(w)})" + (f" ×{c}" if c > 1 else "") for w, c in top[:4]]
    if len(top) > 4:
        wallet_lines.append(f"+{len(top) - 4} more")

    calls: dict[str, int] = {}
    for t in ts:
        calls[t.call] = calls.get(t.call, 0) + 1
    call_lines = [f"`{c}`" + (f" ×{k}" if k > 1 else "") for c, k in calls.items()]

    title = f"⏳ INCOMING {g.side()}" + (f" ×{g.n}" if g.n > 1 else "") + f"  ·  {fmt.pct(pc)} {g.when}"
    f0 = g.first
    return {
        "author": _author(info),
        "title": title,
        "url": subnet_url(info.netuid),
        "description": diff,
        "color": AMBER,
        "fields": [
            {"name": "Trade" if g.n == 1 else "Trades", "value": "\n".join(lines), "inline": True},
            {"name": "Wallet" if len(top) == 1 else "Wallets", "value": "\n".join(wallet_lines), "inline": True},
            {"name": "Call", "value": "\n".join(call_lines)[:1024], "inline": True},
            {"name": "Status", "value": g.status(), "inline": False},
        ],
        "footer": {"text": f"First seen in mempool {f0.seen_ms_after_block / 1000:.1f}s after block #{f0.seen_after_block} · "
                           f"impact from the chain's own swap simulator"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── console ──────────────────────────────────────────────────────────────

def console_alert(sig: Signal, info: SubnetInfo) -> str:
    up = sig.direction > 0
    color = fmt.bgreen if up else fmt.bred
    arrow = "▲" if up else "▼"
    tag = _word(sig.direction) + {"more": "+", "reversal": "↺"}.get(sig.kind, "")
    name = f"SN{info.netuid:<3} {info.name[:16]:<16}"
    if sig.kind == "more":  # whole move first, last leg after
        a, b, pc = fmt.move(sig.anchor_price, sig.to_price)
        span = fmt.duration(sig.to_block - sig.anchor_block)
        extra = f"  (last leg {fmt.pct(fmt.move(sig.from_price, sig.to_price)[2])})"
    else:
        a, b, pc = fmt.move(sig.from_price, sig.to_price)
        span, extra = fmt.duration(sig.span_blocks), ""
    to = f"{b} τ"
    return color(f"{arrow} {tag:<6} {name} {a:>10} → {to:<12} {fmt.pct(pc):>9}  in {span}{extra}")


def console_pending(p: Pending, info: SubnetInfo) -> str:
    verb = "BUY " if p.buy else "SELL"
    name = f"SN{info.netuid:<3} {info.name[:16]:<16}"
    to = f"{fmt.price(p.price_after)} τ"
    return fmt.byellow(f"⏳ {verb}   {name} {fmt.price(p.price_now):>10} → {to:<12} "
                       f"{fmt.pct(p.pct):>9}  predicted · {fmt.tao(p.tao)} τ · {fmt.short(p.signer)}")
