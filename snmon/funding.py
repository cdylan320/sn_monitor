"""Where a big buyer's TAO came from — to tell a team's hidden buyback from an ordinary buyer.

The user's rule (23 Oct): one wallet buys at least NEWS_BUYBACK_BIG_TAO (50 τ) of a subnet, and its money is
  · connected to the subnet's owner wallet — sent by the owner, or by a wallet the owner had funded, or
  · from an exchange / bridge — sent by a service wallet: an on-chain name that says so ("Tao Bridge", an exchange) or
    thousands of transactions (SERVICE_NONCE) — covering at least half of the buy
→ a buyback. SN63, 23 Oct: wallet A (74.77 τ) was paid by "Tao Bridge" (5HiveME…, 49,330 transactions); wallet B
(497.5 τ) got 500 τ from 5FqqXKb9… (68,146 transactions, 12,328 τ free) 4 minutes before it bought. Neither was
connected to the owner, and both bought only the same team's two subnets (SN48, SN63).

Every transfer of at least MIN_TRANSFER between people is kept for WINDOW_DAYS in data/transfers.db (from every block,
and filled back for the last day at the first start), so the check is a lookup, not a search of the chain.
"""
from __future__ import annotations

import logging
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

log = logging.getLogger("snmon.funding")

PALLET = "5EYCAe5"        # "modl…" pallet accounts (a subnet's pool and the like) — not people; their transfers are skipped
MIN_TRANSFER = 1.0        # τ: smaller transfers are not kept
WINDOW_DAYS = 7
SERVICE_NONCE = 5000      # a wallet with this many transactions is a service (an exchange's or a bridge's hot wallet)
SERVICE_NAME = re.compile(r"exchange|bridge|binance|kraken|mexc|gate\.?io|bitget|kucoin|okx|htx|huobi|coinbase|crypto\.com"
                          r"|bybit|upbit|bitfinex|coinex|lbank|bingx|bitmart", re.I)
COVER = 0.5               # exchange / bridge money must cover at least this share of the buy


class TransferLog:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path))
        self.db.execute("CREATE TABLE IF NOT EXISTS transfers (block INTEGER, ts REAL, src TEXT, dst TEXT, tao REAL)")
        self.db.execute("CREATE INDEX IF NOT EXISTS transfers_dst ON transfers (dst, ts)")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v INTEGER)")
        row = self.db.execute("SELECT v FROM state WHERE k = 'last_block'").fetchone()
        self.last_block = int(row[0]) if row else 0
        self._pruned = 0.0

    @staticmethod
    def keep(src: str, dst: str, tao: float) -> bool:
        return tao >= MIN_TRANSFER and not src.startswith(PALLET) and not dst.startswith(PALLET) and src != dst

    def add(self, block: int, ts: float, rows) -> None:
        """`rows` = (from, to, τ) of one block's Balances.Transfer events."""
        kept = [(block, ts, s, d, t) for s, d, t in rows if self.keep(s, d, t)]
        if kept:
            self.db.executemany("INSERT INTO transfers VALUES (?, ?, ?, ?, ?)", kept)
        if block > self.last_block:
            self.last_block = block
            self.db.execute("INSERT OR REPLACE INTO state VALUES ('last_block', ?)", (block,))
        self.db.commit()
        if time.time() - self._pruned > 3600:
            self._pruned = time.time()
            self.db.execute("DELETE FROM transfers WHERE ts < ?", (time.time() - WINDOW_DAYS * 86400,))
            self.db.commit()

    def into(self, dst: str, since: float) -> list[tuple[str, float, float]]:
        """(from, τ, when) of the transfers to `dst` since `since`."""
        return [(s, t, ts) for s, t, ts in
                self.db.execute("SELECT src, tao, ts FROM transfers WHERE dst = ? AND ts >= ?", (dst, since))]


class Funding:
    """`account(address)` → awaitable (nonce, on-chain name) — the chain's own record of that wallet."""

    def __init__(self, transfers: TransferLog, account) -> None:
        self.log = transfers
        self.account = account
        self._service: dict[str, tuple[float, str]] = {}

    async def service(self, addr: str) -> str:
        """"" for a person; else what the wallet is ("Tao Bridge", "a wallet with 68,146 transactions")."""
        hit = self._service.get(addr)
        if hit and time.time() - hit[0] < 86400:
            return hit[1]
        try:
            nonce, name = await self.account(addr)
        except Exception as e:
            log.debug("account %s: %s", addr, e)
            return ""
        label = (f"“{name}”" if name and SERVICE_NAME.search(name) else
                 f"{'“' + name + '”, ' if name else 'a wallet with '}{nonce:,} transactions" if nonce >= SERVICE_NONCE else "")
        self._service[addr] = (time.time(), label)
        return label

    async def check(self, buyer: str, owner: str | None, tao: float, now: float | None = None) -> tuple[str, str] | None:
        """("owner-funded" | "exchange", what shows it) when the buyer's money is the owner's or an exchange's, else None."""
        since = (now or time.time()) - WINDOW_DAYS * 86400
        ins = self.log.into(buyer, since)
        if not ins:
            return None
        if owner:
            got = sum(t for s, t, _ in ins if s == owner)
            if got:
                return "owner-funded", f"{got:,.2f} τ came straight from the owner wallet"
            for src in {s for s, _, _ in ins}:
                via = sum(t for s, t, _ in self.log.into(src, since) if s == owner)
                if via:
                    return "owner-funded", f"its TAO came from {src[:8]}…, a wallet the owner sent {via:,.2f} τ"
        by_src: dict[str, float] = defaultdict(float)
        for s, t, _ in ins:
            by_src[s] += t
        paid, names = 0.0, []
        for src, t in sorted(by_src.items(), key=lambda kv: -kv[1])[:6]:
            label = await self.service(src)
            if label:
                paid += t
                names.append(f"{t:,.2f} τ from {label} ({src[:8]}…)")
        if paid >= COVER * tao:
            return "exchange", "paid with " + ", ".join(names)
        return None
