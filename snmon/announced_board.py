"""The live board of the "only announced" channel (the user, 10 Nov): one message, edited every minute, kept below the
newest card — what the chain says WILL happen and has not yet:

    📋 Coming up on chain — live
    🏷️ Subnet registration: the price now, how fast it falls, when the next one is allowed, the last ones paid
    🔑 Owner coldkey swaps announced: when each can be executed (or "any block now")
    🆕 New subnets waiting for their start call · ⏳ registrations queued · ⚠️ dissolves scheduled
    [chart: the price to register over 10 days and the next 2]

Off the hot path: the chain reads run on the monitor's chain worker thread, the chart is drawn in a thread every few
minutes (~0.2 s), and the message is edited through the Discord client's queue.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

from . import regcost as rc_mod
from .config import BLOCK_SECONDS

log = logging.getLogger("snmon.board")

EVERY = 60            # seconds between edits
CHART_EVERY = 300     # … and between charts (the price falls ~1.8 τ an hour)
START_DAYS = 30       # a subnet registered this recently with no start call is "waiting for its start call"


def ts_of(head: int, block: int) -> int:
    return int(time.time() + (block - head) * BLOCK_SECONDS)


class AnnouncedBoard:
    def __init__(self, discord, watch, meta, regcost, head, path: Path) -> None:
        self.discord, self.watch, self.meta, self.rc, self.head = discord, watch, meta, regcost, head
        self.path = path
        self.msg_id: str | None = None
        self.move = False             # a new card was posted below: post the board again under it
        self.now = asyncio.Event()    # refresh at once (a card was posted or marked done)
        self._png: bytes | None = None
        self._png_at = 0.0
        self._png_key = None
        self._starts: dict[int, int | None] = {}
        try:
            self.msg_id = json.loads(path.read_text()).get("msg_id")
        except (OSError, ValueError):
            pass

    def poke(self, moved: bool = True) -> None:
        """A card went to the channel (moved: the board goes below it) or a card was marked done."""
        self.move = self.move or moved
        self.now.set()

    def _save(self) -> None:
        try:
            self.path.write_text(json.dumps({"msg_id": self.msg_id}))
        except OSError:
            pass

    async def run(self) -> None:
        while True:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("the announced board failed")
            try:
                await asyncio.wait_for(self.now.wait(), EVERY)
            except asyncio.TimeoutError:
                pass
            self.now.clear()
            await asyncio.sleep(2)    # several cards in a row: one move

    async def refresh(self) -> None:
        head = self.head()
        if not head:
            return
        facts = self.watch.facts or {}
        if facts.get("cost"):
            self.rc.calibrate(head, float(facts["cost"]))
        recent = [n for n, i in self.meta.subnets.items() if n and i.registered_at and head - i.registered_at
                  <= START_DAYS * 86400 / BLOCK_SECONDS]
        try:
            self._starts = await asyncio.wait_for(self.meta.first_emission(recent), 20) if recent else {}
        except Exception as e:
            log.debug("start calls unreadable: %s", e)
        embed = self.embed(head, facts)
        key = (len(self.rc.regs), self.rc.last().block if self.rc.last() else 0)
        files = None
        if self._png is None or key != self._png_key or time.time() - self._png_at >= CHART_EVERY:
            names = {n: i.name for n, i in self.meta.subnets.items()}
            self._png = await asyncio.to_thread(rc_mod.render, self.rc, head, time.time(), names=names)
            self._png_at, self._png_key = time.time(), key
            files = {"regcost.png": self._png}
        embed["image"] = {"url": "attachment://regcost.png"}
        payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
        if self.msg_id and self.move:
            await self.discord.delete(self.msg_id)
            self.msg_id = None
        self.move = False
        if not self.msg_id:
            self.msg_id = await self.discord.send(payload, files={"regcost.png": self._png})
            self._save()
        else:
            self.discord.edit_later(self.msg_id, payload, files=files)

    def embed(self, head: int, facts: dict) -> dict:
        lines = ["**🏷️ Subnet registration**"]
        cost = self.rc.cost_at(head)
        if cost is None and facts.get("cost"):
            cost = float(facts["cost"])
        last = self.rc.last()
        if cost is not None:
            rate = (last.tao / self.rc.interval_at(head) * 3600 / BLOCK_SECONDS) if last else 0
            floor = self.rc.floor_block()
            fall = (f" — falls **{rate:,.2f} τ an hour**" + (f", to {self.rc.min_lock:g} τ <t:{ts_of(head, floor)}:R> if no "
                    "one registers" if floor and floor > head else "")) if rate and cost > self.rc.min_lock else ""
            lines.append(f"Price now: **{cost:,.2f} τ**{fall}")
        nxt = self.rc.next_allowed()
        if nxt and nxt > head:
            lines.append(f"Next registration allowed <t:{ts_of(head, nxt)}:R> (one every "
                         f"{self.rc.rate_limit * BLOCK_SECONDS / 86400:g} days)")
        elif nxt:
            lines.append("Next registration: **allowed now**")
        regs = self.rc.regs[-3:][::-1]
        if regs:
            def tag(r):
                name = (self.meta.subnets.get(r.netuid).name if r.netuid in self.meta.subnets else "") or r.name
                return f"SN{r.netuid} {name}".strip() if r.netuid is not None else (name or "a subnet")
            lines.append("Last paid: " + " · ".join(f"{tag(r)} **{r.tao:,.2f} τ** <t:{int(r.ts)}:R>" for r in regs if r.ts))
        if facts.get("limit"):
            prune = facts.get("prune")
            p_name = self.meta.subnets[prune].name if prune in self.meta.subnets else ""
            lines.append(f"Slots {facts.get('subnets', '?')}/{facts['limit']}"
                         + (f" · next to be removed: SN{prune} {p_name}".rstrip() if prune is not None else ""))

        swaps = sorted(self.watch.swaps.items(), key=lambda kv: kv[1].get("exec", 0))
        lines += ["", f"**🔑 Owner coldkey swaps announced** ({len(swaps)})"]
        for owner, sw in swaps:
            ex = int(sw.get("exec", 0))
            nets = sw.get("nets") or []
            label = ", ".join(f"SN{n} {self.meta.subnets[n].name}".strip() if n in self.meta.subnets else f"SN{n}"
                              for n in nets) or owner[:6]
            when = ("⏰ **can be executed now**" + (f" (since <t:{ts_of(head, ex)}:R>)" if head - ex > 25 else "")
                    if ex <= head else f"can be executed <t:{ts_of(head, ex)}:R> · <t:{ts_of(head, ex)}:f>")
            lines.append(f"• **{label}** — {when}")
        if not swaps:
            lines.append("none")

        waiting = sorted((n for n, fe in self._starts.items() if fe is None and n in self.meta.subnets),
                         key=lambda n: -self.meta.subnets[n].registered_at)
        lines += ["", f"**🆕 Waiting for the start call** ({len(waiting)})"]
        for n in waiting:
            i = self.meta.subnets[n]
            lines.append(f"• **SN{n} {i.name}".rstrip() + f"** — registered <t:{ts_of(head, i.registered_at)}:R>")
        if not waiting:
            lines.append("none")
        queued = [k for k in self.watch.announced if k.startswith("queued:")]
        dissolve = [k for k in self.watch.announced if k.startswith("dissolve:")]
        if queued:
            lines += ["", f"**⏳ Registrations queued** ({len(queued)}) — they start in a few minutes"]
        if dissolve:
            lines += ["", "**⚠️ Dissolves scheduled**: " + ", ".join(f"SN{k.split(':')[1]}" for k in dissolve)]
        return {"title": "📋 Coming up on chain — live", "description": "\n".join(lines)[:4000], "color": 0x8B5CF6,
                "footer": {"text": f"Updated every minute · block #{head} · cards above: ✅ done / ❌ not happening"}}
