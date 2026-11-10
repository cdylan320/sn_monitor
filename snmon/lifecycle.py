"""Subnet lifecycle monitor — slots, owners, identities.

Real time from chain events (decoded every block, ~1s after the block):

  🆕 NetworkAdded                 a new subnet took a slot (and which subnet it replaced)
  💀 NetworkRemoved               a subnet was deregistered
  ⏳ NetworkRegistrationQueued    someone queued a new subnet registration (+ who would be pruned)
  ⚠️ DissolveNetworkScheduled     an owner scheduled dissolving their subnet
  🔑 ColdkeySwap*                 a subnet OWNER's coldkey swap: announced / swapped / disputed / reset
  ⏰ swap can be executed now     the wait after an announced swap is over (announcement block + ColdkeySwapAnnouncementDelay)
  🔑 swap in the mempool          the owner's swap transaction seen pending, ~1 block before it lands (see mempool.py)
  👑 SubnetOwnerChanged           ownership reassigned · 🗝️ SubnetOwnerHotkeySet
  ✏️ SubnetIdentitySet / 🔤 SymbolUpdated   name, links, logo or symbol changed (shown as a diff)
  🚀 FirstEmissionBlockNumberSet  emissions start · 📜 lease created / terminated · 🧮 slot limit changed

Safety net: after every metadata refresh (each minute) the subnet list is compared with what was
last reported, so a change is still announced if its event was missed (node hiccup, restart — the
last view is kept on disk). Also announced: when a different subnet becomes first in line to be
deregistered (only once it has held that position for 10 minutes).
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from . import alerts, fmt
from .config import BLOCK_SECONDS
from .meta import SubnetInfo, _text

log = logging.getLogger("snmon.lifecycle")

GREEN, RED, AMBER, VIOLET, BLUE, GREY = 0x22C55E, 0xEF4444, 0xF59E0B, 0x8B5CF6, 0x3B82F6, 0x6B7280
REFRESH_ON = {"NetworkAdded", "NetworkRemoved", "SubnetIdentitySet", "SubnetIdentityRemoved", "SymbolUpdated",
              "SubnetOwnerChanged", "SubnetOwnerHotkeySet", "ColdkeySwapped"}
IDENTITY_LABELS = {"name": "Name", "url": "Website", "github": "GitHub", "description": "Description",
                   "logo": "Logo", "discord": "Discord", "contact": "Contact"}
PRUNE_HOLD = 600  # seconds a new prune candidate must hold before it is announced
CATCH_UP_BLOCKS = 600   # after a restart, the events of up to this many missed blocks (2 h) are read
# announced on chain, not done yet: a registration queued, a new subnet (its start call to come), an owner coldkey swap
# announced / executable now, a dissolve scheduled — the chain channel, and the "only announced" one (the user's, 6 Nov)
ANNOUNCED = ("chain", "announced")


def pick(a, i: int, *names: str):
    """Field of an event: by name when the chain gives named fields (dict), else by position."""
    if isinstance(a, dict):
        for k in names:
            if k in a:
                return a[k]
        vals = list(a.values())
        return vals[i] if i < len(vals) else None
    if isinstance(a, (list, tuple)):
        return a[i] if i < len(a) else None
    return a if i == 0 else None


def acct(a: str) -> str:
    return f"[{fmt.short(a)}]({alerts.account_url(a)})" if a else "—"


def now_t() -> str:
    """Now as a Discord timestamp (each reader's own time zone)."""
    return f"<t:{int(time.time())}:f>"


def expected(blocks: int, at: int) -> str:
    """When something `blocks` from now is expected: a Discord timestamp (each reader sees it in their own time zone) and
    how far off, and the block — 12 s a block, so it may come a little earlier or later."""
    ts = int(time.time() + blocks * BLOCK_SECONDS)
    return f"**<t:{ts}:f>** (<t:{ts}:R>) · block #{at}"


def eta(blocks: int) -> str:
    return fmt.duration(max(blocks, 1)) if blocks < 7200 else f"{blocks * BLOCK_SECONDS / 86400:.1f} days"


class SubnetWatch:
    def __init__(self, meta, discord, refresh, path: Path, dry_run: bool = False) -> None:
        self.meta = meta
        self.discord = discord
        self.refresh = refresh          # async: reload subnet metadata (and reset history of re-registered slots)
        self.on_change = None           # (netuid, what): its identity or owner changed — the news monitor looks for its
                                        # X account again at once (23 Oct: SN27 sold to Komms, renamed at 13:53)
        self.path = path
        self.dry_run = dry_run
        self.known: dict[int, SubnetInfo] = {}
        self.prices: dict[int, int] = {}  # last price seen per netuid — survives the subnet's removal
        self.facts: dict = {}
        self.posted = 0
        self._recent: dict[tuple, float] = {}
        self._prune_announced: int | None = None
        self._prune_seen: tuple[int | None, float] = (None, 0.0)
        self._gone: dict[int, tuple[SubnetInfo, float]] = {}  # recently deregistered, by slot
        self._removed_now: list[int] = []
        self._queued_now: list = []
        # announced owner coldkey swaps still waiting: owner → {"exec": first block it can be executed, "nets": [netuid], "notified": bool}
        self.swaps: dict[str, dict] = {}
        self._swaps_synced = 0.0
        self._pending_txs: dict[str, float] = {}   # swap transactions already announced from the mempool
        # each slot's last real name (its identity's) and the registration it belongs to — kept on disk: a new subnet names
        # the one it replaced even when that one's identity was cleared first and the chain already shows the newcomer's
        # name (6 Nov, SN99: "gani registered — takes the slot of “gani”", it was Thirty Spokes)
        self._slot_names: dict[int, tuple[int, str]] = {}
        self.block_file = path.with_name("lifecycle_block")   # the last block whose events were read (a restart catches up)
        try:                                                  # read now, before this run's first block overwrites it
            self._resume_after: int | None = int(self.block_file.read_text())
        except (OSError, ValueError):
            self._resume_after = None
        self._first_live: int | None = None
        self.sent: list[dict] = []      # payloads, for dry runs and tests
        self.sent_to: list[tuple] = []  # … and the channels each went to
        self.regcost = None             # regcost.RegCost: the registrations and the price to register (set by the app)
        # the cards in the "only announced" channel still pending, by what they wait for ("swap:<owner>:<netuid>",
        # "due:…", "start:<netuid>", "queued:<coldkey>", "dissolve:<netuid>") → {"id", "payload"}: edited to ✅ / ❌ when
        # it happens (the user, 10 Nov: the channel shows what is NOT done yet)
        self.announced: dict[str, dict] = {}
        self.on_announced = None        # called after a card goes to that channel (the live board moves below it)
        self.buy_discord = None         # buybacks and exchange buys (BUY_HOOK_URL); None: the chain channel
        self.announced_discord = None   # announced, not done yet (ONLY_ANNOUNCED_HOOK_URL); None: not posted twice

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Begin from the view saved before the last shutdown, so changes made while we were down are reported: first the
        chain events of the blocks missed (up to the first block read in this run), then the state."""
        self.known = dict(self.meta.subnets)
        try:
            saved = json.loads(self.path.read_text())
            for n, v in saved.get("subnets", {}).items():
                n = int(n)
                self.known[n] = SubnetInfo(n, v["name"], v.get("symbol", ""), v.get("pool", 0.0), v["registered_at"],
                                           None, None, v.get("owner", ""), v.get("owner_hotkey", ""), v.get("identity", {}))
            for n in [n for n in self.known if str(n) not in saved.get("subnets", {})]:
                del self.known[n]
            self._prune_announced = saved.get("prune")
            self._slot_names = {int(n): (int(r), str(nm)) for n, (r, nm) in (saved.get("names") or {}).items()}
            self.announced = {k: v for k, v in (saved.get("announced") or {}).items() if isinstance(v, dict) and v.get("id")}
            self.swaps = {k: v for k, v in (saved.get("swaps") or {}).items() if isinstance(v, dict) and "exec" in v}
        except (OSError, ValueError, KeyError):
            pass
        self._remember(self.known)
        for _ in range(60 if self._resume_after is not None else 0):   # the first live block: where the gap ends (≤ 30 s)
            if self._first_live is not None:
                break
            await asyncio.sleep(0.5)
        await self._catch_up(self._first_live)
        await self.reconcile()
        await self._sync_swaps()

    async def _catch_up(self, first_live: int | None, most: int = CATCH_UP_BLOCKS) -> None:
        """The chain events of the blocks missed while the monitor was down (6 Nov: SN99 Thirty Spokes was pruned during a
        restart — no card; its slot's new subnet came 4 minutes later). Up to `most` blocks; older changes the state
        check (reconcile) still reports, without their block."""
        last = self._resume_after
        if last is None or not first_live or first_live - 1 <= last:
            return
        start = max(last + 1, first_live - most)
        found, failed = 0, []
        for b in range(start, first_live):
            try:
                tr, life, *_ = await self.meta.trades(b, await self.meta.block_hash(b))
            except Exception as e:
                failed.append((b, e))
                continue
            if life:
                found += 1
                await self.on_events(b, life)
        log.info("subnet watch: caught up on #%d…#%d missed while down (%d with subnet events%s)", start, first_live - 1,
                 found, f"; {len(failed)} unreadable, e.g. #{failed[0][0]}: {failed[0][1]}" if failed else "")

    def checked(self, block: int) -> None:
        """This block's events were read (kept on disk, so a restart knows where to catch up from)."""
        if self._first_live is None:
            self._first_live = block
        try:
            self.block_file.write_text(str(block))
        except OSError:
            pass

    def _remember(self, view: dict) -> None:
        for n, i in view.items():
            name = (i.identity or {}).get("name") or ""
            if n and name:
                self._slot_names[n] = (i.registered_at, name)

    def _slot_name(self, n: int, prev: SubnetInfo | None, info: SubnetInfo | None) -> SubnetInfo | None:
        """The subnet that held slot `n` before `info`, with its real name — not the newcomer's, not an empty one."""
        reg, name = self._slot_names.get(n, (None, ""))
        newcomer = info.name if info else ""
        if not name or name == newcomer or (info and reg == info.registered_at):
            return prev
        if prev is None:
            return SubnetInfo(n, name, "", 0.0, reg or 0, None, None)
        return dataclasses.replace(prev, name=name) if not prev.name or prev.name == newcomer else prev

    def note(self, prices: dict[int, int]) -> None:
        self.prices.update(prices)

    def _save(self) -> None:
        try:
            self.path.write_text(json.dumps({"prune": self._prune_announced, "swaps": self.swaps,
                                             "names": {n: list(v) for n, v in self._slot_names.items()},
                                             "announced": self.announced, "subnets": {
                n: {"name": s.name, "symbol": s.symbol, "pool": s.pool_tao, "registered_at": s.registered_at,
                    "owner": s.owner, "owner_hotkey": s.owner_hotkey, "identity": s.identity}
                for n, s in self.known.items()}}))
        except OSError:
            pass

    def _mark(self, kind: str, n) -> None:
        self._recent[(kind, n)] = time.time()

    def _seen(self, kind: str, n) -> bool:
        return time.time() - self._recent.get((kind, n), 0) < 900

    # ── events (real time) ───────────────────────────────────────────────

    async def on_events(self, block: int, events: list[tuple[str, tuple]]) -> None:
        old = dict(self.known)
        names = {name for name, _ in events}
        try:
            if names & REFRESH_ON:
                await self.refresh()
            self.facts = await self.meta.facts()
        except Exception as e:
            log.warning("subnet watch: could not refresh chain state (%s)", e)
        new = self.meta.subnets
        # A registration prunes a subnet and queues the newcomer in ONE block, then adds it a few minutes
        # later together with its own hotkey/identity events. So: removals and additions first, and a new
        # subnet's setup events are part of its registration card, not separate news.
        self._removed_now = [int(pick(a, 0, "netuid")) for name, a in events if name == "NetworkRemoved"]
        self._queued_now = [a for name, a in events if name == "NetworkRegistrationQueued"]
        added = {int(pick(a, 0, "netuid")) for name, a in events if name == "NetworkAdded"}
        order = {"NetworkRemoved": 0, "NetworkRegistrationQueued": 1, "NetworkAdded": 2}
        setup = {"SubnetOwnerHotkeySet", "SubnetIdentitySet", "SymbolUpdated", "SubnetIdentityRemoved"}
        for name, a in sorted(events, key=lambda e: order.get(e[0], 9)):
            handler = getattr(self, "_ev_" + name, None)
            if handler is None:
                continue
            if name in setup and int(pick(a, 0, "netuid")) in added:
                continue
            try:
                await handler(a, old, new, block)
            except Exception:
                log.exception("subnet event %s failed", name)
        self._remember(new)
        self.known = dict(new)
        self._save()

    async def _ev_NetworkAdded(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        info = new.get(n)
        # the subnet REMOVED from this slot first: the view before this event may already hold the newcomer (SN70, 26 Oct:
        # "takes the slot of ghayn" — the newcomer's own name — instead of "Consted 2.0")
        gone = self._gone.get(n)
        was = old.get(n)
        if was is not None and info is not None and was.registered_at == info.registered_at:
            was = None
        prev = (gone[0] if gone and time.time() - gone[1] < 7 * 86400 else None) or was
        await self._added(n, self._slot_name(n, prev, info), info, block)

    async def _added(self, n: int, prev: SubnetInfo | None, info: SubnetInfo | None, block: int | None) -> None:
        if self._seen("added", n):
            return
        self._mark("added", n)
        self._mark("identity", n)
        self._mark("owner", n)
        name = info.name if info and info.name else ""
        replaced = f" — takes the slot of “{prev.name or f'SN{n}'}”" if prev else ""
        fields = []
        if info:
            fields += [{"name": "Owner", "value": acct(info.owner), "inline": True},
                       {"name": "Owner hotkey", "value": acct(info.owner_hotkey), "inline": True}]
            if info.registered_at:
                fields.append({"name": "Registered", "value": f"block #{info.registered_at}", "inline": True})
        fields += self._market(n, info) + self._slots()
        reg = self._reg_of(n, name, info.registered_at) if info and info.registered_at else None
        if reg is not None:
            fields.append(self._reg_price(reg))
        fields.append({"name": "Start call", "value": "⏳ **pending** — emissions (and trading volume) start when the owner "
                                                    "makes it; often days later", "inline": False})
        if info and info.identity.get("description"):
            fields.append({"name": "About", "value": info.identity["description"][:300], "inline": False})
        links = [f"[{k}]({u if u.startswith('http') else 'https://' + u})" for k, u in
                 (("website", (info.identity.get("url") if info else "") or ""), ("github", (info.identity.get("github") if info else "") or "")) if u]
        if links:
            fields.append({"name": "Links", "value": " · ".join(links), "inline": False})
        await self._post(f"🆕 **NEW SUBNET** · **SN{n} {name}**".rstrip() + f" registered{replaced}",
                         self._embed(n, info, f"🆕 NEW SUBNET REGISTERED · SN{n}",
                                     f"```diff\n+ SN{n} {name}".rstrip() + (f"\n- {prev.name or f'SN{n}'} (previous subnet in this slot)" if prev else "") + "\n```",
                                     fields, GREEN, block), ANNOUNCED, key=f"start:{n}")
        if info and info.owner:
            await self._done(f"queued:{info.owner}", f"✅ **Done** — registered as SN{n} {name}".rstrip() + f" {now_t()}")

    async def _ev_NetworkRemoved(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        await self._removed(n, self._slot_name(n, old.get(n), None), block)

    async def _removed(self, n: int, prev: SubnetInfo | None, block: int | None) -> None:
        if self._seen("removed", n):
            return
        self._mark("removed", n)
        if prev:
            self._gone[n] = (prev, time.time())
        name = prev.name if prev else ""
        fields = []
        for q in self._queued_now:  # pruned to make room for a registration queued in the same block
            newcomer = _text((pick(q, 3, "identity") or {}).get("subnet_name")) if isinstance(pick(q, 3, "identity"), dict) else ""
            fields.append({"name": "Removed to make room for", "inline": False,
                           "value": f"a new subnet{f' “{newcomer}”' if newcomer else ''}, registered by "
                                    f"{acct(str(pick(q, 0, 'coldkey')))} — it takes this slot once cleanup finishes (a few minutes)."})
        if n in self.prices:
            fields.append({"name": "Last price", "value": f"**{fmt.price(self.prices[n])} τ**", "inline": True})
        if prev:
            if prev.pool_tao:
                fields.append({"name": "Pool at last check", "value": f"**{fmt.tao_compact(prev.pool_tao)}**", "inline": True})
            fields.append({"name": "Owner", "value": acct(prev.owner), "inline": True})
            if prev.registered_at and block:
                fields.append({"name": "Lifetime", "value": f"{(block - prev.registered_at) * BLOCK_SECONDS / 86400:.0f} days "
                                                            f"(since block #{prev.registered_at})", "inline": True})
        fields += self._slots()
        self.prices.pop(n, None)  # whatever takes this slot next is a different subnet with its own price
        await self._done(f"dissolve:{n}", f"✅ **Done** — the subnet was removed {now_t()}")
        await self._done(f"start:{n}", f"❌ **Never started** — the subnet was removed {now_t()}", ok=False)
        for k in [k for k in self.announced if k.startswith(("swap:", "due:")) and k.endswith(f":{n}")]:
            await self._done(k, f"❌ **No longer pending** — the subnet was removed {now_t()}", ok=False)
        await self._post(f"💀 **SUBNET DEREGISTERED** · **SN{n} {name}**".rstrip() + " was removed from the network",
                         self._embed(n, prev, f"💀 SUBNET DEREGISTERED · SN{n}",
                                     f"```diff\n- SN{n} {name}".rstrip() + "\n```", fields, RED, block))

    async def _ev_NetworkRegistrationQueued(self, a, old, new, block) -> None:
        cold, hot = str(pick(a, 0, "coldkey")), str(pick(a, 1, "hotkey"))
        ident = pick(a, 3, "identity")
        ident = ident if isinstance(ident, dict) else {}
        cost = int(pick(a, 4, "lock_amount") or 0) / 1e9
        name, about = _text(ident.get("subnet_name")), _text(ident.get("description"))
        fields = [{"name": "Coldkey", "value": acct(cold), "inline": True},
                  {"name": "Hotkey", "value": acct(hot), "inline": True},
                  {"name": "TAO locked", "value": f"**{fmt.tao(cost)} τ**", "inline": True}]
        if self.regcost is not None and cost and block:
            reg = self.regcost.add(block, cost, time.time(), self._removed_now[0] if self._removed_now else None, name)
            fields.append(self._reg_price(reg))
        for n in self._removed_now:
            was = old.get(n)
            fields.append({"name": "Slot", "inline": False,
                           "value": f"**SN{n}** — “{was.name if was and was.name else f'SN{n}'}” was deregistered to make room. "
                                    f"The new subnet starts there once cleanup finishes (a few minutes)."})
        if about:
            fields.append({"name": "About", "value": about[:300], "inline": False})
        fields += self._slots()
        slot = f" → slot **SN{self._removed_now[0]}**" if self._removed_now else ""
        await self._post(f"⏳ **NEW SUBNET INCOMING**" + (f" · **“{name}”**" if name else "") + f"{slot} · {fmt.tao(cost)} τ locked",
                         self._embed(self._removed_now[0] if self._removed_now else None, None, "⏳ NEW SUBNET INCOMING — registration queued",
                                     f"```diff\n+ {name or 'new subnet'}\n```\n" + ("" if not self._removed_now else
                                     "It becomes tradable as a new subnet in a few minutes — its price history starts from zero."),
                                     fields, BLUE, block), ANNOUNCED, key=f"queued:{cold}")

    async def _ev_NetworkRegistrationCancelled(self, a, old, new, block) -> None:
        await self._post("❎ **Subnet registration cancelled** — the queued registration failed and its escrow was refunded",
                         self._embed(None, None, "❎ SUBNET REGISTRATION CANCELLED", "A queued registration failed terminally.",
                                     [{"name": "Coldkey", "value": acct(str(pick(a, 0, "coldkey"))), "inline": True},
                                      {"name": "Hotkey", "value": acct(str(pick(a, 1, "hotkey"))), "inline": True}], BLUE, block))

    async def _ev_DissolveNetworkScheduled(self, a, old, new, block) -> None:
        n, at = int(pick(a, 1, "netuid")), int(pick(a, 2, "execution_block", "block", "when"))
        info = new.get(n) or old.get(n)
        await self._post(f"⚠️ **{self._sn(n, info)}** — the owner scheduled **dissolving the subnet** in {eta(at - block)}",
                         self._embed(n, info, f"⚠️ DISSOLVE SCHEDULED · SN{n}",
                                     f"The owner scheduled this subnet to be dissolved at block #{at} (in about {eta(at - block)}).",
                                     [{"name": "Scheduled by", "value": acct(str(pick(a, 0, "account", "coldkey", "who"))), "inline": True},
                                      {"name": "Expected dissolve", "value": expected(at - block, at), "inline": False}]
                                     + self._market(n, info),
                                     RED, block), ANNOUNCED, key=f"dissolve:{n}")

    async def _owner_event(self, cold: str, old, new, block, emoji: str, what: str, detail: str, color: int, extra=None,
                           to: tuple[str, ...] = ("chain",), key: str = "") -> None:
        for n, info in sorted(new.items()):
            if n and (info.owner == cold or (old.get(n) and old[n].owner == cold)):
                await self._post(f"{emoji} **{self._sn(n, info)}** — owner coldkey swap **{what}**",
                                 self._embed(n, info, f"{emoji} OWNER COLDKEY SWAP {what.upper()} · SN{n}", detail,
                                             [{"name": "Owner coldkey", "value": acct(cold), "inline": True}] + (extra or [])
                                             + self._market(n, info), color, block), to, key=f"{key}:{n}" if key else None)

    async def _ev_ColdkeySwapAnnounced(self, a, old, new, block) -> None:
        delay = self.facts.get("swap_delay") or 0
        when = f" The swap can be executed after a wait of {eta(delay)}." if delay else ""
        who = str(pick(a, 0, "who", "coldkey", "old_coldkey"))
        # the user, 6 Nov: show WHEN it is expected — the first block it can be executed (the owner does it then or later)
        extra = [{"name": "Expected swap", "value": expected(delay, block + delay) + "\n(the owner can do it then or any "
                                                    "time after)", "inline": False}] if delay else []
        await self._owner_event(who, old, new, block, "🔑", "announced",
                                f"The subnet owner announced moving ownership to a new coldkey — the subnet is probably "
                                f"being sold.{when}", AMBER, extra=extra, to=ANNOUNCED, key=f"swap:{who}")
        nets = sorted(n for n, i in new.items() if n and i.owner == who)
        if nets and delay:     # remember it: a card follows when the wait is over and the swap can be executed
            self.swaps[who] = {"exec": block + delay, "nets": nets, "notified": False}
            self._save()

    async def _ev_ColdkeySwapped(self, a, old, new, block) -> None:
        old_ck, new_ck = str(pick(a, 0, "old_coldkey", "old")), str(pick(a, 1, "new_coldkey", "new"))
        self._swap_done(old_ck)
        for k in [k for k in self.announced if k.startswith((f"swap:{old_ck}:", f"due:{old_ck}:"))]:
            await self._done(k, f"✅ **Done** — swapped {now_t()} to {acct(new_ck)}")
        hit = False
        for n, info in sorted(new.items()):
            was = old.get(n)
            if n and ((was and was.owner == old_ck) or info.owner == new_ck and was and was.owner != new_ck):
                hit = True
                self._mark("owner", n)
                self._changed(n, "owner coldkey swapped")
                await self._post(f"🔑 **{self._sn(n, info)}** — owner coldkey **swapped**: {fmt.short(old_ck)} → {fmt.short(new_ck)}",
                                 self._embed(n, info, f"🔑 OWNER COLDKEY SWAPPED · SN{n}",
                                             "The subnet's owner coldkey was swapped to a new address.",
                                             [{"name": "Old owner", "value": acct(old_ck), "inline": True},
                                              {"name": "New owner", "value": acct(new_ck), "inline": True}] + self._market(n, info),
                                             AMBER, block))
        if not hit:   # any other wallet's coldkey swap: not a subnet sale, so no alert — but say so in the log
            log.info("coldkey swap in #%d is not a subnet owner's (no alert): %s → %s", block, fmt.short(old_ck), fmt.short(new_ck))

    def _changed(self, n: int, what: str) -> None:
        if self.on_change is not None:
            try:
                self.on_change(n, what)
            except Exception:
                log.exception("telling about SN%d's change failed", n)

    def _swap_done(self, owner: str) -> None:
        if self.swaps.pop(owner, None) is not None:
            self._save()

    async def _swap_off(self, who: str, how: str) -> None:
        self._swap_done(who)
        for k in [k for k in self.announced if k.startswith((f"swap:{who}:", f"due:{who}:"))]:
            await self._done(k, f"❌ **Not happening** — the swap was {how} {now_t()}", ok=False)

    async def _ev_ColdkeySwapDisputed(self, a, old, new, block) -> None:
        await self._swap_off(str(pick(a, 0, "who", "coldkey")), "disputed")
        await self._owner_event(str(pick(a, 0, "who", "coldkey")), old, new, block, "🚨", "disputed",
                                "The announced coldkey swap of this subnet's owner was disputed.", RED)

    async def _ev_ColdkeySwapReset(self, a, old, new, block) -> None:
        await self._swap_off(str(pick(a, 0, "who", "coldkey")), "reset")
        await self._owner_event(str(pick(a, 0, "who", "coldkey")), old, new, block, "🔑", "reset",
                                "The pending coldkey swap of this subnet's owner was reset.", AMBER)

    async def _ev_ColdkeySwapCleared(self, a, old, new, block) -> None:
        await self._swap_off(str(pick(a, 0, "who", "coldkey")), "cleared")
        await self._owner_event(str(pick(a, 0, "who", "coldkey")), old, new, block, "🔑", "cleared",
                                "The coldkey swap announcement of this subnet's owner was cleared.", AMBER)

    async def _ev_SubnetOwnerChanged(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        await self._owner_changed(n, str(pick(a, 1, "old_owner", "old_coldkey", "from")),
                                  str(pick(a, 2, "new_owner", "new_coldkey", "to")), new.get(n) or old.get(n), block,
                                  "Ownership was reassigned by lock conviction.")

    async def _owner_changed(self, n: int, was: str, now: str, info, block, how: str) -> None:
        if self._seen("owner", n):
            return
        self._mark("owner", n)
        self._changed(n, "owner changed")
        await self._post(f"👑 **{self._sn(n, info)}** — **owner changed**: {fmt.short(was)} → {fmt.short(now)}",
                         self._embed(n, info, f"👑 OWNER CHANGED · SN{n}", how,
                                     [{"name": "Old owner", "value": acct(was), "inline": True},
                                      {"name": "New owner", "value": acct(now), "inline": True}] + self._market(n, info),
                                     AMBER, block))

    async def _ev_SubnetOwnerHotkeySet(self, a, old, new, block) -> None:
        n, hot = int(pick(a, 0, "netuid")), str(pick(a, 1, "hotkey"))
        was = old.get(n)
        if was is None or was.owner_hotkey == hot or self._seen("added", n):
            return  # a new subnet's first hotkey is part of its registration
        info = new.get(n) or was
        await self._post(f"🗝️ **{self._sn(n, info)}** — owner hotkey changed to {fmt.short(hot)}",
                         self._embed(n, info, f"🗝️ OWNER HOTKEY CHANGED · SN{n}", "The subnet's owner hotkey was set to a new key.",
                                     [{"name": "Old hotkey", "value": acct(was.owner_hotkey), "inline": True},
                                      {"name": "New hotkey", "value": acct(hot), "inline": True}], AMBER, block))

    async def _ev_SubnetIdentitySet(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        await self._identity(n, old.get(n), new.get(n), block)

    async def _ev_SymbolUpdated(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        await self._identity(n, old.get(n), new.get(n), block)

    async def _identity(self, n: int, was: SubnetInfo | None, info: SubnetInfo | None, block) -> None:
        # one card per CHANGE, not per subnet per 15 min: the event and the 2-minute catch-up report the same change
        # once, while a second, different change (the repo link, then the website …) gets its own card
        sig = (n, tuple(sorted((info.identity or {}).items())) if info else (), info.symbol if info else "")
        if was is None or info is None or was.registered_at != info.registered_at or self._seen("identity", sig):
            return  # a new subnet setting its first identity is covered by its registration card
        changes = [(IDENTITY_LABELS[k], was.identity.get(k, ""), info.identity.get(k, "")) for k in IDENTITY_LABELS
                   if was.identity.get(k, "") != info.identity.get(k, "")]
        if was.symbol != info.symbol:
            changes.append(("Symbol", was.symbol, info.symbol))
        if not changes:
            return
        self._mark("identity", sig)
        self._changed(n, "identity changed")
        renamed = next((c for c in changes if c[0] == "Name"), None)
        diff = "\n".join(f"- {label}: {a_[:120] or '(empty)'}\n+ {label}: {b_[:120] or '(empty)'}" for label, a_, b_ in changes[:6])
        head = (f"✏️ **SN{n}** renamed: **“{renamed[1] or '(none)'}” → “{renamed[2] or '(none)'}”**" if renamed
                else f"✏️ **{self._sn(n, info)}** — identity changed: {', '.join(c[0].lower() for c in changes)}")
        await self._post(head, self._embed(n, info, f"✏️ {'RENAMED' if renamed else 'IDENTITY CHANGED'} · SN{n}",
                                           f"```diff\n{diff}\n```", self._market(n, info)
                                           + [{"name": "Owner", "value": acct(info.owner), "inline": True}], VIOLET, block))

    async def _ev_SubnetIdentityRemoved(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        info = old.get(n) or new.get(n)
        if not self._seen("removed", n) and n in new:
            await self._post(f"✏️ **{self._sn(n, info)}** — identity removed",
                             self._embed(n, info, f"✏️ IDENTITY REMOVED · SN{n}", "The subnet's name and links were removed.",
                                         self._market(n, info), VIOLET, block))

    async def _ev_FirstEmissionBlockNumberSet(self, a, old, new, block) -> None:
        n, at = int(pick(a, 0, "netuid")), int(pick(a, 1, "block_number", "block", "first_emission_block"))
        info = new.get(n) or old.get(n)
        when = "now" if at <= block else f"in about {eta(at - block)}"
        await self._post(f"🚀 **{self._sn(n, info)}** — **emissions start** {when} (block #{at})",
                         self._embed(n, info, f"🚀 EMISSIONS START · SN{n}", f"The subnet's start call was made: emissions begin at block #{at}.",
                                     self._market(n, info), GREEN, block))
        await self._done(f"start:{n}", f"✅ **Done** — start call made {now_t()}, emissions from block #{at}")

    async def _ev_SubnetLeaseCreated(self, a, old, new, block) -> None:
        n = int(pick(a, 2, "netuid"))
        info = new.get(n) or old.get(n)
        end = pick(a, 3, "end_block")
        await self._post(f"📜 **{self._sn(n, info)}** — subnet lease created",
                         self._embed(n, info, f"📜 SUBNET LEASE CREATED · SN{n}",
                                     "A lease was created for this subnet" + (f", ending at block #{int(end)}." if end else " (perpetual)."),
                                     [{"name": "Beneficiary", "value": acct(str(pick(a, 0, "beneficiary"))), "inline": True}],
                                     BLUE, block))

    async def _ev_SubnetLeaseTerminated(self, a, old, new, block) -> None:
        n = int(pick(a, 1, "netuid"))
        info = new.get(n) or old.get(n)
        await self._post(f"📜 **{self._sn(n, info)}** — subnet lease terminated",
                         self._embed(n, info, f"📜 SUBNET LEASE TERMINATED · SN{n}", "The subnet's lease ended.",
                                     [{"name": "Beneficiary", "value": acct(str(pick(a, 0, "beneficiary"))), "inline": True}],
                                     BLUE, block))

    async def _ev_SubnetLimitSet(self, a, old, new, block) -> None:
        limit = int(pick(a, 0, "limit", "max_subnets"))
        await self._post(f"🧮 **Subnet slots changed** — the network now allows **{limit}** subnets",
                         self._embed(None, None, "🧮 SUBNET SLOT LIMIT CHANGED", f"The maximum number of subnets is now **{limit}**.",
                                     self._slots(), BLUE, block))

    # ── safety net (each metadata refresh) ───────────────────────────────

    async def reconcile(self) -> None:
        """Announce anything that changed since the last view and wasn't already reported by its event."""
        new, old = self.meta.subnets, self.known
        if not new:
            return
        for n in sorted(set(new) - set(old)):
            if n:
                await self._added(n, self._slot_name(n, None, new[n]), new[n], None)
        for n in sorted(set(old) - set(new)):
            if n:
                await self._removed(n, self._slot_name(n, old[n], None), None)
        for n in sorted(set(old) & set(new)):
            if not n:
                continue
            was, info = old[n], new[n]
            if was.registered_at != info.registered_at:
                await self._added(n, self._slot_name(n, was, info), info, None)
            elif was.owner and info.owner and was.owner != info.owner:
                await self._owner_changed(n, was.owner, info.owner, info, None, "The subnet's owner coldkey changed.")
            else:
                await self._identity(n, was, info, None)
        self._remember(new)
        self.known = dict(new)
        if time.time() - self._swaps_synced > 1800:
            await self._sync_swaps()
        try:
            self.facts = await self.meta.facts()
            await self._prune_watch()
        except Exception as e:
            log.debug("subnet facts failed: %s", e)
        self._save()

    async def _prune_watch(self) -> None:
        cur = self.facts.get("prune")
        cur = int(cur) if cur is not None else None
        if self._prune_announced is None and self._prune_seen[0] is None:
            self._prune_announced = cur  # first look: nothing to compare with
        seen, since = self._prune_seen
        if cur != seen:
            self._prune_seen = (cur, time.time())
            return
        if cur is None or cur == self._prune_announced or time.time() - since < PRUNE_HOLD:
            return
        prev, self._prune_announced = self._prune_announced, cur
        info = self.meta.subnets.get(cur)
        was = f" (it was SN{prev} {self.meta.info(prev).name})".rstrip() if prev is not None else ""
        await self._post(f"⚠️ **{self._sn(cur, info)}** is now **first in line to be deregistered** when the next subnet registers{was}",
                         self._embed(cur, info, f"⚠️ NEXT IN LINE TO BE DEREGISTERED · SN{cur}",
                                     "If a new subnet registers while every slot is taken, this subnet is the one removed.",
                                     self._market(cur, info) + self._slots(), AMBER, None))

    # ── a subnet sale, early: the swap sits in the mempool; and the wait after the announcement ends ───

    def _owned(self, owner: str) -> list[int]:
        return sorted(n for n, i in {**self.known, **self.meta.subnets}.items() if n and i.owner == owner)

    async def swap_pending(self, owner: str, new_coldkey: str, tx_hash: str) -> None:
        """The owner's swap transaction was seen in the public mempool: it lands in the next block or two."""
        nets = self._owned(owner)
        if not nets or tx_hash in self._pending_txs:
            return
        self._pending_txs[tx_hash] = time.time()
        for h in [h for h, t in self._pending_txs.items() if time.time() - t > 3600]:
            del self._pending_txs[h]
        for n in nets:
            info = self.meta.subnets.get(n) or self.known.get(n)
            await self._post(f"🔑 **{self._sn(n, info)}** — owner coldkey **swap transaction in the mempool**"
                             f"{': ' + fmt.short(owner) + ' → ' + fmt.short(new_coldkey) if new_coldkey else ''}",
                             self._embed(n, info, f"🔑 OWNER COLDKEY SWAP PENDING · SN{n}",
                                         "The owner's swap transaction is waiting in the public mempool and lands in the next block or two. "
                                         "Bots stake into the subnet in that very block, so the price can move at once.",
                                         [{"name": "Old owner", "value": acct(owner), "inline": True}]
                                         + ([{"name": "New owner", "value": acct(new_coldkey), "inline": True}] if new_coldkey else [])
                                         + self._market(n, info), AMBER, None, "seen in the mempool, before the block"))

    def due_swaps(self, head: int) -> list[str]:
        """Owners whose announced swap can be executed from `head` on and who have not been told yet."""
        return [o for o, s in self.swaps.items() if not s.get("notified") and head >= int(s["exec"])]

    async def post_due(self, owners: list[str], head: int) -> None:
        for owner in owners:
            s = self.swaps.get(owner)
            if not s or s.get("notified"):
                continue
            s["notified"] = True
            self._save()
            for n in s.get("nets") or self._owned(owner):
                info = self.meta.subnets.get(n) or self.known.get(n)
                late = head - int(s["exec"])
                await self._post(f"⏰ **{self._sn(n, info)}** — the announced coldkey swap **can be executed now**",
                                 self._embed(n, info, f"⏰ OWNER COLDKEY SWAP CAN BE EXECUTED NOW · SN{n}",
                                             f"The wait after the announcement is over (since block #{s['exec']}"
                                             f"{', ' + eta(late) + ' ago' if late > 25 else ''}). The owner can complete the swap in any "
                                             f"block from now on — when it lands, the subnet changes hands.",
                                             [{"name": "Owner coldkey", "value": acct(owner), "inline": True}] + self._market(n, info)
                                             + [{"name": "Expected swap", "value": "⏳ **any block now** — waiting for the "
                                                                                   "owner to execute it", "inline": False}],
                                             AMBER, head), ANNOUNCED, key=f"due:{owner}:{n}")

    async def _sync_swaps(self) -> None:
        """Read the announced swaps from the chain (after a restart, and as a check on the events)."""
        self._swaps_synced = time.time()
        owners = sorted({i.owner for n, i in self.meta.subnets.items() if n and i.owner})
        if not owners:
            return
        try:
            chain = await self.meta.swap_announcements(owners)
        except Exception as e:
            log.debug("could not read the swap announcements (%s)", e)
            return
        keep = {}
        for owner, ex in chain.items():
            before = self.swaps.get(owner) or {}
            keep[owner] = {"exec": ex, "nets": self._owned(owner),
                           "notified": bool(before.get("exec") == ex and before.get("notified"))}
        if keep != self.swaps:
            self.swaps = keep
            self._save()

    # ── building blocks ──────────────────────────────────────────────────

    @staticmethod
    def _sn(n: int, info: SubnetInfo | None) -> str:
        return f"SN{n} {info.name}".strip() if info else f"SN{n}"

    def _market(self, n: int, info: SubnetInfo | None) -> list[dict]:
        out = []
        if n in self.prices and self.prices[n]:
            out.append({"name": "Price", "value": f"**{fmt.price(self.prices[n])} τ**", "inline": True})
        if info and info.pool_tao:
            out.append({"name": "Pool liquidity", "value": f"**{fmt.tao_compact(info.pool_tao)}**", "inline": True})
        return out

    def _slots(self) -> list[dict]:
        f = self.facts
        if not f.get("limit"):
            return []
        out = [{"name": "Slots", "value": f"**{f.get('subnets', 0)} / {f['limit']}** used", "inline": True}]
        if f.get("cost"):
            out.append({"name": "Cost to register now", "value": f"**{fmt.tao(f['cost'])} τ**", "inline": True})
        return out

    def _embed(self, n: int | None, info: SubnetInfo | None, title: str, desc: str, fields: list[dict], color: int,
               block: int | None, note: str = "") -> dict:
        e = {"title": title, "description": desc, "color": color, "fields": fields,
             "footer": {"text": "Subnet lifecycle" + (f" · block #{block}" if block else f" · {note or 'found by state check'}")},
             "timestamp": datetime.now(timezone.utc).isoformat()}
        if n is not None:
            e["url"] = alerts.subnet_url(n)
            e["author"] = alerts._author(info) if info else {"name": f"SN{n}", "url": alerts.subnet_url(n)}
        return e

    async def buyback(self, b, block: int) -> None:
        """💰 a subnet's team bought its own token back (buyback.find)."""
        info = self.meta.subnets.get(b.netuid)
        how = {"add_stake_burn": "the chain's buyback call (add_stake_burn): bought and burned at once",
               "owner": "the subnet owner's own coldkey", "wallet": "the team's buyback wallet (NEWS_BUYBACK_WALLETS)",
               "buy+burn": "bought, then burned / recycled in the same block",
               "owner-funded": "a new wallet paid with the owner's money",
               "exchange": "a new, quiet wallet paid with money straight from an exchange / bridge"}.get(b.how, b.how)
        fields = [{"name": "Bought", "value": f"**{b.tao:,.2f} τ**", "inline": True}] + self._market(b.netuid, info)
        if b.coldkey:
            fields.append({"name": "Wallet", "value": acct(b.coldkey), "inline": True})
        if b.detail:
            fields.append({"name": "Money", "value": b.detail[:1000], "inline": False})
        await self._post(f"💰 **BUYBACK** · **{self._sn(b.netuid, info)}** — {b.text()}",
                         self._embed(b.netuid, info, f"💰 BUYBACK · SN{b.netuid}", f"How: {how}.", fields, GREEN, block), ("buy",))

    async def exchange_buy(self, b, block: int) -> None:
        """🏦 a new, quiet wallet bought big with money straight from an exchange / bridge — it MAY be the owner buying
        back, but nothing ties it to the owner, so it is only shown (the bots do not trade it)."""
        info = self.meta.subnets.get(b.netuid)
        fields = [{"name": "Bought", "value": f"**{b.tao:,.2f} τ**", "inline": True}] + self._market(b.netuid, info)
        if b.coldkey:
            fields.append({"name": "Wallet", "value": acct(b.coldkey), "inline": True})
        if b.detail:
            fields.append({"name": "Money", "value": b.detail[:1000], "inline": False})
        await self._post(f"🏦 **EXCHANGE BUY** · **{self._sn(b.netuid, info)}** — a new wallet bought {b.tao:,.2f} τ with money "
                         f"straight from an exchange / bridge · it may be the owner buying back",
                         self._embed(b.netuid, info, f"🏦 EXCHANGE BUY · SN{b.netuid}",
                                     "A new, quiet wallet bought with money straight from an exchange / bridge. It may be the "
                                     "owner buying back — nothing on chain ties it to the owner's wallet. Not traded.",
                                     fields, BLUE, block), ("buy",))

    def _reg_of(self, n: int, name: str, registered_at: int):
        """The registration a new subnet came from (queued ≤ 2 h before it), named after it."""
        if self.regcost is None:
            return None
        self.regcost.named(n, name, registered_at)
        return next((r for r in reversed(self.regcost.regs) if r.netuid == n and r.block <= registered_at), None)

    def _reg_price(self, reg) -> dict:
        """The user, 10 Nov: what this registration paid next to the one before ("last 252.78 τ → now 85.94 τ")."""
        prev = self.regcost.last(reg.block) if self.regcost is not None else None
        if prev is None:
            return {"name": "Registration price", "value": f"**{reg.tao:,.2f} τ**", "inline": False}
        who = f"SN{prev.netuid} {prev.name}".strip() if prev.netuid is not None else (prev.name or "the one before")
        when = f", <t:{int(prev.ts)}:d>" if prev.ts else ""
        pct = (reg.tao / prev.tao - 1) * 100 if prev.tao else 0
        return {"name": "Registration price", "inline": False,
                "value": f"last one **{prev.tao:,.2f} τ** ({who}{when}) → this one **{reg.tao:,.2f} τ** ({pct:+.0f}%)"}

    async def _done(self, key: str, status: str, ok: bool = True) -> None:
        """A card in the "only announced" channel whose wait is over: edited to say so (✅ done / ❌ not happening)."""
        card = self.announced.pop(key, None)
        if card is None:
            return
        self._save()
        if self.announced_discord is None or self.dry_run:
            return
        e = dict(card["payload"]["embeds"][0])
        e["title"] = ("✅ " if ok else "❌ ") + e.get("title", "")
        e["color"] = GREEN if ok else GREY
        e["fields"] = [f for f in e.get("fields", []) if f.get("name") not in ("Expected swap", "Start call",
                                                                                 "Expected dissolve")]
        e["fields"].append({"name": "Status", "value": status, "inline": False})
        self.announced_discord.edit_later(card["id"], {**card["payload"], "embeds": [e]})
        if self.on_announced is not None:
            self.on_announced(False)

    async def _post(self, headline: str, embed: dict, to: tuple[str, ...] = ("chain",), key: str | None = None) -> None:
        """`to`: "chain" (CHAIN_UPDATE_HOOK_URL), "buy" (buybacks and exchange buys: BUY_HOOK_URL, else the chain
        channel), "announced" (announced, not done yet: ONLY_ANNOUNCED_HOOK_URL, if set) — the user's split, 6 Nov."""
        payload = {"content": alerts.content([headline]), "embeds": [embed], "allowed_mentions": {"parse": []}}
        self.posted += 1
        self.sent.append(payload)
        self.sent_to.append(to)
        del self.sent[:-50], self.sent_to[:-50]
        print(fmt.byellow(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {headline[:170]}"))
        if not self.dry_run:
            hooks = {"chain": self.discord, "buy": self.buy_discord or self.discord, "announced": self.announced_discord}
            for d in dict.fromkeys(hooks[k] for k in to if hooks.get(k) is not None):   # one post per channel
                msg_id = await d.send(payload)
                if d is self.announced_discord and "announced" in to:
                    if key and msg_id:              # edited to ✅ / ❌ when its wait is over
                        self.announced[key] = {"id": msg_id, "payload": payload}
                        self._save()
                    if self.on_announced is not None:
                        self.on_announced(True)     # the live board moves below the new card
