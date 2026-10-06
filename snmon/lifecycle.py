"""Subnet lifecycle monitor — slots, owners, identities.

Real time from chain events (decoded every block, ~1s after the block):

  🆕 NetworkAdded                 a new subnet took a slot (and which subnet it replaced)
  💀 NetworkRemoved               a subnet was deregistered
  ⏳ NetworkRegistrationQueued    someone queued a new subnet registration (+ who would be pruned)
  ⚠️ DissolveNetworkScheduled     an owner scheduled dissolving their subnet
  🔑 ColdkeySwap*                 a subnet OWNER's coldkey swap: announced / swapped / disputed / reset
  👑 SubnetOwnerChanged           ownership reassigned · 🗝️ SubnetOwnerHotkeySet
  ✏️ SubnetIdentitySet / 🔤 SymbolUpdated   name, links, logo or symbol changed (shown as a diff)
  🚀 FirstEmissionBlockNumberSet  emissions start · 📜 lease created / terminated · 🧮 slot limit changed

Safety net: after every metadata refresh (each minute) the subnet list is compared with what was
last reported, so a change is still announced if its event was missed (node hiccup, restart — the
last view is kept on disk). Also announced: when a different subnet becomes first in line to be
deregistered (only once it has held that position for 10 minutes).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from . import alerts, fmt
from .config import BLOCK_SECONDS
from .meta import SubnetInfo, _text

log = logging.getLogger("snmon.lifecycle")

GREEN, RED, AMBER, VIOLET, BLUE = 0x22C55E, 0xEF4444, 0xF59E0B, 0x8B5CF6, 0x3B82F6
REFRESH_ON = {"NetworkAdded", "NetworkRemoved", "SubnetIdentitySet", "SubnetIdentityRemoved", "SymbolUpdated",
              "SubnetOwnerChanged", "SubnetOwnerHotkeySet", "ColdkeySwapped"}
IDENTITY_LABELS = {"name": "Name", "url": "Website", "github": "GitHub", "description": "Description",
                   "logo": "Logo", "discord": "Discord", "contact": "Contact"}
PRUNE_HOLD = 600  # seconds a new prune candidate must hold before it is announced


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


def eta(blocks: int) -> str:
    return fmt.duration(max(blocks, 1)) if blocks < 7200 else f"{blocks * BLOCK_SECONDS / 86400:.1f} days"


class SubnetWatch:
    def __init__(self, meta, discord, refresh, path: Path, dry_run: bool = False) -> None:
        self.meta = meta
        self.discord = discord
        self.refresh = refresh          # async: reload subnet metadata (and reset history of re-registered slots)
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
        self.sent: list[dict] = []      # payloads, for dry runs and tests

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Begin from the view saved before the last shutdown, so changes made while we were down are reported."""
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
        except (OSError, ValueError, KeyError):
            pass
        await self.reconcile()

    def note(self, prices: dict[int, int]) -> None:
        self.prices.update(prices)

    def _save(self) -> None:
        try:
            self.path.write_text(json.dumps({"prune": self._prune_announced, "subnets": {
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
        self.known = dict(new)
        self._save()

    async def _ev_NetworkAdded(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        gone = self._gone.get(n)
        prev = old.get(n) or (gone[0] if gone and time.time() - gone[1] < 7 * 86400 else None)
        await self._added(n, prev, new.get(n), block)

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
        if info and info.identity.get("description"):
            fields.append({"name": "About", "value": info.identity["description"][:300], "inline": False})
        links = [f"[{k}]({u if u.startswith('http') else 'https://' + u})" for k, u in
                 (("website", (info.identity.get("url") if info else "") or ""), ("github", (info.identity.get("github") if info else "") or "")) if u]
        if links:
            fields.append({"name": "Links", "value": " · ".join(links), "inline": False})
        await self._post(f"🆕 **NEW SUBNET** · **SN{n} {name}**".rstrip() + f" registered{replaced}",
                         self._embed(n, info, f"🆕 NEW SUBNET REGISTERED · SN{n}",
                                     f"```diff\n+ SN{n} {name}".rstrip() + (f"\n- {prev.name or f'SN{n}'} (previous subnet in this slot)" if prev else "") + "\n```",
                                     fields, GREEN, block))

    async def _ev_NetworkRemoved(self, a, old, new, block) -> None:
        n = int(pick(a, 0, "netuid"))
        await self._removed(n, old.get(n), block)

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
                                     fields, BLUE, block))

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
                                     [{"name": "Scheduled by", "value": acct(str(pick(a, 0, "account", "coldkey", "who"))), "inline": True}]
                                     + self._market(n, info),
                                     RED, block))

    async def _owner_event(self, cold: str, old, new, block, emoji: str, what: str, detail: str, color: int, extra=None) -> None:
        for n, info in sorted(new.items()):
            if n and (info.owner == cold or (old.get(n) and old[n].owner == cold)):
                await self._post(f"{emoji} **{self._sn(n, info)}** — owner coldkey swap **{what}**",
                                 self._embed(n, info, f"{emoji} OWNER COLDKEY SWAP {what.upper()} · SN{n}", detail,
                                             [{"name": "Owner coldkey", "value": acct(cold), "inline": True}] + (extra or [])
                                             + self._market(n, info), color, block))

    async def _ev_ColdkeySwapAnnounced(self, a, old, new, block) -> None:
        delay = self.facts.get("swap_delay") or 0
        when = f" It can be executed in about {eta(delay)} (block #{block + delay})." if delay else ""
        await self._owner_event(str(pick(a, 0, "who", "coldkey", "old_coldkey")), old, new, block, "🔑", "announced",
                                f"The subnet owner announced moving ownership to a new coldkey.{when}", AMBER)

    async def _ev_ColdkeySwapped(self, a, old, new, block) -> None:
        old_ck, new_ck = str(pick(a, 0, "old_coldkey", "old")), str(pick(a, 1, "new_coldkey", "new"))
        for n, info in sorted(new.items()):
            was = old.get(n)
            if n and ((was and was.owner == old_ck) or info.owner == new_ck and was and was.owner != new_ck):
                self._mark("owner", n)
                await self._post(f"🔑 **{self._sn(n, info)}** — owner coldkey **swapped**: {fmt.short(old_ck)} → {fmt.short(new_ck)}",
                                 self._embed(n, info, f"🔑 OWNER COLDKEY SWAPPED · SN{n}",
                                             "The subnet's owner coldkey was swapped to a new address.",
                                             [{"name": "Old owner", "value": acct(old_ck), "inline": True},
                                              {"name": "New owner", "value": acct(new_ck), "inline": True}] + self._market(n, info),
                                             AMBER, block))

    async def _ev_ColdkeySwapDisputed(self, a, old, new, block) -> None:
        await self._owner_event(str(pick(a, 0, "who", "coldkey")), old, new, block, "🚨", "disputed",
                                "The announced coldkey swap of this subnet's owner was disputed.", RED)

    async def _ev_ColdkeySwapReset(self, a, old, new, block) -> None:
        await self._owner_event(str(pick(a, 0, "who", "coldkey")), old, new, block, "🔑", "reset",
                                "The pending coldkey swap of this subnet's owner was reset.", AMBER)

    async def _ev_ColdkeySwapCleared(self, a, old, new, block) -> None:
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
        if was is None or info is None or was.registered_at != info.registered_at or self._seen("identity", n):
            return  # a new subnet setting its first identity is covered by its registration card
        changes = [(IDENTITY_LABELS[k], was.identity.get(k, ""), info.identity.get(k, "")) for k in IDENTITY_LABELS
                   if was.identity.get(k, "") != info.identity.get(k, "")]
        if was.symbol != info.symbol:
            changes.append(("Symbol", was.symbol, info.symbol))
        if not changes:
            return
        self._mark("identity", n)
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
                await self._added(n, None, new[n], None)
        for n in sorted(set(old) - set(new)):
            if n:
                await self._removed(n, old[n], None)
        for n in sorted(set(old) & set(new)):
            if not n:
                continue
            was, info = old[n], new[n]
            if was.registered_at != info.registered_at:
                await self._added(n, was, info, None)
            elif was.owner and info.owner and was.owner != info.owner:
                await self._owner_changed(n, was.owner, info.owner, info, None, "The subnet's owner coldkey changed.")
            else:
                await self._identity(n, was, info, None)
        self.known = dict(new)
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
               block: int | None) -> dict:
        e = {"title": title, "description": desc, "color": color, "fields": fields,
             "footer": {"text": "Subnet lifecycle" + (f" · block #{block}" if block else " · found by state check")},
             "timestamp": datetime.now(timezone.utc).isoformat()}
        if n is not None:
            e["url"] = alerts.subnet_url(n)
            e["author"] = alerts._author(info) if info else {"name": f"SN{n}", "url": alerts.subnet_url(n)}
        return e

    async def _post(self, headline: str, embed: dict) -> None:
        payload = {"content": alerts.content([headline]), "embeds": [embed], "allowed_mentions": {"parse": []}}
        self.posted += 1
        self.sent.append(payload)
        del self.sent[:-50]
        print(fmt.byellow(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {headline[:170]}"))
        if not self.dry_run:
            await self.discord.send(payload)
