"""Run: .venv/bin/python tests/test_lifecycle.py   (event shapes are the real ones from blocks 9210610 / 9210632)"""
import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.lifecycle import SubnetWatch  # noqa: E402
from snmon.meta import SubnetInfo  # noqa: E402

OLD_OWNER = "5F48aA3Pw1vFRfnhhfu7EJKqzrj8y9VBWtS4PE9xJ1chHXqB"
NEW_OWNER = "5D5RwyMPFaKLwChEcfb5yPA6VENHxJBagcHkP5aztfg8Ynj9"
NEW_HOT = "5DDLUnX2yicKLC1T5PN7bkwgU51buJt93XQFV1giqhDsGUTk"
QUEUED = {"coldkey": NEW_OWNER, "hotkey": NEW_HOT, "mechid": 1,
          "identity": {"subnet_name": "Carbon", "github_repo": "", "subnet_contact": "", "subnet_url": "", "discord": "",
                       "description": "Transforming engineering simulation through Machine Learning", "logo_url": "", "additional": ""},
          "lock_amount": 653019966758, "median_subnet_alpha_price": {"bits": 82425368120209387}, "registration_block": 9210610}


def sub(n, name, owner=OLD_OWNER, reg=8294730, hot="5Hot", pool=829.0, **ident):
    return SubnetInfo(n, name, "a", pool, reg, None, None, owner, hot, {"name": name, "url": "", "github": "", "description": "",
                                                                        "logo": "", "discord": "", "contact": "", **ident})


class Meta:
    def __init__(self, subnets):
        self.subnets = dict(subnets)
        self.next = None
        self.prune = 92

    def info(self, n):
        return self.subnets.get(n) or SubnetInfo(n, "", "", 0.0, 0, None, None)

    async def facts(self):
        return {"subnets": 128, "limit": 128, "cost": 1149.6, "prune": self.prune, "swap_delay": 36000}

    announced: dict = {}

    async def swap_announcements(self, owners):
        return {o: b for o, b in self.announced.items() if o in owners}


def watch(subnets):
    meta = Meta(subnets)

    async def refresh():
        if meta.next is not None:
            meta.subnets, meta.next = meta.next, None

    w = SubnetWatch(meta, None, refresh, Path(tempfile.mkdtemp()) / "s.json", dry_run=True)
    w.known = dict(meta.subnets)
    return w, meta


def heads(w):
    return [p["content"].replace("​\n", "") for p in w.sent]


BASE = {0: sub(0, "root"), 64: sub(64, "Chutes", owner="5Chutes"), 116: sub(116, "for sale")}


def test_real_slot_change_makes_exactly_three_cards():
    async def go():
        w, meta = watch(BASE)
        w.note({116: 1_995_000})
        meta.next = {k: v for k, v in BASE.items() if k != 116}                       # block 9210610: pruned, newcomer queued
        await w.on_events(9210610, [("NetworkRemoved", 116), ("NetworkRegistrationQueued", QUEUED)])
        meta.next = {**meta.subnets, 116: sub(116, "Carbon", owner=NEW_OWNER, reg=9210632, hot=NEW_HOT, pool=653.02)}
        w.note({116: 2_100_000})
        await w.on_events(9210632, [("SubnetOwnerHotkeySet", (116, NEW_HOT)), ("SubnetIdentitySet", 116), ("NetworkAdded", (116, 1))])
        h = heads(w)
        assert len(h) == 3, h
        assert h[0].startswith("💀 **SUBNET DEREGISTERED** · **SN116 for sale**")
        assert h[1].startswith("⏳ **NEW SUBNET INCOMING** · **“Carbon”** → slot **SN116** · 653.02 τ locked")
        assert h[2] == "🆕 **NEW SUBNET** · **SN116 Carbon** registered — takes the slot of “for sale”"
        price = next(f["value"] for f in w.sent[2]["embeds"][0]["fields"] if f["name"] == "Price")
        assert "0.002100" in price, "the newcomer's card must show its own price, not the removed subnet's"
        await w.reconcile()                                                           # the safety net adds nothing
        assert len(w.sent) == 3
    asyncio.run(go())


def test_rename_is_shown_as_a_diff_and_no_change_is_silent():
    async def go():
        w, meta = watch(BASE)
        meta.next = {**BASE, 64: sub(64, "Chutes AI", owner="5Chutes", url="chutes.ai")}
        await w.on_events(100, [("SubnetIdentitySet", 64)])
        assert heads(w) == ["✏️ **SN64** renamed: **“Chutes” → “Chutes AI”**"]
        d = w.sent[0]["embeds"][0]["description"]
        assert "- Name: Chutes\n+ Name: Chutes AI" in d and "+ Website: chutes.ai" in d
        await w.on_events(101, [("SubnetIdentitySet", 64)])                           # set again, same values
        assert len(w.sent) == 1
    asyncio.run(go())



def test_a_second_different_identity_change_soon_after_gets_its_own_card_the_same_one_does_not():
    async def go():
        w, meta = watch(BASE)
        meta.next = {**BASE, 64: sub(64, "Chutes", owner="5Chutes", github="https://github.com/chutes/miner")}
        await w.on_events(100, [("SubnetIdentitySet", 64)])                           # the repo link
        assert len(w.sent) == 1 and "+ GitHub: https://github.com/chutes/miner" in w.sent[0]["embeds"][0]["description"]
        w.known = dict(meta.subnets)
        await w._identity(64, BASE[64], meta.subnets[64], None)                       # the same change seen again
        assert len(w.sent) == 1
        meta.next = {**meta.subnets, 64: sub(64, "Chutes", owner="5Chutes", github="https://github.com/chutes/miner",
                                             url="https://chutes.ai")}
        await w.on_events(110, [("SubnetIdentitySet", 64)])                           # minutes later: the website
        assert len(w.sent) == 2 and "+ Website: https://chutes.ai" in w.sent[1]["embeds"][0]["description"]
    asyncio.run(go())


def test_coldkey_swap_only_matters_for_subnet_owners():
    async def go():
        w, meta = watch(BASE)
        await w.on_events(100, [("ColdkeySwapAnnounced", ("5Nobody", "0xabc"))])
        assert w.sent == []
        await w.on_events(101, [("ColdkeySwapAnnounced", ("5Chutes", "0xabc"))])
        assert heads(w) == ["🔑 **SN64 Chutes** — owner coldkey swap **announced**"]
        assert "5.0 days" in w.sent[0]["embeds"][0]["description"]
        meta.next = {**BASE, 64: sub(64, "Chutes", owner="5NewChutes")}
        await w.on_events(102, [("ColdkeySwapped", ("5Chutes", "5NewChutes"))])
        assert heads(w)[1].startswith("🔑 **SN64 Chutes** — owner coldkey **swapped**")
        await w.reconcile()
        assert len(w.sent) == 2, "the state check must not repeat what the event already reported"
    asyncio.run(go())


def test_state_check_catches_what_events_missed():
    async def go():
        w, meta = watch(BASE)
        meta.subnets = {0: BASE[0], 64: sub(64, "Chutes", owner="5Sold"), 77: sub(77, "Fresh", owner="5New", reg=999)}
        await w.reconcile()
        h = heads(w)
        assert any(x.startswith("🆕 **NEW SUBNET** · **SN77 Fresh**") for x in h)
        assert any(x.startswith("💀 **SUBNET DEREGISTERED** · **SN116 for sale**") for x in h)
        assert any(x.startswith("👑 **SN64 Chutes** — **owner changed**") for x in h)
        assert len(h) == 3
    asyncio.run(go())


def test_next_to_be_pruned_is_announced_only_after_it_holds():
    async def go():
        import snmon.lifecycle as L
        w, meta = watch({**BASE, 92: sub(92, "Bottom"), 55: sub(55, "NIOME")})
        await w.reconcile()                      # first look: SN92, nothing to say
        meta.prune = 55
        await w.reconcile()                      # changed just now — wait
        assert w.sent == []
        w._prune_seen = (55, time.time() - L.PRUNE_HOLD - 1)
        await w.reconcile()
        assert heads(w) == ["⚠️ **SN55 NIOME** is now **first in line to be deregistered** when the next subnet registers (it was SN92 Bottom)"]
        await w.reconcile()
        assert len(w.sent) == 1
    asyncio.run(go())


def test_a_swap_in_the_mempool_posts_a_card_before_the_block_and_only_once_per_transaction():
    async def go():
        w, meta = watch(BASE)
        await w.swap_pending("5Nobody", "5New", "0xtx0")                      # not a subnet owner: nothing
        assert w.sent == []
        await w.swap_pending("5Chutes", "5NewChutes", "0xtx1")
        assert heads(w)[0].startswith("🔑 **SN64 Chutes** — owner coldkey **swap transaction in the mempool**")
        e = w.sent[0]["embeds"][0]
        assert "PENDING" in e["title"] and "next block or two" in e["description"] and "mempool" in e["footer"]["text"]
        assert "found by state check" not in e["footer"]["text"]
        await w.swap_pending("5Chutes", "5NewChutes", "0xtx1")                # the same transaction seen again: still one card
        assert len(w.sent) == 1
        await w.swap_pending("5Chutes", "5NewChutes", "0xtx2")                # a different transaction: a new card
        assert len(w.sent) == 2
    asyncio.run(go())


def test_an_announced_swap_gets_a_second_card_the_moment_it_can_be_executed():
    async def go():
        w, meta = watch(BASE)
        await w.on_events(101, [("ColdkeySwapAnnounced", ("5Chutes", "0xabc"))])
        assert w.swaps["5Chutes"] == {"exec": 101 + 36000, "nets": [64], "notified": False}
        assert w.due_swaps(36100) == [] and w.due_swaps(36101) == ["5Chutes"]
        await w.post_due(w.due_swaps(36101), 36101)
        h = heads(w)
        assert h[-1] == "⏰ **SN64 Chutes** — the announced coldkey swap **can be executed now**"
        assert "CAN BE EXECUTED NOW" in w.sent[-1]["embeds"][0]["title"] and "#36101" in w.sent[-1]["embeds"][0]["description"]
        assert w.due_swaps(36500) == []                                       # told once
        await w.post_due(["5Chutes"], 36500)
        assert len(w.sent) == 2                                               # (the announcement card + this one)
        # it is remembered across a restart, with the "already told" flag
        w2 = SubnetWatch(meta, None, w.refresh, w.path, dry_run=True)
        meta.announced = {"5Chutes": 101 + 36000}
        await w2.start()
        assert w2.swaps["5Chutes"]["notified"] is True and w2.due_swaps(40000) == []
    asyncio.run(go())


def test_the_wait_card_does_not_come_when_the_swap_was_done_disputed_or_withdrawn():
    async def go():
        for ev in ("ColdkeySwapped", "ColdkeySwapDisputed", "ColdkeySwapReset", "ColdkeySwapCleared"):
            w, meta = watch(BASE)
            await w.on_events(101, [("ColdkeySwapAnnounced", ("5Chutes", "0xabc"))])
            assert "5Chutes" in w.swaps
            await w.on_events(200, [(ev, ("5Chutes", "5NewChutes") if ev == "ColdkeySwapped" else ("5Chutes",))])
            assert w.swaps == {}, ev
            assert w.due_swaps(99999) == []
    asyncio.run(go())


def test_the_chain_is_the_source_of_truth_for_pending_announcements():
    async def go():
        w, meta = watch(BASE)
        meta.announced = {"5Chutes": 50000, "5Nobody": 60000}                 # one real owner, one wallet that is nobody's
        await w._sync_swaps()
        assert w.swaps == {"5Chutes": {"exec": 50000, "nets": [64], "notified": False}}
        meta.announced = {}                                                   # it was cancelled meanwhile
        await w._sync_swaps()
        assert w.swaps == {}
    asyncio.run(go())



def test_a_new_identity_or_owner_tells_the_news_monitor_to_look_for_the_x_account_again():
    # 23 Oct: SN27 sold to Komms, renamed at 13:53 — its new X account (@KommsApp) was found only in the hourly round
    async def go():
        w, meta = watch(BASE)
        told = []
        w.on_change = lambda n, what: told.append((n, what))
        meta.next = {**BASE, 64: sub(64, "Komms", owner="5Chutes", url="https://komms.app")}
        await w.on_events(100, [("SubnetIdentitySet", 64)])
        assert told == [(64, "identity changed")]
        await w.on_events(101, [("SubnetIdentitySet", 64)])                           # set again, same values: nothing
        assert told == [(64, "identity changed")]
        meta.next = {**meta.subnets, 116: sub(116, "for sale", owner="5Buyer")}
        await w.on_events(102, [("ColdkeySwapped", (OLD_OWNER, "5Buyer"))])
        assert (116, "owner coldkey swapped") in told
    asyncio.run(go())



def test_a_new_subnet_names_the_subnet_removed_from_its_slot_not_itself():
    # 26 Oct, SN70: "Consted 2.0" removed at 01:37:50, "ghayn" registered at 01:40:24 — the card said "takes the slot of
    # ghayn": the view had already been refreshed with the newcomer when the NetworkAdded event came
    async def go():
        w, meta = watch(BASE)
        newcomer = sub(116, "ghayn", owner=NEW_OWNER, reg=9364303)
        w._gone[116] = (BASE[116], time.time() - 150)                       # "for sale" was removed 2.5 minutes ago
        await w._ev_NetworkAdded((116, 1), {**BASE, 116: newcomer}, {**BASE, 116: newcomer}, 9364303)
        assert heads(w)[-1] == "🆕 **NEW SUBNET** · **SN116 ghayn** registered — takes the slot of “for sale”"
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} lifecycle tests passed")
