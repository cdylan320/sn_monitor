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


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} lifecycle tests passed")
