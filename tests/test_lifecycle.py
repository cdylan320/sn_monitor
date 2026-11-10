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


def test_chain_updates_go_to_their_own_channel_named_in_any_case():
    # 6 Nov: the user set Chain_Update_Hook_URL — chain updates there, the news channel keeps Discord and X news
    from unittest import mock
    from snmon import config
    base = {"PRICE_HOOK_URL": "https://d/price", "NEWS_WEB_HOOK_URL": "https://d/news"}
    with mock.patch.object(config, "load_dotenv", lambda *a, **k: None):
        for extra, want in (({"Chain_Update_Hook_URL": "https://d/chain"}, "https://d/chain"),
                            ({"CHAIN_UPDATE_HOOK_URL": "https://d/chain2"}, "https://d/chain2"),
                            ({"SUBNET_WEB_HOOK_URL": "https://d/old"}, "https://d/old"),
                            ({"Chain_Update_Hook_URL": "  "}, "https://d/news"),
                            ({}, "https://d/news")):
            with mock.patch.dict(os.environ, {**base, **extra}, clear=True):
                c = config.load()
                assert c.subnet_webhook_url == want and c.news_webhook_url == "https://d/news", (extra, c.subnet_webhook_url)


def test_buys_go_to_the_buy_channel_and_what_is_announced_also_to_the_announced_one():
    # 6 Nov, the user's split: chain updates · buybacks and exchange buys (Buy_HOOK_URL) · announced, not done yet
    # (Only_Announced_HOOK_URL: a swap announced / executable now, a new subnet before its start call, a queued registration)
    import types

    class Hook:
        def __init__(self):
            self.sent = []

        async def send(self, payload):
            self.sent.append(payload["embeds"][0]["title"])
    w, meta = watch({0: sub(0, "root"), 64: sub(64, "Chutes"), 70: sub(70, "Fugal", owner=NEW_OWNER)})
    w.dry_run, w.discord, w.buy_discord, w.announced_discord = False, Hook(), Hook(), Hook()
    buy = types.SimpleNamespace(netuid=64, tao=86.0, coldkey=OLD_OWNER, detail="", how="owner", text=lambda: "the owner bought 86 τ")

    async def go():
        await w.buyback(buy, 9432900)
        await w.exchange_buy(buy, 9432901)
        await w.on_events(9431616, [("ColdkeySwapAnnounced", (OLD_OWNER, "0xhash"))])
        await w.post_due([OLD_OWNER], 9467616)
        await w.on_events(9426442, [("FirstEmissionBlockNumberSet", (70, 9426443))])
        meta.next = {**meta.subnets, 116: sub(116, "Carbon", owner=NEW_OWNER, reg=9420559)}
        await w.on_events(9420559, [("NetworkAdded", (116, 1))])
    asyncio.run(go())
    assert w.buy_discord.sent == ["💰 BUYBACK · SN64", "🏦 EXCHANGE BUY · SN64"]
    assert w.announced_discord.sent == ["🔑 OWNER COLDKEY SWAP ANNOUNCED · SN64", "⏰ OWNER COLDKEY SWAP CAN BE EXECUTED NOW · SN64",
                                        "🆕 NEW SUBNET REGISTERED · SN116"]
    assert w.discord.sent == ["🔑 OWNER COLDKEY SWAP ANNOUNCED · SN64", "⏰ OWNER COLDKEY SWAP CAN BE EXECUTED NOW · SN64",
                              "🚀 EMISSIONS START · SN70", "🆕 NEW SUBNET REGISTERED · SN116"]   # no buys; the start call: done
    w.buy_discord, w.announced_discord, w.discord.sent = None, None, []                       # neither set: all in the chain one
    asyncio.run(w.buyback(buy, 9432902))
    assert w.discord.sent == ["💰 BUYBACK · SN64"]


def test_a_new_subnet_names_the_one_it_replaced_even_when_the_slot_already_shows_its_own_name():
    # 6 Nov, SN99: Thirty Spokes' identity was cleared (14:05:26), the chain then showed the queued newcomer's name "gani"
    # on the old registration — the card said "gani registered — takes the slot of “gani”"
    async def go():
        w, meta = watch({0: sub(0, "root"), 99: sub(99, "Thirty Spokes", reg=8000000)})
        w._remember(w.known)
        cleared = SubnetInfo(99, "gani", "a", 252.0, 8000000, None, None, OLD_OWNER, "5Hot",
                             {"name": "", "url": "", "github": "", "description": "", "logo": "", "discord": "", "contact": ""})
        meta.next = {0: sub(0, "root"), 99: cleared}
        await w.on_events(9447219, [("SubnetIdentityRemoved", (99,))])
        meta.next = {0: sub(0, "root"), 99: sub(99, "gani", owner=NEW_OWNER, reg=9447220)}
        await w.on_events(9447220, [("NetworkAdded", (99, 1))])
        assert heads(w)[-1] == "🆕 **NEW SUBNET** · **SN99 gani** registered — takes the slot of “Thirty Spokes”"
        e = w.sent[-1]["embeds"][0]
        assert "- Thirty Spokes (previous subnet in this slot)" in e["description"]
        assert any(f["name"] == "Start call" and "pending" in f["value"] for f in e["fields"])      # the user, 6 Nov
        assert json.loads(w.path.read_text())["names"]["99"] == [9447220, "gani"]                 # kept on disk
    import json
    asyncio.run(go())


def test_an_announced_swap_shows_when_it_is_expected():
    # the user, 6 Nov: "mention expected completed time — when the swap will happen" (5 days after the announcement)
    async def go():
        w, meta = watch(BASE)
        w.facts = await meta.facts()
        t0 = time.time()
        await w.on_events(9431616, [("ColdkeySwapAnnounced", (OLD_OWNER, "0xhash"))])
        f = {x["name"]: x["value"] for x in w.sent[-1]["embeds"][0]["fields"]}
        ts = int(f["Expected swap"].split("<t:")[1].split(":")[0])
        assert abs(ts - (t0 + 36000 * 12)) < 5 and "block #9467616" in f["Expected swap"]     # 5 days, 12 s a block
        await w.post_due([OLD_OWNER], 9467616)
        assert any(x["name"] == "Expected swap" and "any block now" in x["value"] for x in w.sent[-1]["embeds"][0]["fields"])
    asyncio.run(go())


def test_a_registration_card_shows_what_it_paid_next_to_the_last_one_and_announced_cards_are_marked_done():
    # the user, 10 Nov: "last subnet reg 252 τ → now registered with 86 τ"; the only-announced channel shows what is not
    # done yet — a card whose wait is over is edited to ✅ / ❌
    from snmon.regcost import RegCost

    class Hook:
        def __init__(self):
            self.sent, self.edits, self.n = [], [], 0

        async def send(self, payload):
            self.n += 1
            self.sent.append(payload)
            return str(self.n)

        def edit_later(self, msg_id, payload, files=None):
            self.edits.append((msg_id, payload))

    async def go():
        w, meta = watch({0: sub(0, "root"), 78: sub(78, "Umi", reg=8000000)})
        w.dry_run, w.discord, w.announced_discord = False, Hook(), Hook()
        w.regcost = RegCost(Path(tempfile.mkdtemp()) / "r.json")
        w.regcost.regs = []
        w.regcost.add(9447194, 252.78, 1793970024, 99, "gani")
        w.facts = await meta.facts()
        meta.next = {0: sub(0, "root")}
        await w.on_events(9471098, [("NetworkRemoved", (78,)), ("NetworkRegistrationQueued", dict(QUEUED, lock_amount=85_943_556_872))])
        queued = w.announced_discord.sent[-1]["embeds"][0]
        price = next(f["value"] for f in queued["fields"] if f["name"] == "Registration price")
        assert "last one **252.78 τ** (SN99 gani, <t:1793970024:d>) → this one **85.94 τ** (-66%)" == price
        assert "queued:" + NEW_OWNER in w.announced
        meta.next = {0: sub(0, "root"), 78: sub(78, "waw", owner=NEW_OWNER, reg=9471124)}
        await w.on_events(9471124, [("NetworkAdded", (78, 1))])
        added = w.announced_discord.sent[-1]["embeds"][0]
        assert any(f["name"] == "Registration price" and "this one **85.94 τ**" in f["value"] for f in added["fields"])
        assert w.regcost.last().netuid == 78 and w.regcost.last().name == "waw"
        done = w.announced_discord.edits[-1][1]["embeds"][0]                                  # the queued card: done
        assert done["title"].startswith("✅ ⏳ NEW SUBNET INCOMING") and "registered as SN78 waw" in done["fields"][-1]["value"]
        assert "start:78" in w.announced and "queued:" + NEW_OWNER not in w.announced
        await w.on_events(9480000, [("FirstEmissionBlockNumberSet", (78, 9480001))])
        start = w.announced_discord.edits[-1][1]["embeds"][0]
        assert start["title"].startswith("✅ 🆕") and not any(f["name"] == "Start call" for f in start["fields"])
        assert "start call made" in start["fields"][-1]["value"] and "start:78" not in w.announced
        # a swap announced, then done
        await w.on_events(9480100, [("ColdkeySwapAnnounced", (NEW_OWNER, "0xh"))])
        assert "swap:" + NEW_OWNER + ":78" in w.announced
        meta.next = {0: sub(0, "root"), 78: sub(78, "waw", owner=OLD_OWNER, reg=9471124)}
        await w.on_events(9516100, [("ColdkeySwapped", (NEW_OWNER, OLD_OWNER))])
        swapped = w.announced_discord.edits[-1][1]["embeds"][0]
        assert swapped["title"].startswith("✅ 🔑") and "swapped" in swapped["fields"][-1]["value"] and not w.announced
    asyncio.run(go())


def test_the_chain_reads_the_catch_up_and_the_board_use_really_run_on_the_worker():
    # 10 Nov: Meta.block_hash called self.run (no such thing) — the restart catch-up failed on every block, silently
    from snmon.meta import Meta, _Worker
    m = Meta.__new__(Meta)
    m._w = _Worker()

    async def block_hash(b):
        return f"0x{b}"

    async def first_emission(ns):
        return {n: None for n in ns}
    m._block_hash, m._first_emission = block_hash, first_emission

    async def go():
        assert await m.block_hash(9447199) == "0x9447199"
        assert await m.first_emission([78]) == {78: None}
    asyncio.run(go())


def test_after_a_restart_the_events_of_the_blocks_missed_are_read():
    # 6 Nov: SN99 was pruned during a restart (#9447199) — neither the removal nor the queued registration got a card
    async def go():
        w, meta = watch({0: sub(0, "root"), 99: sub(99, "Thirty Spokes", reg=8000000)})
        w.block_file.write_text("9447195")
        w._resume_after = 9447195
        read = []

        async def block_hash(b):
            return f"0x{b}"

        async def trades(b, h):
            read.append(b)
            life = [("NetworkRemoved", (99,)), ("NetworkRegistrationQueued", QUEUED)] if b == 9447199 else []
            return [], life, [], []
        meta.block_hash, meta.trades = block_hash, trades
        meta.next = {0: sub(0, "root")}
        w.checked(9447203)                                  # the first block read in this run
        await w.start()
        assert read == [9447196, 9447197, 9447198, 9447199, 9447200, 9447201, 9447202]
        assert [h.split(" · ")[0] for h in heads(w)][:2] == ["💀 **SUBNET DEREGISTERED**", "⏳ **NEW SUBNET INCOMING**"]
        assert "SN99 Thirty Spokes" in heads(w)[0] and int(w.block_file.read_text()) == 9447203
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} lifecycle tests passed")
