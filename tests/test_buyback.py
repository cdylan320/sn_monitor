"""Run: .venv/bin/python tests/test_buyback.py — a subnet's team buying its own token back, found on chain."""
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.buyback import RAO, find  # noqa: E402
from snmon.lifecycle import SubnetWatch  # noqa: E402
from snmon.meta import SubnetInfo, Trade  # noqa: E402

OWNER = "5CqTmNfgDchxULD1bfoz8jvj9rDYSoq76kiq98oBUUEDpWqX"        # SN63's owner coldkey
OWNERS = {63: OWNER, 61: "5ECEsYL82fbXx9KfTZy7G2KXSurnScciJyAVMHZjekc8jUbn"}


def buy(net, tao, cold, sell=False):
    return Trade(1, net, not sell, tao, tao * 250, cold)


def test_the_chains_own_buyback_call_counts_whoever_makes_it():
    # add_stake_burn: AddStakeBurn {netuid, hotkey, amount, alpha} (and its StakeAdded, which is not counted twice)
    ev = [("AddStakeBurn", {"netuid": 63, "hotkey": "5Hot", "amount": 12 * RAO, "alpha": 3000 * RAO}),
          ("AlphaBurned", ("5Anyone", "5Hot", 3000 * RAO, 63))]
    out = find([buy(63, 12, "5Anyone")], ev, OWNERS, {}, 5.0)
    assert [(b.netuid, b.how, round(b.tao, 2)) for b in out] == [(63, "add_stake_burn", 12.0)]
    assert "burned" in out[0].text()
    assert find([], [("AddStakeBurn", (61, "5Hot", 2 * RAO, 500 * RAO))], OWNERS, {}, 5.0) == []   # under 5 τ


def test_the_owner_buying_its_own_subnet_counts_moving_stake_or_an_ordinary_big_buy_does_not():
    out = find([buy(63, 50, OWNER)], [], OWNERS, {}, 5.0)
    assert [(b.netuid, b.how, b.coldkey) for b in out] == [(63, "owner", OWNER)]
    moved = [buy(63, 50, OWNER, sell=True), buy(63, 50, OWNER)]           # to another validator: sold and bought
    assert find(moved, [], OWNERS, {}, 5.0) == []
    # 23 Oct 15:43:36: SN63's pump began with an unknown wallet's 74.77 τ buy — not a buyback
    assert find([buy(63, 74.77, "5Da5PCiBBkSFS9gYWziHBLvTVRyZox1TRUGbhdqS3ydGmC4w")], [], OWNERS, {}, 5.0) == []
    assert find([buy(61, 50, OWNER)], [], OWNERS, {}, 5.0) == []          # the owner of another subnet


def test_a_named_buyback_wallet_and_a_buy_burned_in_the_same_block_count():
    wallet = "5BuybackWalletOfRedTeam"
    assert [b.how for b in find([buy(61, 20, wallet)], [], OWNERS, {61: {wallet}}, 5.0)] == ["wallet"]
    burned = [("AlphaRecycled", ("5Team", "5Hot", 4000 * RAO, 61))]
    assert [b.how for b in find([buy(61, 20, "5Team")], burned, OWNERS, {}, 5.0)] == ["buy+burn"]
    assert find([buy(61, 20, "5Team")], [("AlphaBurned", ("5Team", "5Hot", 1, 62))], OWNERS, {}, 5.0) == []


def test_the_buyback_card():
    async def go():
        class Meta:
            subnets = {63: SubnetInfo(63, "Enigma", "", 2200.0, 1, None, None, OWNER)}
        w = SubnetWatch(Meta(), None, None, Path(tempfile.mkdtemp()) / "s.json", dry_run=True)
        w.prices[63] = 4_000_000
        await w.buyback(find([buy(63, 50, OWNER)], [], OWNERS, {}, 5.0)[0], 9346619)
        p = w.sent[-1]
        assert "💰 **BUYBACK** · **SN63 Enigma** — the owner bought 50.00 τ of its own token" in p["content"]
        assert p["embeds"][0]["title"] == "💰 BUYBACK · SN63"
        # every kind gets a card — 23 Oct 20:03, SN39: "exchange" was missing here, the card crashed (KeyError), only the
        # bots got the event
        from snmon.buyback import Buyback
        for how in ("add_stake_burn", "owner", "wallet", "buy+burn", "owner-funded", "exchange"):
            await w.buyback(Buyback(63, 120.0, how, "5Buyer", "paid with 500.00 τ from a wallet with 68,153 transactions"), 1)
            assert w.sent[-1]["embeds"][0]["title"] == "💰 BUYBACK · SN63"
        assert any(f["name"] == "Money" and "68,153" in f["value"] for f in w.sent[-1]["embeds"][0]["fields"])
    asyncio.run(go())



A = "5Da5PCiBBkSFS9gYWziHBLvTVRyZox1TRUGbhdqS3ydGmC4w"      # SN63's buyer at 15:43 (74.77 τ)
B = "5E6zdJowQkZ9ef5wp72MerXHwBs82BgtqEfH2mqAT1wHW7kf"      # … at 16:01 (497.5 τ)
HIVE = "5HiveMEoWPmQmBAb8v63bKPcFhgTGCmST1TVZNvPHSTKFLCv"   # "Tao Bridge", 49,330 transactions
HOT = "5FqqXKb9zonSNKbZhEuHYjCXnmPbX9tdzMCU2gx8gir8Z8a5"    # 68,146 transactions, no name: an exchange's hot wallet
POOL = "5EYCAe5jLQhn6ofDSw8LqPr86oRyDdKpKXDftdxZr1UHqWzq"   # SN63's pool (a pallet account)


def funding(rows, sells=()):
    import time
    from snmon.funding import Funding, TransferLog
    t = TransferLog(Path(tempfile.mkdtemp()) / "t.db")
    for when, src, dst, tao in rows:
        t.add(1, when, [(src, dst, tao)])
    for i in range(57):                                                         # the hot wallet pays many people a day
        t.add(1, time.time() - 3600, [(HOT, f"5Customer{i}", 5.0)])
    for when, cold, net, tao in sells:
        t.add(1, when, [], [(cold, net, tao)])
    names = {HIVE: (49330, "Tao Bridge"), HOT: (68146, ""), OWNER: (39, "Alpha Trader Exchange"), "5Friend": (12, ""),
             BOT: (6401, ""), "5FH3p333o7": (251, "")}

    async def account(addr):
        return names.get(addr, (3, ""))
    return Funding(t, account)


def test_a_big_buy_paid_by_an_exchange_or_a_bridge_is_a_buyback_like_sn63s():
    import time
    now = time.time()
    f = funding([(now - 4 * 86400, HIVE, A, 274.24), (now - 240, HOT, B, 500.0), (now - 100, B, POOL, 497.5)])

    async def go():
        a = await f.check(A, OWNER, 74.77)
        b = await f.check(B, OWNER, 497.5)
        assert a[0] == "exchange" and "Tao Bridge" in a[1]
        assert b[0] == "exchange" and "68,146 transactions that paid" in b[1]
        assert await f.check("5Nobody", OWNER, 80.0) is None                   # no money came in: an old balance
    asyncio.run(go())
    assert f.log.into(POOL, 0) == []                                            # pool movements are not kept


def test_money_from_the_owner_directly_or_through_one_wallet_is_a_buyback_a_persons_money_is_not():
    import time
    now = time.time()
    f = funding([(now - 3600, OWNER, "5Direct", 60.0), (now - 7200, OWNER, "5Mid", 200.0), (now - 3600, "5Mid", "5Via", 90.0),
                 (now - 600, "5Friend", "5Person", 100.0), (now - 600, HOT, "5Little", 10.0), (now - 600, "5Friend", "5Little", 90.0)])

    async def go():
        assert (await f.check("5Direct", OWNER, 55.0))[0] == "owner-funded"
        assert (await f.check("5Via", OWNER, 85.0))[0] == "owner-funded"
        assert await f.check("5Person", OWNER, 95.0) is None                   # a friend's money: a person, not a service
        assert await f.check("5Little", OWNER, 95.0) is None                   # 10 τ of exchange money cannot pay 95 τ
    asyncio.run(go())


def test_a_big_buy_is_new_money_not_moving_everything_from_other_subnets():
    new = [buy(63, 74.77, A)]
    assert find_big(new) == [(A, 63, 74.77)]
    allin = [buy(5, 120, "5Bot", sell=True), buy(63, 118, "5Bot")]           # our bots' all-in move: sold 5, bought 63
    assert find_big(allin) == []
    assert find_big([buy(63, 30, A)]) == []                                     # under 50 τ


def find_big(trades):
    from snmon.buyback import big_buys
    return [(c, n, round(t, 2)) for c, n, t in big_buys(trades, 50.0)]



BOT = "5Cw2qFy2tZy9ZLt4LsJc9b68nffQndZDHF1KZynmVbA4Z3pC"     # a follower bot: 6,401 transactions, pays ONE wallet


def test_a_busy_bot_paying_one_wallet_is_not_an_exchange_and_a_buyer_that_sold_lately_is_a_trader():
    # 23 Oct dry run: 15 of 98 big buys counted with "5,000+ transactions" alone — among them a follower bot's wallet,
    # and 5FUH8krjU3, which had bought 180 τ of SN63 at 15:48 and sold 194 τ at 16:02
    import time
    now = time.time()
    f = funding([(now - 900, BOT, "5Gb23Q7n11", 140.0), (now - 7200, HOT, "5FUH8krjU3", 458.4)],
                sells=[(now - 2700, "5FUH8krjU3", 63, 193.9)])

    async def go():
        assert await f.check("5Gb23Q7n11", None, 50.0, netuid=53) is None     # the bot's money is not an exchange's
        assert await f.check("5FUH8krjU3", OWNER, 100.0, netuid=63) is None   # sold SN63 45 minutes ago: a trader
        assert (await f.check("5FUH8krjU3", None, 100.0, netuid=39))[0] == "exchange"   # another subnet: still counts
    asyncio.run(go())


def test_an_active_wallet_is_a_whale_or_a_bot_not_a_teams_buyback_wallet():
    # 23 Oct: 5FH3p333o7 (251 transactions) bought SN64, SN4 and SN51 in 9 minutes with exchange money
    import time
    f = funding([(time.time() - 3600, HOT, "5FH3p333o7", 179.3)])

    async def go():
        assert await f.check("5FH3p333o7", None, 60.8, netuid=51) is None
    asyncio.run(go())



def test_an_exchange_funded_big_buy_is_only_shown_not_sent_to_the_bots_an_owner_funded_one_is_a_buyback():
    # 23 Oct 17:01, SN51: a new wallet's 50 τ buy paid by a 46,139-transaction wallet was posted as 💰 BUYBACK and traded;
    # the user: that is an exchange buy — show it ("it may be the owner buying back"), do not trade it
    import types
    from snmon.app import App
    from snmon.buyback import Buyback

    async def go():
        app = App.__new__(App)
        cards, events = [], []

        class Watch:
            async def exchange_buy(self, b, block):
                cards.append(("exchange", b.netuid))

            async def buyback(self, b, block):
                cards.append(("buyback", b.netuid))

        class Funding:
            def __init__(self, hit):
                self.hit = hit

            async def check(self, *a, **k):
                return self.hit

        class Feed:
            def publish(self, ev):
                events.append(ev)
                return 2
        app.subnets, app.news_feed = Watch(), Feed()
        app.det = types.SimpleNamespace(history={}, last_block=1)
        app.funding = Funding(("exchange", "paid with 141.36 τ from a wallet with 46,139 transactions that paid 24 wallets"))
        await app._big_buy("5GA1r", 51, 50.0, "5Owner", 9348808)
        assert cards == [("exchange", 51)] and events == []                     # shown, not sent to the bots
        app.funding = Funding(("owner-funded", "60.00 τ came straight from the owner wallet"))
        await app._big_buy("5Direct", 51, 55.0, "5Owner", 9348809)
        assert cards[-1] == ("buyback", 51) and events[-1]["kind"] == "buyback"
    asyncio.run(go())


def test_the_exchange_buy_card():
    async def go():
        from snmon.buyback import Buyback

        class Meta:
            subnets = {51: SubnetInfo(51, "lium.io", "", 184200.0, 1, None, None, "5Owner")}
        w = SubnetWatch(Meta(), None, None, Path(tempfile.mkdtemp()) / "s.json", dry_run=True)
        await w.exchange_buy(Buyback(51, 50.0, "exchange", "5GA1r", "paid with 141.36 τ from a wallet with 46,139 "
                                                                     "transactions that paid 24 wallets"), 9348808)
        p = w.sent[-1]
        assert "🏦 **EXCHANGE BUY** · **SN51 lium.io**" in p["content"] and "it may be the owner buying back" in p["content"]
        assert "BUYBACK" not in p["content"] and "Not traded" in p["embeds"][0]["description"]
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} buyback tests passed")
