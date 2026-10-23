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
    asyncio.run(go())



A = "5Da5PCiBBkSFS9gYWziHBLvTVRyZox1TRUGbhdqS3ydGmC4w"      # SN63's buyer at 15:43 (74.77 τ)
B = "5E6zdJowQkZ9ef5wp72MerXHwBs82BgtqEfH2mqAT1wHW7kf"      # … at 16:01 (497.5 τ)
HIVE = "5HiveMEoWPmQmBAb8v63bKPcFhgTGCmST1TVZNvPHSTKFLCv"   # "Tao Bridge", 49,330 transactions
HOT = "5FqqXKb9zonSNKbZhEuHYjCXnmPbX9tdzMCU2gx8gir8Z8a5"    # 68,146 transactions, no name: an exchange's hot wallet
POOL = "5EYCAe5jLQhn6ofDSw8LqPr86oRyDdKpKXDftdxZr1UHqWzq"   # SN63's pool (a pallet account)


def funding(rows):
    from snmon.funding import Funding, TransferLog
    t = TransferLog(Path(tempfile.mkdtemp()) / "t.db")
    for when, src, dst, tao in rows:
        t.add(1, when, [(src, dst, tao)])
    names = {HIVE: (49330, "Tao Bridge"), HOT: (68146, ""), OWNER: (39, "Alpha Trader Exchange"), "5Friend": (12, "")}

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
        assert b[0] == "exchange" and "68,146 transactions" in b[1]
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


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} buyback tests passed")
