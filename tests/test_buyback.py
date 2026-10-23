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


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} buyback tests passed")
