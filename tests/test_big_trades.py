"""Run: .venv/bin/python tests/test_big_trades.py — a landed sell of 20+ τ or buy of 50+ τ gets a card in the price channel."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon import alerts  # noqa: E402
from snmon.chain import RAO, Block  # noqa: E402
from snmon.meta import SubnetInfo, Trade  # noqa: E402

BOT26, HAKIMI, A, B = "5HGoL7fE" + "x" * 40, "5CXwAFZS" + "y" * 40, "5E6zdJow" + "z" * 40, "5FRv6CKv" + "w" * 40
INFO = SubnetInfo(48, "Quantum", "Q", 6890.0, 1, None, None)


def test_sells_of_20_and_buys_of_50_count_a_wallets_split_order_added_up_and_never_sn0():
    # 29 Oct, SN48: bot_26 sold 34.38 τ (#9389986), others bought 24.92, 15, 12.46 + 11 (two wallets) and 30 τ
    tr = [Trade(9389986, 48, False, 34.38, 4600, BOT26), Trade(9389986, 48, True, 24.92, 3300, A),
          Trade(9389986, 48, True, 30.0, 4000, HAKIMI), Trade(9389986, 48, True, 25.0, 3300, HAKIMI),   # 55 τ in one block
          Trade(9389986, 48, False, 19.99, 2600, A), Trade(9389986, 0, False, 500.0, 500, B),
          Trade(9389986, 64, True, 49.99, 900, B)]
    big = alerts.big_trades(tr, 20, 50)
    assert [(b.netuid, b.buy, round(b.tao, 2), b.coldkey) for b in big] == [(48, True, 55.0, HAKIMI), (48, False, 34.38, BOT26)]
    assert alerts.big_trades(tr, 0, 50)[0].buy and len(alerts.big_trades(tr, 0, 50)) == 1     # 0 = sells off
    assert alerts.big_trades(tr, 20, 50, watched=lambda n: n != 48) == []                       # an excluded subnet


def test_the_card_names_the_wallet_and_shows_the_blocks_price_move():
    b = alerts.BigTrade(48, False, 34.38, BOT26, "bot_26")
    line = alerts.big_trade_line(b, INFO, -0.27)
    assert line == "🔴 **SN48 Quantum** big SELL **34.38 τ** · bot_26 · -0.27% in the block"
    e = alerts.big_trades_embed([b], INFO, 9389986, int(0.007489 * RAO), int(0.007469 * RAO), 20, 50)
    assert e["title"] == "🔴 BIG SELL  ·  34.38 τ" and e["color"] == alerts.RED
    assert "bot_26 (5HGo" in e["description"] and "ss-ims.space/account/" + BOT26 in e["description"]
    assert e["fields"][0]["value"] == "0.007489 → 0.007469 τ  (**-0.27%**)" and e["fields"][1]["value"] == "6,890 τ"
    assert e["footer"]["text"] == "Block #9389986 · big trades: sells ≥ 20.00 τ · buys ≥ 50.00 τ"
    two = alerts.big_trades_embed([alerts.BigTrade(48, True, 55, HAKIMI), b], INFO, 9389986, None, None, 20, 50)
    assert two["title"] == "Big trades  ·  1 buy 55.00 τ · 1 sell 34.38 τ" and two["color"] == alerts.GREEN
    assert not two["fields"][:1] or two["fields"][0]["name"] == "Pool"                          # no prices: no move field


def test_one_message_a_block_with_a_card_per_subnet():
    from snmon.app import App

    class Discord:
        sent = []

        async def send(self, payload):
            self.sent.append(payload)

    class Meta:
        def info(self, n):
            return INFO if n == 48 else SubnetInfo(n, "Chutes", "C", 9000.0, 1, None, None)

        async def account(self, addr):
            return 7, "bot_26" if addr == BOT26 else ""

    class Det:
        history = {9389985: {48: int(0.007489 * RAO), 64: int(0.05 * RAO)}}

    class Cfg:
        big_trade_sell_tao, big_trade_buy_tao = 20.0, 50.0

    app = App.__new__(App)
    app.cfg, app.meta, app.det, app.big_discord, app._names, app.big_trade_count = Cfg(), Meta(), Det(), Discord(), {}, 0
    block = Block(9389986, "0x", {48: int(0.007469 * RAO), 64: int(0.051 * RAO)}, "x", "x", 0, 0, 0, None)
    big = [alerts.BigTrade(48, False, 34.38, BOT26), alerts.BigTrade(64, True, 80, A)]
    asyncio.run(app._big_trades(big, block))
    msg = Discord.sent[-1]
    assert len(Discord.sent) == 1 and len(msg["embeds"]) == 2 and msg["allowed_mentions"] == {"parse": []}
    assert "🔴 **SN48 Quantum** big SELL **34.38 τ** · bot_26 · -0.27% in the block" in msg["content"]
    assert "🟢 **SN64 Chutes** big BUY **80.00 τ** · 5E6z" in msg["content"] and "+2.00% in the block" in msg["content"]
    assert app.big_trade_count == 2 and app._names[BOT26][1] == "bot_26"


def test_a_stake_transfer_within_a_subnet_is_not_a_trade():
    # 30 Oct, #9392656: a wallet gave SN8's owner 39,000 α (transfer_stake) — the chain books StakeRemoved + StakeAdded,
    # the same τ and alpha, no fee; it was posted as "the owner bought 1,246.91 τ" (a buyback) and a big buy + big sell
    from snmon.meta import Meta

    def ev(name, cold, tao, alpha, netuid, fee=0, module="SubtensorModule"):
        return {"event": {"module_id": module, "event_id": name, "attributes": (cold, "HOT", tao, alpha, netuid, fee)}}

    events = [ev("StakeRemoved", "5EBzT", 1_246_914_090_583, 39_000 * RAO, 8),
              ev("StakeAdded", "OWNER8", 1_246_914_090_583, 39_000 * RAO, 8),
              {"event": {"module_id": "SubtensorModule", "event_id": "StakeTransferred", "attributes": ()}},
              ev("StakeRemoved", "SELLER", 20 * RAO, 3000 * RAO, 105, 0),          # a real sale with no fee …
              ev("StakeAdded", "BUYER", 20 * RAO, 3100 * RAO, 105, 0),             # … and a real buy right after
              ev("StakeRemoved", "A", 5 * RAO, 700 * RAO, 64, 1000),                # same amounts, but fees: trades
              ev("StakeAdded", "B", 5 * RAO, 700 * RAO, 64, 1000),
              ev("StakeRemoved", "C", 7 * RAO, 900 * RAO, 9),                       # not back to back: trades
              {"event": {"module_id": "System", "event_id": "ExtrinsicSuccess", "attributes": ()}},
              ev("StakeAdded", "D", 7 * RAO, 900 * RAO, 9)]

    class Sub:
        async def get_events(self, h):
            return events

    m = Meta.__new__(Meta)

    async def sub():
        return Sub()
    m._substrate = sub
    tr, *_ = asyncio.run(m._trades(9392656, "0x"))
    assert [(t.netuid, t.buy, t.coldkey) for t in tr] == [(105, False, "SELLER"), (105, True, "BUYER"), (64, False, "A"),
                                                          (64, True, "B"), (9, False, "C"), (9, True, "D")]


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} big trade tests passed")
