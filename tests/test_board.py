"""Run: .venv/bin/python tests/test_board.py — the live board of the "only announced" channel (the user, 10 Nov)."""
import asyncio
import os
import sys
import tempfile
import types
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.announced_board import AnnouncedBoard  # noqa: E402
from snmon.meta import SubnetInfo  # noqa: E402
from snmon.regcost import RegCost  # noqa: E402

HEAD = 9472437


def _board():
    class Hook:
        def __init__(self):
            self.sent, self.edits, self.deleted = [], [], []

        async def send(self, payload, files=None):
            self.sent.append((payload, files))
            return f"m{len(self.sent)}"

        def edit_later(self, msg_id, payload, files=None):
            self.edits.append((msg_id, files))

        async def delete(self, msg_id):
            self.deleted.append(msg_id)

    class Meta:
        subnets = {26: SubnetInfo(26, "Perturb", "", 1300, 7000000, None, None, "5Own26"),
                   78: SubnetInfo(78, "waw", "", 86, 9471124, None, None, "5Own78")}

        async def first_emission(self, ns):
            return {n: None if n == 78 else 1 for n in ns}
    d = tempfile.mkdtemp()
    rc = RegCost(Path(d) / "r.json")
    rc.regs = []
    rc.add(9447194, 252.78, 1793970024, 99, "gani")
    rc.add(9471098, 85.94, 1794256920, 78, "waw")
    watch = types.SimpleNamespace(facts={"cost": 163.9, "subnets": 128, "limit": 128, "prune": 26}, announced={},
                                  swaps={"5Own26": {"exec": 9458406, "nets": [26], "notified": True},
                                         "5OwnX": {"exec": HEAD + 7200, "nets": [26], "notified": False}})
    return AnnouncedBoard(Hook(), watch, Meta(), rc, lambda: HEAD, Path(d) / "b.json")


def test_the_board_shows_the_price_the_swaps_and_the_start_calls_and_keeps_below_new_cards():
    b = _board()

    async def go():
        await b.refresh()
        payload, files = b.discord.sent[0]
        text = payload["embeds"][0]["description"]
        assert "Price now: **163.89 τ** — falls **1.79 τ an hour**" in text                  # 85.94 τ / 14,400 blocks
        assert "Last paid: SN78 waw **85.94 τ**" in text and "SN99 gani **252.78 τ**" in text
        assert "• **SN26 Perturb** — ⏰ **can be executed now**" in text and "can be executed <t:" in text
        assert "**🆕 Waiting for the start call** (1)\n• **SN78 waw** — registered <t:" in text
        assert "next to be removed: SN26 Perturb" in text and files and files["regcost.png"][:4] == b"\x89PNG"
        assert payload["embeds"][0]["image"]["url"] == "attachment://regcost.png"
        await b.refresh()                                       # a minute later: edited in place, no new chart
        assert len(b.discord.sent) == 1 and b.discord.edits[-1] == ("m1", None)
        b.poke(True)                                            # a card went to the channel: the board goes below it
        await b.refresh()
        assert b.discord.deleted == ["m1"] and len(b.discord.sent) == 2 and b.msg_id == "m2"
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} board tests passed")
