"""A subnet's team buying its own token back — found on chain, block by block, and sent to the bots.

The chain has a buyback call: `add_stake_burn` (call 132) buys a subnet's alpha with TAO and burns it at once — event
AddStakeBurn {netuid, hotkey, amount (TAO), alpha}; anyone may call it. Teams also buy and then burn or recycle what they
bought (AlphaBurned / AlphaRecycled (coldkey, hotkey, alpha, netuid) by the same coldkey in the same block), or buy and
keep it: the subnet owner's coldkey, or a buyback wallet the team named (NEWS_BUYBACK_WALLETS — RedTeam SN61 shows its
own on theredteam.io). An ordinary wallet's big buy is not a buyback.

23 Oct, SN63 (15:43) and SN61 (16:42): "there was a buyback" — on chain there was none: no add_stake_burn on any subnet
in 6 hours, no burn there, no owner buy; the pumps were ordinary buyers followed by a group of follower bots.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

RAO = 1_000_000_000
EVENTS = frozenset({"AddStakeBurn", "AlphaBurned", "AlphaRecycled"})


@dataclass
class Buyback:
    netuid: int
    tao: float           # TAO the team spent buying
    how: str             # "add_stake_burn" | "owner" | "wallet" | "buy+burn" | "owner-funded" | "exchange"
    coldkey: str         # who bought ("" when the event does not say)
    detail: str = ""     # where the money came from (funding.Funding.check)

    def text(self) -> str:
        return {"add_stake_burn": f"bought back {self.tao:,.2f} τ of its token and burned it (the chain's buyback)",
                "owner": f"the owner bought {self.tao:,.2f} τ of its own token",
                "wallet": f"the team's buyback wallet bought {self.tao:,.2f} τ",
                "buy+burn": f"{self.tao:,.2f} τ bought and burned in the same block",
                "owner-funded": f"a wallet funded by the owner bought {self.tao:,.2f} τ",
                "exchange": f"a new wallet bought {self.tao:,.2f} τ with money straight from an exchange / bridge — it may "
                            f"be the owner buying back"}[self.how]


def _fields(a, names: tuple[str, ...]) -> list:
    """An event's attributes in `names` order — the decoder gives a dict of named fields or a plain tuple."""
    if isinstance(a, dict):
        return [a.get(k) for k in names]
    return list(a) + [None] * (len(names) - len(a)) if isinstance(a, (list, tuple)) else [None] * len(names)


def big_buys(trades, min_tao: float) -> list[tuple[str, int, float]]:
    """(coldkey, netuid, τ) of each wallet that bought at least `min_tao` of one subnet in this block with NEW money — not
    when it sold about as much of other subnets in the same block (moving everything into one subnet, like our bots)."""
    net: dict[tuple[str, int], float] = defaultdict(float)
    sold: dict[str, float] = defaultdict(float)
    for t in trades:
        net[(t.coldkey, t.netuid)] += t.tao if t.buy else -t.tao
        if not t.buy:
            sold[t.coldkey] += t.tao
    out = []
    for (cold, n), tao in net.items():
        other = sold[cold] - sum(t.tao for t in trades if not t.buy and t.coldkey == cold and t.netuid == n)
        if n and tao >= min_tao and other < 0.5 * tao:
            out.append((cold, n, tao))
    return sorted(out, key=lambda r: -r[2])


def find(trades, events, owners: dict[int, str], wallets: dict[int, set[str]], min_tao: float) -> list[Buyback]:
    """The buybacks in one block. `trades` = the block's stake events (meta.Trade: netuid, buy, tao, coldkey);
    `events` = its (name, attributes) of EVENTS; `owners` = netuid → owner coldkey; at least `min_tao` TAO each."""
    out: dict[int, Buyback] = {}
    burned: set[tuple[str, int]] = set()
    for name, a in events:
        if name == "AddStakeBurn":
            net, _hot, amount, _alpha = _fields(a, ("netuid", "hotkey", "amount", "alpha"))
            tao = int(amount or 0) / RAO
            if net and tao >= min_tao:
                b = out.setdefault(int(net), Buyback(int(net), 0.0, "add_stake_burn", ""))
                b.tao += tao
        elif name in ("AlphaBurned", "AlphaRecycled"):
            cold, _hot, _alpha, net = _fields(a, ("coldkey", "hotkey", "amount", "netuid"))
            if cold and net is not None:
                burned.add((str(cold), int(net)))
    # what each wallet bought on each subnet in this block, net of what it sold there (moving stake to another
    # validator is a sell and a buy of the same subnet — not a buyback)
    bought: dict[tuple[str, int], float] = defaultdict(float)
    for t in trades:
        bought[(t.coldkey, t.netuid)] += t.tao if t.buy else -t.tao
    for (cold, net), tao in sorted(bought.items(), key=lambda kv: -kv[1]):
        if not net or net in out or tao < min_tao:
            continue
        if cold == owners.get(net):
            how = "owner"
        elif cold in wallets.get(net, ()):
            how = "wallet"
        elif (cold, net) in burned:
            how = "buy+burn"
        else:
            continue
        out[net] = Buyback(net, tao, how, cold)
    return [out[n] for n in sorted(out)]
