"""Slow-path chain reads that need full SCALE decoding (names, logos, liquidity, events,
mempool extrinsics). Runs on its own connection so it never competes with the hot path."""
from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field

from async_substrate_interface import AsyncSubstrateInterface
from scalecodec.base import ScaleBytes

from .chain import RAO

log = logging.getLogger("snmon.meta")
for noisy in ("async_substrate_interface", "websockets", "bittensor"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp")

# SubtensorModule events about the subnets themselves (registration, removal, ownership, identity …)
LIFECYCLE_EVENTS = frozenset({
    "NetworkAdded", "NetworkRemoved", "NetworkRegistrationQueued", "NetworkRegistrationCancelled",
    "DissolveNetworkScheduled", "ColdkeySwapAnnounced", "ColdkeySwapped", "ColdkeySwapDisputed",
    "ColdkeySwapReset", "ColdkeySwapCleared", "SubnetOwnerChanged", "SubnetOwnerHotkeySet",
    "SubnetIdentitySet", "SubnetIdentityRemoved", "SymbolUpdated", "FirstEmissionBlockNumberSet",
    "SubnetLeaseCreated", "SubnetLeaseTerminated", "SubnetLimitSet",
})


def _text(v) -> str:
    if isinstance(v, (list, tuple)):
        try:
            return bytes(v).decode("utf-8", "replace").strip("\x00 ").strip()
        except (ValueError, TypeError):
            return ""
    return (v or "").strip() if isinstance(v, str) else ""


@dataclass
class SubnetInfo:
    netuid: int
    name: str
    symbol: str
    pool_tao: float        # TAO reserve in the pool (liquidity)
    registered_at: int
    logo: str | None
    url: str | None
    owner: str = ""            # owner coldkey
    owner_hotkey: str = ""
    identity: dict = field(default_factory=dict)  # name, url, github, description, logo, discord, contact

    @property
    def label(self) -> str:
        return f"SN{self.netuid} {self.name}".strip()


@dataclass
class Trade:
    block: int
    netuid: int
    buy: bool
    tao: float
    alpha: float
    coldkey: str


class _Worker:
    """A private event loop in a thread, so heavy SCALE decoding never stalls the hot path."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, name="snmon-meta", daemon=True).start()

    def run(self, coro) -> asyncio.Future:
        return asyncio.wrap_future(asyncio.run_coroutine_threadsafe(coro, self.loop))


class Meta:
    """Public methods return awaitables that execute on the worker thread."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.subnets: dict[int, SubnetInfo] = {}
        self._sub: AsyncSubstrateInterface | None = None
        self._lock: asyncio.Lock | None = None
        self._runtime = None
        self._w = _Worker()

    async def _substrate(self) -> AsyncSubstrateInterface:
        if self._sub is None:
            sub = AsyncSubstrateInterface(self.url, ss58_format=42, chain_name="Bittensor")
            await sub.initialize()
            self._sub = sub
        return self._sub

    async def _reset(self) -> None:
        sub, self._sub, self._runtime = self._sub, None, None
        if sub is not None:
            try:
                await sub.close()
            except Exception:
                pass

    def info(self, netuid: int) -> SubnetInfo:
        return self.subnets.get(netuid) or SubnetInfo(netuid, "", "", 0.0, 0, None, None)

    async def _refresh(self, block_hash: str | None = None) -> list[int]:
        """Reload names/liquidity (at the chain head, or at `block_hash` for replays). Returns netuids
        whose registration changed (re-registered)."""
        try:
            sub = await self._substrate()
            res = await sub.runtime_call("SubnetInfoRuntimeApi", "get_all_dynamic_info", block_hash=block_hash)
        except Exception:
            await self._reset()
            raise
        rows = getattr(res, "value", res) or []
        fresh: dict[int, SubnetInfo] = {}
        for r in rows:
            if not r:
                continue
            ident = r.get("subnet_identity") or {}
            logo = _text(ident.get("logo_url")) or None
            if logo and not logo.lower().split("?")[0].endswith(IMG_EXT):
                logo = None  # Discord can't render SVG
            url = _text(ident.get("subnet_url")) or None
            if url and not url.startswith("http"):
                url = "https://" + url
            n = int(r["netuid"])
            fresh[n] = SubnetInfo(
                netuid=n,
                name=_text(ident.get("subnet_name")) or _text(r.get("subnet_name")),
                symbol=_text(r.get("token_symbol")),
                pool_tao=int(r.get("tao_in") or 0) / RAO,
                registered_at=int(r.get("network_registered_at") or 0),
                logo=logo,
                url=url,
                owner=str(r.get("owner_coldkey") or ""),
                owner_hotkey=str(r.get("owner_hotkey") or ""),
                identity={"name": _text(ident.get("subnet_name")), "url": _text(ident.get("subnet_url")),
                          "github": _text(ident.get("github_repo")), "description": _text(ident.get("description")),
                          "logo": _text(ident.get("logo_url")), "discord": _text(ident.get("discord")),
                          "contact": _text(ident.get("subnet_contact"))},
            )
        changed = [n for n, s in fresh.items() if n in self.subnets and self.subnets[n].registered_at != s.registered_at]
        self.subnets = fresh
        return changed

    # ── events → trades (for "what caused it") ───────────────────────────

    async def _trades(self, block: int, block_hash: str) -> tuple[list[Trade], list[tuple[str, tuple]]]:
        """One decode of the block's events → (stake trades, subnet lifecycle events)."""
        sub = await self._substrate()
        try:
            events = await sub.get_events(block_hash)
        except Exception:
            await self._reset()
            raise
        out: list[Trade] = []
        life: list[tuple[str, tuple]] = []
        for e in events:
            ev = e.get("event", e)
            if ev.get("module_id") != "SubtensorModule":
                continue
            name = ev.get("event_id")
            if name in ("StakeAdded", "StakeRemoved"):
                # (coldkey, hotkey, tao, alpha, netuid, fee) — swaps/moves also emit these
                cold, _hot, tao, alpha, netuid, *_ = ev["attributes"]
                out.append(Trade(block, int(netuid), name == "StakeAdded", int(tao) / RAO, int(alpha) / RAO, str(cold)))
            elif name in LIFECYCLE_EVENTS:
                life.append((name, ev.get("attributes")))  # a tuple, a dict of named fields, or one bare value
        return out, life

    async def _facts(self) -> dict:
        """Slot usage, the cost to register a subnet, and which subnet is first in line to be pruned."""
        sub = await self._substrate()

        async def storage(item):
            r = await sub.query("SubtensorModule", item, [])
            return getattr(r, "value", r)

        async def api(name, method):
            r = await sub.runtime_call(name, method)
            return getattr(r, "value", r)

        total, limit, cost, prune, swap_delay = await asyncio.gather(
            storage("TotalNetworks"), storage("SubnetLimit"),
            api("SubnetRegistrationRuntimeApi", "get_network_registration_cost"),
            api("SubnetInfoRuntimeApi", "get_subnet_to_prune"), storage("ColdkeySwapAnnouncementDelay"),
            return_exceptions=True)

        def ok(v, default=None):
            return default if isinstance(v, BaseException) else v

        return {"subnets": max(int(ok(total, 0) or 0) - 1, 0),  # TotalNetworks counts root
                "limit": int(ok(limit, 0) or 0), "cost": int(ok(cost, 0) or 0) / RAO,
                "prune": ok(prune), "swap_delay": int(ok(swap_delay, 0) or 0)}

    # ── mempool decoding ─────────────────────────────────────────────────

    async def _decode_extrinsic(self, hex_ext: str) -> dict | None:
        sub = await self._substrate()
        if self._runtime is None:
            self._lock = self._lock or asyncio.Lock()
            async with self._lock:
                if self._runtime is None:
                    self._runtime = await sub.init_runtime()
        rt = self._runtime
        obj = rt.runtime_config.create_scale_object("Extrinsic", metadata=rt.metadata)
        obj.decode(ScaleBytes(hex_ext))
        return obj.value

    def runtime_changed(self) -> None:
        self._runtime = None

    # ── thread-hopping wrappers (call these from the main loop) ──────────

    def refresh(self, block_hash: str | None = None):
        return self._w.run(self._refresh(block_hash))

    def trades(self, block: int, block_hash: str):
        """→ (trades, lifecycle events) for one block."""
        return self._w.run(self._trades(block, block_hash))

    def facts(self):
        return self._w.run(self._facts())

    def decode_extrinsic(self, hex_ext: str):
        return self._w.run(self._decode_extrinsic(hex_ext))

    def stake_of(self, coldkey: str, hotkey: str, netuid: int):
        return self._w.run(self._stake_of(coldkey, hotkey, netuid))

    def stakes_on_hotkey(self, coldkey: str, hotkey: str):
        return self._w.run(self._stakes_on_hotkey(coldkey, hotkey))

    def storage_keys(self, netuid: int):
        """(System.Number, SubnetTAO, SubnetAlphaIn, SwapBalancer) storage keys for one subnet."""
        return self._w.run(self._storage_keys(netuid))

    async def _storage_keys(self, netuid: int) -> tuple[str, str, str, str]:
        sub = await self._substrate()
        out = []
        for mod, item, params in (("System", "Number", []), ("SubtensorModule", "SubnetTAO", [netuid]),
                                  ("SubtensorModule", "SubnetAlphaIn", [netuid]), ("Swap", "SwapBalancer", [netuid])):
            out.append((await sub.create_storage_key(mod, item, params)).to_hex())
        return tuple(out)

    async def _stake_of(self, coldkey: str, hotkey: str, netuid: int) -> int:
        """Alpha (rao) a coldkey has on a hotkey in a subnet."""
        sub = await self._substrate()
        res = await sub.runtime_call(
            "StakeInfoRuntimeApi", "get_stake_info_for_hotkey_coldkey_netuid", [hotkey, coldkey, netuid]
        )
        v = getattr(res, "value", res) or {}
        return int(v.get("stake") or 0)

    async def _stakes_on_hotkey(self, coldkey: str, hotkey: str) -> dict[int, int]:
        """netuid → alpha (rao) for every subnet where coldkey stakes on hotkey."""
        sub = await self._substrate()
        res = await sub.runtime_call("StakeInfoRuntimeApi", "get_stake_info_for_coldkey", [coldkey])
        out: dict[int, int] = {}
        for s in getattr(res, "value", res) or []:
            if str(s.get("hotkey")) == hotkey and int(s.get("stake") or 0) > 0:
                out[int(s["netuid"])] = out.get(int(s["netuid"]), 0) + int(s["stake"])
        return out
