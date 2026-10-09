"""Settings, read once from .env. Every knob has a sane default; only the webhook is required."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
BLOCK_SECONDS = 12

DEFAULT_ENDPOINTS = (
    "wss://lite.chain.opentensor.ai:443",
    "wss://archive.chain.opentensor.ai:443",
    "wss://entrypoint-finney.opentensor.ai:443",
    "wss://bittensor-finney.api.onfinality.io/public-ws",
)
DEFAULT_ARCHIVE = "wss://archive.chain.opentensor.ai:443"
# Mempool polling gets its own connections, on nodes that don't throttle author_pendingExtrinsics
# (lite/finney cap it at ~1 req/s per connection; these two handle 2+/s).
DEFAULT_MEMPOOL_ENDPOINTS = (
    "wss://archive.chain.opentensor.ai:443",
    "wss://bittensor-finney.api.onfinality.io/public-ws",
)
# blocks:percent — "alert if price moves >= percent within that many blocks" (1 block = 12s)
DEFAULT_WINDOWS = "1:2,5:3,25:4,300:7"


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v and v.strip() else default


def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default


def _list(name: str, default: tuple[str, ...]) -> list[str]:
    v = os.getenv(name)
    if not v or not v.strip():
        return list(default)
    return [x.strip() for x in v.split(",") if x.strip()]


def _netuids(name: str) -> set[int]:
    out: set[int] = set()
    for part in (os.getenv(name) or "").replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return out


def _windows(raw: str) -> list[tuple[int, float]]:
    out = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        blocks, pct = part.split(":")
        out.append((int(blocks), float(pct)))
    if not out:
        raise ValueError("WINDOWS is empty")
    return sorted(out)


@dataclass(frozen=True)
class Config:
    price_webhook_url: str   # PRICE_HOOK_URL — pump/dump and pending-trade alerts
    endpoints: list[str]
    archive_endpoint: str
    windows: list[tuple[int, float]]
    realert_step_pct: float
    episode_ttl_blocks: int
    mempool: bool
    mempool_min_pct: float
    mempool_poll_ms: int
    mempool_endpoints: list[str]
    min_pool_tao: float
    mention_pct: float
    mention: str
    startup_message: bool
    include: set[int]
    exclude: set[int]
    backfill_blocks: int
    dry_run: bool
    trend_webhook_url: str
    trend_timeframes: str
    trend_story_hours: float
    trend_history_endpoint: str
    trend_board_seconds: float
    trend_signals: bool
    trend_signal_dump_pct: float
    trend_signal_window: str
    trend_signal_bounce_pct: float
    trend_signal_sharp_pct: float
    trend_signal_tiny_pct: float
    trend_signal_flow_tao: float
    trend_signal_flow_min_drop: float
    subnet_alerts: bool
    subnet_webhook_url: str
    news_webhook_url: str
    news_discord_token: str
    news_guild: str
    news_min_team_chars: int
    news_x_accounts: list[str]
    news_x_poll_seconds: float

    @property
    def max_window(self) -> int:
        return max(w for w, _ in self.windows)

    def watched(self, netuid: int) -> bool:
        if netuid == 0 or netuid in self.exclude:
            return False
        return not self.include or netuid in self.include


def load() -> Config:
    load_dotenv(ROOT / ".env")
    # PRICE_HOOK_URL is the name; the older WEB_HOOK_URL still works so an old .env doesn't break
    webhook = next((v.strip() for v in (os.getenv(k) for k in ("PRICE_HOOK_URL", "PRICE_WEB_HOOK_URL", "WEB_HOOK_URL",
                                                               "DISCORD_WEBHOOK_URL")) if v and v.strip()), "")
    if not webhook:
        raise SystemExit("PRICE_HOOK_URL is missing in .env")
    news_webhook = (os.getenv("NEWS_WEB_HOOK_URL") or "").strip()
    windows = _windows(os.getenv("WINDOWS") or DEFAULT_WINDOWS)
    return Config(
        price_webhook_url=webhook,
        endpoints=_list("RPC_ENDPOINTS", DEFAULT_ENDPOINTS),
        archive_endpoint=(os.getenv("ARCHIVE_ENDPOINT") or DEFAULT_ARCHIVE).strip(),
        windows=windows,
        realert_step_pct=_float("REALERT_STEP_PCT", 3.0),
        episode_ttl_blocks=max(1, round(_float("EPISODE_TTL_MIN", 15) * 60 / BLOCK_SECONDS)),
        mempool=_bool("MEMPOOL_ALERTS", True),
        mempool_min_pct=_float("MEMPOOL_MIN_PCT", windows[0][1]),
        mempool_poll_ms=_int("MEMPOOL_POLL_MS", 200),
        mempool_endpoints=_list("MEMPOOL_ENDPOINTS", DEFAULT_MEMPOOL_ENDPOINTS),
        min_pool_tao=_float("MIN_POOL_TAO", 0.0),
        mention_pct=_float("MENTION_PCT", 0.0),
        mention=(os.getenv("MENTION") or "").strip(),
        startup_message=_bool("STARTUP_MESSAGE", True),
        include=_netuids("ONLY_NETUIDS"),
        exclude=_netuids("EXCLUDE_NETUIDS"),
        backfill_blocks=_int("BACKFILL_BLOCKS", max(300, max(w for w, _ in windows))),
        dry_run=_bool("DRY_RUN", False),
        trend_webhook_url=(os.getenv("TREND_WEB_HOOK_URL") or "").strip(),
        trend_timeframes=(os.getenv("TREND_TIMEFRAMES") or "1h:2:0.85:4:0.9,3h:3.5:0.8:5:0.85,6h:6:0.8,12h:8:0.7,24h:10:0.65,3d:12:0.6").strip(),
        trend_story_hours=_float("TREND_STORY_HOURS", 12.0),
        # must allow state_queryStorage (every-block history for exact charts); public opentensor nodes don't
        trend_history_endpoint=(os.getenv("TREND_HISTORY_ENDPOINT")
                                or "wss://bittensor-finney.api.onfinality.io/public-ws").strip(),
        trend_board_seconds=_float("TREND_BOARD_SECONDS", 60.0),
        trend_signals=_bool("TREND_SIGNALS", True),
        trend_signal_dump_pct=_float("TREND_SIGNAL_DUMP_PCT", 5.0),
        # window:min %:steadiness — the turn up that fires a reversal signal
        trend_signal_window=(os.getenv("TREND_SIGNAL_WINDOW") or "15m:2:0.8").strip(),
        trend_signal_bounce_pct=_float("TREND_SIGNAL_BOUNCE_PCT", 3.0),
        trend_signal_sharp_pct=_float("TREND_SIGNAL_SHARP_DUMP_PCT", 8.0),
        trend_signal_tiny_pct=_float("TREND_SIGNAL_TINY_PUMP_PCT", 1.0),
        trend_signal_flow_tao=_float("TREND_SIGNAL_DUMP_FLOW_TAO", 0.0),
        trend_signal_flow_min_drop=_float("TREND_SIGNAL_DUMP_FLOW_MIN_DROP_PCT", 3.0),
        subnet_alerts=_bool("SUBNET_ALERTS", True),
        # subnet lifecycle alerts go to the news channel (their own webhook if set; price channel as last resort)
        subnet_webhook_url=(os.getenv("SUBNET_WEB_HOOK_URL") or "").strip() or news_webhook or webhook,
        news_webhook_url=news_webhook,
        news_discord_token=(os.getenv("NEWS_DISCORD_TOKEN") or "").strip().strip('"'),
        news_guild=(os.getenv("NEWS_GUILD_ID") or "").strip(),
        news_min_team_chars=_int("NEWS_MIN_TEAM_CHARS", 120),
        news_x_accounts=[h.lstrip("@") for h in _list("NEWS_X_ACCOUNTS", ("const_reborn",))],
        news_x_poll_seconds=_float("NEWS_X_POLL_SECONDS", 20.0),
    )
