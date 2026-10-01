"""Number / text formatting shared by Discord and the console."""
from __future__ import annotations

import math
import os

from .chain import RAO
from .config import BLOCK_SECONDS


def price(rao: int | float) -> str:
    """Alpha price in TAO with 4 significant digits (0.004300, 0.06500, 1.2340)."""
    p = rao / RAO
    if p <= 0:
        return "0"
    decimals = max(4, 3 - math.floor(math.log10(p)))
    return f"{p:.{decimals}f}"


def move(a_rao: float, b_rao: float) -> tuple[str, str, float]:
    """("0.01945", "0.02075", 6.684) — the % is computed from the two numbers as displayed, so
    anyone checking "A → B (X%)" with a calculator gets exactly X."""
    sa, sb = price(a_rao), price(b_rao)
    return sa, sb, (float(sb) / float(sa) - 1) * 100


def pct(x: float, signed: bool = True) -> str:
    if abs(x) >= 100:
        s = f"{x:+,.0f}%" if signed else f"{abs(x):,.0f}%"
    else:
        s = f"{x:+.2f}%" if signed else f"{abs(x):.2f}%"
    return s


def tao(x: float) -> str:
    if x >= 1000:
        return f"{x:,.0f}"
    if x >= 1:
        return f"{x:,.2f}"
    return f"{x:.4f}"


def tao_compact(x: float) -> str:
    """τ5.47K / τ1.23M / τ431.25 — the taomarketcap style."""
    if x >= 999_995:  # would round to 1000.00K
        return f"τ{x / 1_000_000:.2f}M"
    if x >= 999.995:  # would round to 1000.00
        return f"τ{x / 1_000:.2f}K"
    return f"τ{x:.2f}"


def usd(x: float) -> str:
    if x >= 1:
        return f"${x:,.2f}"
    if x >= 0.01:
        return f"${x:.3f}"
    return f"${x:.5f}"


def duration(blocks: int) -> str:
    s = max(blocks, 1) * BLOCK_SECONDS
    if s < 60:
        return f"{s}s"
    if s < 3600:
        m, r = divmod(s, 60)
        return f"{m}m" if not r else f"{m}m {r}s"
    h, r = divmod(s, 3600)
    return f"{h}h" if r < 60 else f"{h}h {r // 60}m"


def window_label(blocks: int) -> str:
    return "1 block" if blocks == 1 else f"{blocks} blocks"


def short(addr: str) -> str:
    return f"{addr[:5]}…{addr[-4:]}" if len(addr) > 12 else addr


# ── console colors ───────────────────────────────────────────────────────

_TTY = not os.getenv("NO_COLOR")  # pm2 logs render ANSI fine


def _c(code: str):
    return (lambda s: f"\033[{code}m{s}\033[0m") if _TTY else (lambda s: str(s))


green, red, yellow, cyan, dim, bold = _c("32"), _c("31"), _c("33"), _c("36"), _c("2"), _c("1")
bgreen, bred, byellow = _c("1;92"), _c("1;91"), _c("1;93")
