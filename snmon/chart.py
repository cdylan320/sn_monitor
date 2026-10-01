"""Candlestick chart PNG for trend alerts (Pillow, drawn at 2× and downsampled for smooth lines).

    ┌ SN80 · OpenRoboto   [24h DOWNTREND −12.1%]                τ0.02512 ┐
    │  candles … shaded trend window … amber fitted trend line ┄┄┄ now ▸│
    └ -72h            -48h            -24h                         now ┘
"""
from __future__ import annotations

import io
import math

import numpy as np
from PIL import Image, ImageDraw, ImageFont

from . import fmt

W, H, S = 1000, 460, 2          # output size, supersampling factor
PAD_L, PAD_R, PAD_T, PAD_B = 18, 92, 62, 36

BG = (22, 23, 26)
GRID = (42, 44, 49)
TEXT = (181, 186, 193)
TEXT_HI = (235, 237, 240)
UP = (34, 197, 94)
DOWN = (239, 68, 68)
TREND = (250, 204, 21)

_FONT_DIR = "/usr/share/fonts/truetype/dejavu/"


def _font(size: int, bold: bool = False):
    try:
        return ImageFont.truetype(_FONT_DIR + ("DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"), size * S)
    except OSError:
        return ImageFont.load_default(size * S)


def _p(v: float) -> str:
    return fmt.price(v * 1e9)


def _fade(col: tuple[int, int, int], k: float = 0.38) -> tuple[int, int, int]:
    return tuple(round(b + (x - b) * k) for x, b in zip(col, BG))


def render(o: np.ndarray, h: np.ndarray, lo: np.ndarray, c: np.ndarray, fit: np.ndarray, *,
           title: str, badge: str, direction: int, x_labels: list[tuple[int, str]],
           interval: str = "", exact: np.ndarray | None = None) -> bytes:
    """All arrays have one entry per candle; `fit` is NaN outside the trend window. Candles whose
    `exact` is False (sampled history, not every block) are drawn faded."""
    n = len(c)
    img = Image.new("RGB", (W * S, H * S), BG)
    d = ImageDraw.Draw(img)
    x0, x1, y0, y1 = PAD_L * S, (W - PAD_R) * S, PAD_T * S, (H - PAD_B) * S

    vals = np.concatenate([h[~np.isnan(h)], lo[~np.isnan(lo)], fit[~np.isnan(fit)]])
    vmin, vmax = float(vals.min()), float(vals.max())
    pad = (vmax - vmin) * 0.08 or vmax * 0.01
    vmin, vmax = vmin - pad, vmax + pad

    def Y(v: float) -> float:
        return y1 - (v - vmin) / (vmax - vmin) * (y1 - y0)

    step = (x1 - x0) / n

    def X(i: float) -> float:
        return x0 + (i + 0.5) * step

    # grid + price axis (labels that would sit under the current-price tag are skipped)
    small = _font(12)
    y_last = Y(float(c[~np.isnan(c)][-1]))
    for k in range(6):
        v = vmin + (vmax - vmin) * k / 5
        y = Y(v)
        d.line([(x0, y), (x1, y)], fill=GRID, width=S)
        if abs(y - y_last) > 16 * S:
            d.text((x1 + 10 * S, y), _p(v), font=small, fill=TEXT, anchor="lm")
    for i, label in x_labels:
        x = X(i)
        d.line([(x, y0), (x, y1)], fill=GRID, width=S)
        d.text((x, y1 + 10 * S), label, font=small, fill=TEXT, anchor="mt")

    # shaded trend window
    win = np.nonzero(~np.isnan(fit))[0]
    color = UP if direction > 0 else DOWN
    if len(win):
        layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
        ImageDraw.Draw(layer).rectangle([X(win[0]) - step / 2, y0, x1, y1], fill=color + (22,))
        img = Image.alpha_composite(img.convert("RGBA"), layer).convert("RGB")
        d = ImageDraw.Draw(img)

    # candles
    body = max(S, step * 0.62)
    for i in range(n):
        if math.isnan(c[i]):
            continue
        col = UP if c[i] >= o[i] else DOWN
        if exact is not None and not exact[i]:
            col = _fade(col)
        x = X(i)
        d.line([(x, Y(h[i])), (x, Y(lo[i]))], fill=col, width=max(1, S))
        top, bot = Y(max(o[i], c[i])), Y(min(o[i], c[i]))
        if bot - top < S:
            top, bot = top - S / 2, bot + S / 2
        d.rectangle([x - body / 2, top, x + body / 2, bot], fill=col)

    # fitted trend line (dark halo for contrast, then amber)
    pts = [(X(i), Y(fit[i])) for i in win]
    if len(pts) > 1:
        d.line(pts, fill=BG, width=7 * S, joint="curve")
        d.line(pts, fill=TREND, width=3 * S, joint="curve")

    # legend: candle size, what the amber line is, and what faded candles mean
    lx, ly = x0 + 12 * S, y0 + 14 * S
    if interval:
        d.text((lx, ly), f"{interval} candles", font=small, fill=TEXT_HI, anchor="lm")
        lx += d.textlength(f"{interval} candles", font=small) + 18 * S
    d.line([(lx, ly), (lx + 22 * S, ly)], fill=TREND, width=3 * S)
    d.text((lx + 30 * S, ly), "fitted trend line", font=small, fill=TEXT, anchor="lm")
    if exact is not None and not exact.all():
        lx += 30 * S + d.textlength("fitted trend line", font=small) + 18 * S
        d.rectangle([lx, ly - 6 * S, lx + 8 * S, ly + 6 * S], fill=_fade(UP))
        d.text((lx + 16 * S, ly), "faded = sampled price history (not every block)", font=small, fill=TEXT, anchor="lm")

    # current price: dashed line + tag on the axis
    last = float(c[~np.isnan(c)][-1])
    y = Y(last)
    xd = x0
    while xd < x1:
        d.line([(xd, y), (min(xd + 6 * S, x1), y)], fill=color, width=S)
        xd += 11 * S
    tag = _p(last)
    tw = d.textlength(tag, font=_font(12, True))
    d.rounded_rectangle([x1 + 4 * S, y - 11 * S, x1 + 4 * S + tw + 12 * S, y + 11 * S], radius=4 * S, fill=color)
    d.text((x1 + 10 * S, y), tag, font=_font(12, True), fill=(255, 255, 255), anchor="lm")

    # header: title, badge, price
    big = _font(19, True)
    d.text((x0, 30 * S), title, font=big, fill=TEXT_HI, anchor="lm")
    bx = x0 + d.textlength(title, font=big) + 14 * S
    bf = _font(13, True)
    bw = d.textlength(badge, font=bf)
    d.rounded_rectangle([bx, 18 * S, bx + bw + 20 * S, 42 * S], radius=6 * S, fill=color)
    d.text((bx + 10 * S, 30 * S), badge, font=bf, fill=(255, 255, 255), anchor="lm")
    d.text(((W - 14) * S, 30 * S), f"τ{_p(last)}", font=big, fill=TEXT_HI, anchor="rm")

    out = img.resize((W, H), Image.LANCZOS)
    buf = io.BytesIO()
    out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
