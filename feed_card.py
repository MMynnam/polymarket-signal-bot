"""
feed_card.py — renders the recap card image for the audience feed (feed v2).

One function, one artifact: a 1200x675 dark PNG with the window's W-L record, net
dollars, and a 30-day cumulative-P&L curve — the shareable "scoreboard" moment of
the day. Pure presentation: callers pass already-computed numbers.

FAILURE PATH IS SACRED: any exception (Pillow missing, fonts unreadable, bad data)
returns None and the caller falls back to the plain-text recap. The feed must never
go silent because an image didn't render.

Fonts: bundled DejaVu TTFs in assets/ (deterministic on Railway); falls back to
Pillow's scalable default font if they're unreadable.
"""

from __future__ import annotations

import io
import logging
import os
from typing import Optional, Sequence

log = logging.getLogger("feed_card")

_ASSETS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")

# Palette — tuned for Telegram dark mode, readable in light mode too.
_BG       = (15, 23, 34)      # deep navy
_PANEL    = (22, 32, 46)      # card panel
_FG       = (236, 239, 244)   # near-white text
_DIM      = (124, 138, 158)   # secondary text
_GREEN    = (61, 220, 132)
_RED      = (255, 99, 99)
_ACCENT   = (255, 200, 87)    # brand amber
_GRIDLINE = (44, 58, 78)

W, H = 1200, 675
_M = 56  # outer margin


def _font(size: int, bold: bool = False):
    from PIL import ImageFont
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    try:
        return ImageFont.truetype(os.path.join(_ASSETS, name), size)
    except Exception:
        try:
            return ImageFont.load_default(size=size)
        except TypeError:  # very old Pillow: fixed-size bitmap default
            return ImageFont.load_default()


def _fmt_money(x: float) -> str:
    sign = "+" if x >= 0 else "−"  # true minus sign renders better than hyphen
    return f"{sign}${abs(x):.2f}"


def render_recap_card(
    *,
    title: str,
    date_str: str,
    wins: int,
    losses: int,
    net: float,
    curve: Sequence[tuple],          # [(unix_ts, cumulative_pnl), ...] ASC — 30d context
    lifetime_net: Optional[float],
    balance: Optional[float],
    curve_label: str = "last 30 days, realized",
) -> Optional[bytes]:
    """Render the recap card PNG → bytes, or None on ANY failure (caller falls back)."""
    try:
        return _render(title, date_str, wins, losses, net, list(curve or []),
                       lifetime_net, balance, curve_label)
    except Exception as exc:
        log.warning("[FeedCard] render failed (falling back to text): %s", exc)
        return None


def _render(title, date_str, wins, losses, net, curve, lifetime_net, balance, curve_label):
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (W, H), _BG)
    d = ImageDraw.Draw(img)

    # ---- header -----------------------------------------------------------
    d.text((_M, _M - 8), "V1 POLY", font=_font(28, bold=True), fill=_ACCENT)
    d.text((_M, _M + 30), title.upper(), font=_font(64, bold=True), fill=_FG)
    d.text((_M, _M + 108), date_str, font=_font(30), fill=_DIM)

    # ---- left column: the numbers ------------------------------------------
    col_x = _M
    y = 248
    rec_f = _font(58, bold=True)
    d.text((col_x, y), f"{wins}W", font=rec_f, fill=_GREEN)
    w_w = d.textlength(f"{wins}W", font=rec_f)
    d.text((col_x + w_w + 18, y), "–", font=rec_f, fill=_DIM)
    dash_w = d.textlength("–", font=rec_f)
    d.text((col_x + w_w + dash_w + 36, y), f"{losses}L", font=rec_f, fill=_RED)

    # win-rate bar
    y += 92
    total = max(wins + losses, 1)
    bar_w, bar_h = 380, 18
    fill_w = int(bar_w * wins / total)
    d.rounded_rectangle([col_x, y, col_x + bar_w, y + bar_h], radius=9, fill=_GRIDLINE)
    if fill_w > 8:
        d.rounded_rectangle([col_x, y, col_x + fill_w, y + bar_h], radius=9, fill=_GREEN)

    # net on the window
    y += 56
    d.text((col_x, y), "net", font=_font(28), fill=_DIM)
    d.text((col_x, y + 34), _fmt_money(net), font=_font(84, bold=True),
           fill=_GREEN if net >= 0 else _RED)

    # ---- footer strip -------------------------------------------------------
    fy = H - _M - 34
    bits = []
    if balance is not None:
        bits.append(f"bank ${balance:.2f}")
    if lifetime_net is not None:
        bits.append(f"all-time {_fmt_money(lifetime_net)}")
    bits.append("not financial advice. obviously.")
    d.text((_M, fy), "   ·   ".join(bits), font=_font(26), fill=_DIM)

    # ---- right panel: equity curve ------------------------------------------
    px0, py0, px1, py1 = 560, 230, W - _M, H - 150
    d.rounded_rectangle([px0 - 24, py0 - 56, px1 + 24, py1 + 44], radius=18, fill=_PANEL)
    d.text((px0, py0 - 44), curve_label, font=_font(24), fill=_DIM)

    pts = [(float(ts), float(v)) for ts, v in curve if ts is not None and v is not None]
    if len(pts) >= 2:
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        x_lo, x_hi = min(xs), max(xs)
        y_lo, y_hi = min(ys + [0.0]), max(ys + [0.0])
        if x_hi == x_lo:
            x_hi = x_lo + 1
        pad = max((y_hi - y_lo) * 0.12, 0.5)
        y_lo, y_hi = y_lo - pad, y_hi + pad

        def sx(x): return px0 + (x - x_lo) / (x_hi - x_lo) * (px1 - px0)
        def sy(y): return py1 - (y - y_lo) / (y_hi - y_lo) * (py1 - py0)

        # zero line
        zy = sy(0.0)
        if py0 <= zy <= py1:
            for gx in range(int(px0), int(px1), 18):
                d.line([(gx, zy), (gx + 9, zy)], fill=_GRIDLINE, width=2)
            d.text((px1 - 30, zy - 30), "$0", font=_font(22), fill=_DIM)

        line_pts = [(sx(x), sy(y)) for x, y in pts]
        end_up = pts[-1][1] >= 0
        d.line(line_pts, fill=_GREEN if end_up else _RED, width=5, joint="curve")

        # endpoint dot + value
        ex, ey = line_pts[-1]
        d.ellipse([ex - 8, ey - 8, ex + 8, ey + 8], fill=_GREEN if end_up else _RED)
        lbl = _fmt_money(pts[-1][1])
        lf = _font(30, bold=True)
        lw = d.textlength(lbl, font=lf)
        lx = min(ex + 16, px1 - lw)
        ly = max(ey - 44, py0)
        d.text((lx, ly), lbl, font=lf, fill=_FG)
    else:
        d.text((px0, (py0 + py1) // 2 - 16), "not enough tape yet",
               font=_font(28), fill=_DIM)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
