"""
self_assessment.py — the bot grades its OWN signaling, honestly, and PROPOSES weight
changes only when forward-validated (it never auto-applies anything).

Background: the 2026-06-13 edge audit proved, on clean CLOB-verified labels with strict
train/holdout discipline, that this signal has no exploitable edge — win rate tracks
price, wallet skill heterogeneity is zero, and every score component has rank-AUC ~0.5.
So the bot cannot tune its way to profit; pretending otherwise (a self-tuning weight
optimizer) would just fit noise — the exact trap that has burned this project before.

This routine is the honest version of "give the bot freedom to assess the roots of its
signaling": it looks at its OWN accumulating forward book (faithful era, after the
2026-06-11 side-fix) and asks, per score component, "does my high-component bucket win
MORE OFTEN than the price it pays?" (edge = win_rate − mean entry price, in pp — a bet at
price p has positive expected ROI iff this is > 0). It reports the truth to the operator,
and ONLY if a component clears a strict, walk-forward, cost-cleared bar does it emit a
weight-change PROPOSAL — for the operator to review and apply by hand. It changes nothing.

Survival framing: the bot's existence depends on turning a profit; this is it staring at
the roots of its own decisions and admitting what works. The honest answer, until the book
is much larger, is "nothing yet — surviving on discipline and the ratchet." That is the
truth, not a failure of the routine.

Pattern-matched to results_recap.py / digest.py; ops-routed (operator material, carries the
non-predictive score) via make_research_sender. Read-only over the existing tables.
"""

import asyncio
import html
import json
import logging
import math
import os
import statistics
from datetime import datetime, timedelta

import httpx

import config
import database
from alerter import make_research_sender

log = logging.getLogger("self_assessment")

# The side-resolution fix deploy (faithful era). Only fills after this executed the
# designed strategy; everything before had the token[0] side bug, so its labels are not
# the bot's true signaling. Matches forward_checkpoint.py.
FIX_TS: int = int(os.getenv("SELF_ASSESS_FIX_TS", "1781221337"))

# Cadence + honesty gates.
SELF_ASSESS_HOUR_UTC: int = int(os.getenv("SELF_ASSESS_HOUR_UTC", "13"))
SELF_ASSESS_EVERY_DAYS: int = int(os.getenv("SELF_ASSESS_EVERY_DAYS", "7"))   # weekly
_MIN_BOOK = 30            # below this, say nothing beyond "insufficient data"
_MIN_PER_BUCKET = 20      # per-component tercile minimum to report a component edge
# Proposal bar (deliberately hard — a real, durable, cost-cleared, walk-forward signal):
_PROPOSE_MIN_BOOK = 120   # need a real forward book before proposing anything
_PROPOSE_MIN_HALF = 40    # per time-half minimum for the walk-forward check
_PROPOSE_MIN_EDGE_PP = 4.0  # component top−bottom edge gap, in pp, after costs
_ONE_LEG_COST_PP = 1.0    # conservative per-leg execution cost (audit: ~+0.1pp avg)

_COMPONENTS = ["timing", "funding_velocity", "win_rate", "size_anomaly",
               "wallet_age", "concentration", "cluster_bonus"]


def _wilson(k: int, n: int, z: float = 1.96):
    if n == 0:
        return (0.0, 0.0)
    ph = k / n
    d = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / d
    h = z * math.sqrt((ph * (1 - ph) + z * z / (4 * n)) / n) / d
    return (c - h, c + h)


def _edge(rows):
    """(n, win_rate, mean_px, edge_pp) for a list of {won, px} dicts."""
    n = len(rows)
    if n == 0:
        return (0, 0.0, 0.0, 0.0)
    wr = sum(r["won"] for r in rows) / n
    mp = sum(r["px"] for r in rows) / n
    return (n, wr, mp, (wr - mp) * 100.0)


def _load_forward_book(db):
    """Faithful-era resolved executions joined to their score breakdown. One dict per bet:
    {won, px(entry), score, ts, comps{component: value}}. Read-only."""
    rows = db.execute(
        "SELECT te.bet_price_filled, te.bet_price_intended, te.resolution_status, "
        "       te.created_at, ao.score, ao.score_breakdown_json "
        "FROM trade_executions te JOIN alert_outcomes ao ON te.alert_id = ao.alert_id "
        "WHERE te.status IN ('filled','partial') "
        "  AND te.resolution_status IN ('won','lost') "
        "  AND te.created_at >= ?",
        (FIX_TS,),
    ).fetchall()
    book = []
    for px_f, px_i, status, ts, score, bj in rows:
        px = px_f or px_i
        if not px or not (0.0 < px < 1.0):
            continue
        comps = {}
        try:
            b = json.loads(bj or "{}")
            for c in _COMPONENTS:
                if isinstance(b.get(c), (int, float)):
                    comps[c] = float(b[c])
        except Exception:
            pass
        book.append({"won": 1 if status == "won" else 0, "px": float(px),
                     "score": score, "ts": int(ts or 0), "comps": comps})
    return book


def _component_edge(book, comp):
    """Top-tercile minus bottom-tercile edge (pp) for a component, price-controlled by the
    edge metric itself. Returns None if the component has too few/too-uniform values."""
    vals = sorted(r["comps"][comp] for r in book if comp in r["comps"])
    if len(vals) < 3 * _MIN_PER_BUCKET:
        return None
    lo_cut = vals[len(vals) // 3]
    hi_cut = vals[2 * len(vals) // 3]
    if hi_cut <= lo_cut:  # degenerate (constant component) — nothing to split on
        return None
    bottom = [r for r in book if r["comps"].get(comp, None) is not None and r["comps"][comp] <= lo_cut]
    top = [r for r in book if r["comps"].get(comp, None) is not None and r["comps"][comp] >= hi_cut]
    if len(bottom) < _MIN_PER_BUCKET or len(top) < _MIN_PER_BUCKET:
        return None
    _, _, _, e_top = _edge(top)
    _, _, _, e_bot = _edge(bottom)
    return {"comp": comp, "n_top": len(top), "n_bot": len(bottom),
            "edge_top": e_top, "edge_bot": e_bot, "gap": e_top - e_bot}


def _walk_forward_proposal(book):
    """A weight-change PROPOSAL, or None. STRICT: requires a real book, and a component
    whose top−bottom edge gap clears the cost-adjusted bar in BOTH time halves (so it
    isn't an in-sample artifact). Never auto-applies — returns text for the operator."""
    if len(book) < _PROPOSE_MIN_BOOK:
        return None
    ordered = sorted(book, key=lambda r: r["ts"])
    mid = len(ordered) // 2
    first, second = ordered[:mid], ordered[mid:]
    if len(first) < _PROPOSE_MIN_HALF or len(second) < _PROPOSE_MIN_HALF:
        return None
    bar = _PROPOSE_MIN_EDGE_PP + _ONE_LEG_COST_PP
    for comp in _COMPONENTS:
        whole = _component_edge(book, comp)
        h1 = _component_edge(first, comp)
        h2 = _component_edge(second, comp)
        if not (whole and h1 and h2):
            continue
        # Same direction in both halves AND clears the cost-adjusted bar in both.
        if (h1["gap"] >= bar and h2["gap"] >= bar) or (h1["gap"] <= -bar and h2["gap"] <= -bar):
            direction = "UP" if whole["gap"] > 0 else "DOWN (or invert/fade)"
            return (comp, direction, whole["gap"], h1["gap"], h2["gap"])
    return None


def format_self_assessment(book):
    """Honest ops report (HTML), or a short 'insufficient data' note. Returns (text, proposal_or_None)."""
    n = len(book)
    head = "🔎 <b>BOT SELF-ASSESSMENT</b> — <i>how's my signaling, really?</i>"
    if n < _MIN_BOOK:
        return (f"{head}\n\n"
                f"📉 <b>{n}</b> resolved bets in the faithful era — below the {_MIN_BOOK} I need to "
                f"say anything honest. Surviving on discipline (vig gate) + the vault ratchet.\n"
                f"<i>no edge claimed, none invented.</i>"), None

    _, wr, mp, edge = _edge(book)
    lo, hi = _wilson(sum(r["won"] for r in book), n)
    lines = [head, "",
             f"📊 <b>{n}</b> resolved · WR <b>{wr*100:.1f}%</b> vs entry <b>{mp*100:.1f}%</b> · "
             f"edge <b>{edge:+.1f}pp</b> (95% CI {(lo-mp)*100:+.1f}..{(hi-mp)*100:+.1f}pp)",
             ""]
    # Per-component read (only where the sample supports it).
    comp_lines = []
    for comp in _COMPONENTS:
        ce = _component_edge(book, comp)
        if ce:
            comp_lines.append(f"  • <b>{comp}</b>: high {ce['edge_top']:+.1f}pp vs low "
                              f"{ce['edge_bot']:+.1f}pp → gap <b>{ce['gap']:+.1f}pp</b> "
                              f"(n {ce['n_top']}/{ce['n_bot']})")
    if comp_lines:
        lines.append("component read (does a high bucket beat its price?):")
        lines.extend(comp_lines)
    else:
        lines.append("<i>not enough per-component sample to read individual signals yet.</i>")
    lines.append("")

    proposal = _walk_forward_proposal(book)
    if proposal:
        comp, direction, gw, g1, g2 = proposal
        lines.append(f"🧪 <b>PROPOSAL (review — NOT applied):</b> component <b>{comp}</b> shows a "
                     f"durable, cost-cleared edge gap (whole {gw:+.1f}pp; halves {g1:+.1f}/{g2:+.1f}pp). "
                     f"Consider weighting it {direction}. Validate once more before touching live weights.")
    else:
        lines.append("🧪 <b>Proposal:</b> none — nothing clears the forward-validated bar. "
                     "No weight change justified; the audit's no-edge finding still holds.")
    return "\n".join(lines), proposal


async def self_assessment_loop(dry_run: bool = False) -> None:
    """Weekly: post the bot's honest self-read to the ops/research channel."""
    log.info("[SelfAssess] Started (every %dd at %02d:00 UTC, dry_run=%s)",
             SELF_ASSESS_EVERY_DAYS, SELF_ASSESS_HOUR_UTC, dry_run)
    sender = make_research_sender()
    async with httpx.AsyncClient() as client:
        while True:
            now = datetime.utcnow()
            target = now.replace(hour=SELF_ASSESS_HOUR_UTC, minute=0, second=0, microsecond=0)
            # advance to the next day-of-cadence at the target hour
            while target <= now or (target.toordinal() % SELF_ASSESS_EVERY_DAYS) != 0:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            log.info("[SelfAssess] Next self-assessment at %s UTC (in %.1fh)",
                     target.strftime("%Y-%m-%d %H:%M"), wait / 3600)
            await asyncio.sleep(wait)
            try:
                book = _load_forward_book(database.get_db())
                text, proposal = format_self_assessment(book)
                if proposal:
                    log.warning("[SelfAssess] PROPOSAL emitted (review only): %s", proposal)
                if dry_run:
                    log.info("[SelfAssess] DRY RUN:\n%s", text)
                elif sender is None:
                    log.info("[SelfAssess] No research channel — assessment logged only:\n%s", text)
                else:
                    await sender.send_message(text, client)
                    log.info("[SelfAssess] Posted self-assessment (book n=%d)", len(book))
            except Exception as exc:
                log.exception("[SelfAssess] Failed to build/post self-assessment: %s", exc)
