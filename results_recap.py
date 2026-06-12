"""
results_recap.py — Daily spectator results recap to Telegram (content/entertainment only).

Posts once per day at RECAP_SEND_HOUR_UTC summarizing the prior RECAP_WINDOW_HOURS of
resolved trades — W-L record + streak, biggest win, biggest loss, wildest call, net
notional P&L, free wallet balance, and open positions — in the same banter register as
the trade-resolution streak callouts. Read-only over the existing trade_executions table
(no new instrumentation); free balance via a raw eth_call (no web3 dependency).

Pattern-matched to digest.py's digest_loop; wired as a supervised task in main.py. This
module touches no trading/sizing/sweep logic.
"""

import asyncio
import html
import logging
import os
from datetime import datetime, timedelta

import httpx

import config
import database
from alerter import TelegramSender

try:
    from feed_card import render_recap_card
except Exception:  # pillow/asset trouble must never take the recap down
    def render_recap_card(**_kw):
        return None

log = logging.getLogger("results_recap")

# ~09:00 Stockholm (CEST) — a morning "how'd yesterday go" ritual that captures
# overnight/late sports resolutions. Change via env without a redeploy.
RECAP_SEND_HOUR_UTC: int = int(os.getenv("RECAP_SEND_HOUR_UTC", "7"))
RECAP_WINDOW_HOURS: int = int(os.getenv("RECAP_WINDOW_HOURS", "24"))

# Funder proxy (holds pUSD) + pUSD collateral, for the free-balance line.
_FUNDER = "0x00BD1F45caAFd08a1FFfEABa7e17c712a8791e9E"
_PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
_FALLBACK_RPC = "https://polygon-bor-rpc.publicnode.com"

_COLS = ["market_question", "bet_side", "bet_price_filled", "bet_price_intended",
         "pnl", "resolution_status", "resolved_at"]


async def _free_balance(client: httpx.AsyncClient):
    """pUSD balanceOf the funder via raw eth_call. Returns float or None."""
    rpc = getattr(config, "ALCHEMY_RPC_URL", "") or _FALLBACK_RPC
    data = "0x70a08231" + _FUNDER[2:].rjust(64, "0")
    try:
        r = await client.post(rpc, json={"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                                         "params": [{"to": _PUSD, "data": data}, "latest"]}, timeout=15)
        res = r.json().get("result")
        if res and res != "0x":
            return int(res, 16) / 1e6
    except Exception as exc:
        log.warning("[Recap] balance fetch failed: %s", exc)
    return None


def _collapse_ties(seq):
    """Collapse consecutive same-resolved_at trades (one grading batch) into a single
    outcome — 'won' if all won, 'lost' if all lost, else 'mixed'. seq is a list of
    (status, resolved_at) in time order (ASC or DESC). Returns [(outcome, ts), ...].

    The resolver grades markets in hourly batches, so dozens of unrelated trades can
    share one resolved_at; without this, a single batch reads as a huge "streak"
    (the infamous 32W). Collapsing to one outcome per batch keeps streaks believable —
    a batch counts once, and a batch containing both wins and losses ('mixed') breaks
    any run."""
    out = []
    i, n = 0, len(seq)
    while i < n:
        ts = seq[i][1]
        statuses = set()
        j = i
        while j < n and seq[j][1] == ts:
            statuses.add(seq[j][0])
            j += 1
        out.append(("won" if statuses == {"won"} else "lost" if statuses == {"lost"} else "mixed", ts))
        i = j
    return out


def _streak_emoji(n: int) -> str:
    """Escalating heat for a win streak of length n (rounds)."""
    if n >= 9:
        return "🌋👑"
    if n >= 6:
        return "🌋"
    if n >= 4:
        return "🔥🔥"
    return "🔥"


def _winbar(wins: int, losses: int, cells: int = 10) -> str:
    """Unicode win-rate bar — ▰ for the win share, ▱ for the rest."""
    total = wins + losses
    if total <= 0:
        return "▱" * cells
    k = round(cells * wins / total)
    k = max(0, min(cells, k))
    return "▰" * k + "▱" * (cells - k)


def _money(x: float) -> str:
    """One money grammar for the whole feed: +$4.80 / −$3.00 (sign first, true
    minus) — matches the image card and the fly trader's settle cards."""
    sign = "+" if x >= 0 else "−"
    return f"{sign}${abs(x):.2f}"


# Honest footer pools (feed v2; written by the 2026-06-12 copy panel).
# {net} is the formatted all-time number; {n} the open-slip count.
# Down-bad lines fire only when lifetime net is actually negative — joking about
# "tuition" while up would be its own kind of dishonest.
_LIFETIME_LINES = [
    "lifetime: {net}. we prefer the term 'tuition.'",
    "{net} since inception. inception was a mistake.",
    "all-time stands at {net}. the vault remains theoretical.",
    "all-time: {net}. somewhere out there, our money is thriving.",
    "{net} all-time. every empire starts somewhere. usually not here.",
    "all-time {net}. we paid that for entertainment. honestly? fair.",
]
_LIFETIME_UP_LINES = [
    "all-time: {net}. somehow. nobody breathe.",
    "{net} lifetime. the bot demands respect (a small amount).",
    "all-time {net}. green. genuinely unclear how. don't jinx it.",
]
_SWEAT_LINES = [
    "{n} slips still open overnight. sleep is for the solvent.",
    "{n} open positions. schrödinger's bankroll till morning.",
    "{n} bets sweating till morning. the bets are fine. we are not.",
    "carrying {n} into the night. the night has a record against us.",
]


def _lifetime_line(lt_net: float, seed: int) -> str:
    pool = _LIFETIME_LINES if lt_net < 0 else _LIFETIME_UP_LINES
    return _pick(pool, seed).format(net=_money(lt_net))


def _sweat_line(n: int) -> str:
    """Overnight open-positions line. Seeded by count AND day so a cap-pinned bot
    doesn't post the identical joke every single night; singular gets its own line."""
    if n == 1:
        return "one slip still open overnight. all eyes on it."
    seed = n + datetime.utcnow().timetuple().tm_yday
    return _pick(_SWEAT_LINES, seed).format(n=n)


def _lifetime_stats():
    """(record_str, net_float) lifetime, from the (chain-healed) trade stats; (None, None)
    on any failure — the recap renders fine without it."""
    try:
        s = database.get_trade_stats()
        won, lost = int(s.get("won") or 0), int(s.get("lost") or 0)
        net = s.get("total_pnl")
        if won + lost == 0 or net is None:
            return None, None
        return f"{won}W–{lost}L", float(net)
    except Exception as exc:
        log.warning("[Recap] lifetime stats unavailable: %s", exc)
        return None, None


def _fetch_curve(days: int = 30):
    """[(resolved_at, cumulative_pnl), ...] ASC over the last N days — the card's
    equity tape. Realized P&L only (the honest, settled number)."""
    try:
        db = database.get_db()
        since = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        rows = db.execute(
            "SELECT resolved_at, pnl FROM trade_executions "
            "WHERE resolution_status IN ('won','lost') AND resolved_at >= ? AND pnl IS NOT NULL "
            "ORDER BY resolved_at ASC",
            (since,),
        ).fetchall()
        out, cum = [], 0.0
        for ts, pnl in rows:
            cum += float(pnl or 0.0)
            out.append((int(ts), round(cum, 4)))
        return out
    except Exception as exc:
        log.warning("[Recap] curve fetch failed: %s", exc)
        return []


def _fetch_recap_data(window_hours: int):
    """Resolved trades in window (list of dicts) + open count + trailing streak."""
    db = database.get_db()
    since = int((datetime.utcnow() - timedelta(hours=window_hours)).timestamp())
    rows = db.execute(
        "SELECT market_question, bet_side, bet_price_filled, bet_price_intended, "
        "pnl, resolution_status, resolved_at FROM trade_executions "
        "WHERE resolution_status IN ('won','lost') AND resolved_at >= ? ORDER BY resolved_at ASC",
        (since,),
    ).fetchall()
    resolved = [dict(zip(_COLS, row)) for row in rows]
    open_count = db.execute(
        "SELECT COUNT(*) FROM trade_executions WHERE status='filled' AND resolution_status='pending'"
    ).fetchone()[0]
    streak_rows = db.execute(
        "SELECT resolution_status, resolved_at FROM trade_executions WHERE resolution_status IN ('won','lost') "
        "ORDER BY resolved_at DESC LIMIT 200"
    ).fetchall()
    streak_kind, streak_n = "none", 0
    rounds = _collapse_ties(streak_rows)  # DESC: rounds[0] is the most recent batch; ties merged so a grading run can't inflate the trailing streak
    if rounds and rounds[0][0] != "mixed":
        streak_kind = rounds[0][0]
        for outcome, _ts in rounds:
            if outcome == streak_kind:
                streak_n += 1
            else:
                break
    return resolved, open_count, (streak_kind, streak_n)


def _price(t) -> float:
    return float(t.get("bet_price_filled") or t.get("bet_price_intended") or 0.0)


def _mkt(t, n: int = 48) -> str:
    q = t.get("market_question") or "?"
    return html.escape(q[:n] + ("…" if len(q) > n else ""))


# Rotating banter pools — varied so the feed doesn't read the same line every day.
# Picked deterministically by a per-post seed (no randomness → reproducible).
_HOT = [
    "the bot is HOT rn. do not breathe on it.",
    "everything it touches resolves YES. cursed or blessed, unclear.",
    "the bot is cooking and we are NOT asking questions.",
    "on a heater. the favorites are being very cooperative today.",
]
_COLD = [
    "the bot is ice cold. like, concerningly cold.",
    "the markets are bullying a small bot that bets a dollar. flagged. 🚩",
    "rough patch. the bot is journaling about it. we ride at dawn. 🫡",
    "favorites losing. this is not how probability works and yet.",
]
_GREEN = [
    "up on the day. small stakes, big feelings.",
    "green on the day. chef's kiss. 📈",
    "profit unlocked. the bot bought a metaphorical coffee. ☕",
    "day closes green. quietly chuffed.",
]
_RED = [
    "red day. one dollar at a time, it bleeds.",
    "the market said no. the bot said ok and went to lie down.",
    "down on the day. shake it off — tomorrow's a fresh slate.",
    "not the day we wanted. the bot has logged off emotionally.",
]


def _pick(pool, seed: int) -> str:
    return pool[seed % len(pool)]


def _streak_tag(kind: str, n: int) -> str:
    if n < 2:
        return ""
    if kind == "won":
        return f"  ·  {_streak_emoji(n)} {n}-win heater" + (" 👑" if n >= 7 else "")
    return f"  ·  💀 {n}-loss skid" + (" 🪦" if n >= 5 else "")


def _commentary(net: float, wins: int, losses: int, streak_kind: str, streak_n: int) -> str:
    seed = wins + losses
    if wins + losses == 0:
        return "crickets. the books are quiet. 🦗"
    if streak_kind == "won" and streak_n >= 3:
        return _pick(_HOT, seed)
    if streak_kind == "lost" and streak_n >= 3:
        return _pick(_COLD, seed)
    if net > 1.0:
        return _pick(_GREEN, seed)
    if net < -1.0:
        return _pick(_RED, seed)
    return "chop. lived to fight another day."


def format_results_recap(resolved, open_count, streak, balance, milestone=None,
                         lifetime=None):
    """Build the daily recap (HTML) or None if there's truly nothing to say.

    Feed v2: tighter card grammar (winbar + record up top, real dollars — the share-units
    bug is fixed and history chain-healed, so no more '(notional)' hedge), an honest
    all-time line, and rotating sweat copy. lifetime=(record_str, net_float) or None.
    Designed to fit a Telegram photo caption (≤1024 chars) so it can ride the image card."""
    streak_kind, streak_n = streak
    if not resolved and open_count == 0:
        return None

    now = datetime.utcnow()
    date_human = f"{now.strftime('%A, %b')} {now.day}"
    lines = [f"📊 <b>THE DAILY</b> — <i>{date_human}</i>", ""]

    wins = [t for t in resolved if t["resolution_status"] == "won"]
    losses = [t for t in resolved if t["resolution_status"] == "lost"]
    net = sum((t.get("pnl") or 0.0) for t in resolved)

    if resolved:
        lines.append(f"{_winbar(len(wins), len(losses))}  <b>{len(wins)}W–{len(losses)}L</b>"
                     f"{_streak_tag(streak_kind, streak_n)}")
        lines.append(f"{'📈' if net >= 0 else '📉'} <b>{_money(net)}</b> on the day")
        lines.append("")
        if wins:
            bw = max(wins, key=lambda t: t.get("pnl") or 0.0)
            lines.append(f"🏆 {html.escape(str(bw['bet_side']))} @ {_price(bw)*100:.0f}¢ — {_mkt(bw, 36)}  <b>{_money(bw.get('pnl') or 0.0)}</b>")
        if losses:
            bl = min(losses, key=lambda t: t.get("pnl") or 0.0)
            lines.append(f"💀 {html.escape(str(bl['bet_side']))} @ {_price(bl)*100:.0f}¢ — {_mkt(bl, 36)}  <b>{_money(bl.get('pnl') or 0.0)}</b>")
        # Wildest call = longest-odds WINNER (most surprising hit); else boldest swing.
        if wins:
            w = min(wins, key=_price)
            if _price(w) <= 0.45:
                lines.append(f"🎲 wildest: {html.escape(str(w['bet_side']))} cashed at <b>{_price(w)*100:.0f}¢</b> 🤯")
        elif resolved:
            w = min(resolved, key=_price)
            lines.append(f"🎲 boldest: {html.escape(str(w['bet_side']))} @ {_price(w)*100:.0f}¢ — didn't land")
        lines.append("")
    else:
        lines.append("🦗 <b>quiet 24h</b> — nothing settled, kitchen's still open.")
        lines.append("")

    if open_count:
        lines.append(f"📂 {_sweat_line(open_count)}")
    money_bits = []
    if balance is not None:
        money_bits.append(f"🏦 bank <b>${balance:.2f}</b>")
    lt_record, lt_net = lifetime if lifetime else (None, None)
    if lt_net is not None:
        money_bits.append(f"all-time <b>{_money(lt_net)}</b>" + (f" ({lt_record})" if lt_record else ""))
    if money_bits:
        lines.append("  ·  ".join(money_bits))
    if milestone:
        lines.append("")
        lines.append(milestone)
    lines.append("")
    lines.append(f"<i>{_commentary(net if resolved else 0.0, len(wins), len(losses), streak_kind, streak_n)}</i>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Milestones (all-time records / round-number crossings)
# ---------------------------------------------------------------------------

def _runs(rows):
    """Collapse ASC ('won'/'lost', resolved_at) into runs of consecutive same-outcome
    ROUNDS — same-resolved_at batches are merged first and a 'mixed' batch breaks the
    run: list of (kind, length, end_ts). Keeps the longest-streak milestone believable."""
    runs = []
    kind, length, end_ts = None, 0, None
    for outcome, ts in _collapse_ties(rows):
        if outcome == "mixed":
            if kind is not None:
                runs.append((kind, length, end_ts))
            kind, length, end_ts = None, 0, None
            continue
        if outcome == kind:
            length += 1
            end_ts = ts
        else:
            if kind is not None:
                runs.append((kind, length, end_ts))
            kind, length, end_ts = outcome, 1, ts
    if kind is not None:
        runs.append((kind, length, end_ts))
    return runs


def _compute_milestone(db, since_ts):
    """ONE most-entertaining all-time record/milestone broken in [since_ts, now], or None.
    Pure read over trade_executions — no new instrumentation."""
    agg = db.execute(
        "SELECT MAX(pnl), MIN(pnl), COUNT(*) FROM trade_executions WHERE resolution_status IN ('won','lost')"
    ).fetchone()
    all_max, all_min, total = agg[0], agg[1], agg[2]
    wagg = db.execute(
        "SELECT MAX(pnl), MIN(pnl), COUNT(*) FROM trade_executions "
        "WHERE resolution_status IN ('won','lost') AND resolved_at >= ?", (since_ts,)
    ).fetchone()
    w_max, w_min, w_count = wagg[0], wagg[1], wagg[2]
    prior_total = total - w_count

    runs = _runs(db.execute(
        "SELECT resolution_status, resolved_at FROM trade_executions "
        "WHERE resolution_status IN ('won','lost') ORDER BY resolved_at ASC"
    ).fetchall())
    win_lens = [r[1] for r in runs if r[0] == "won"]
    loss_lens = [r[1] for r in runs if r[0] == "lost"]

    cand = []  # (priority, line) — lowest priority number wins
    if all_max is not None and all_max > 0 and w_max is not None and w_max >= all_max:
        cand.append((1, f"🥇 <b>New record:</b> biggest win ever — <b>{_money(all_max)}</b>"))
    if win_lens:
        longest = max(win_lens)
        if longest >= 3 and win_lens.count(longest) == 1 and any(
                k == "won" and ln == longest and ts >= since_ts for k, ln, ts in runs):
            cand.append((2, f"🔥 <b>New record:</b> longest win streak — <b>{longest} in a row</b>"))
    if total > 0:
        boundary = (total // 100) * 100
        if boundary >= 100 and prior_total < boundary <= total:
            cand.append((3, f"🎯 <b>Milestone:</b> <b>{boundary}</b> total trades resolved"))
    if all_min is not None and all_min < 0 and w_min is not None and w_min <= all_min:
        cand.append((4, f"💸 <b>New record:</b> biggest loss ever — <b>{_money(all_min)}</b> (oof)"))
    if loss_lens:
        longest = max(loss_lens)
        if longest >= 3 and loss_lens.count(longest) == 1 and any(
                k == "lost" and ln == longest and ts >= since_ts for k, ln, ts in runs):
            cand.append((5, f"💀 <b>New record:</b> longest loss skid — <b>{longest} straight</b>"))
    if not cand:
        return None
    cand.sort(key=lambda c: c[0])
    return cand[0][1]


# ---------------------------------------------------------------------------
# Weekly highlights (Sundays)
# ---------------------------------------------------------------------------

def _best_win_streak(resolved):
    """Longest run of consecutive winning rounds (same-resolved_at batches collapsed
    first, so one grading batch can't inflate it). resolved is ASC by resolved_at."""
    best = run = 0
    for outcome, _ts in _collapse_ties([(t["resolution_status"], t.get("resolved_at")) for t in resolved]):
        if outcome == "won":
            run += 1
            best = max(best, run)
        else:
            run = 0
    return best


def _weekly_commentary(net, best_streak):
    if best_streak >= 5:
        return f"a {best_streak}-round heater in there — the bot briefly became a genius."
    if net > 2.0:
        return "green week — the boys eat. 🍽️"
    if net < -2.0:
        return "red week, but we're building character. 📚"
    if best_streak >= 3:
        return f"a {best_streak}-round win run headlined the week. we'll take it."
    return "breakeven-ish week — lived to bet another one."


def format_weekly_highlights(resolved, balance, lifetime=None):
    """Sunday week-in-review message, or a graceful quiet-week line."""
    now = datetime.utcnow()
    start = now - timedelta(days=7)
    rng = f"{start.strftime('%b')} {start.day} – {now.strftime('%b')} {now.day}"
    lines = [f"🗓 <b>THE WEEK</b> — <i>{rng}</i>", ""]

    lt_record, lt_net = lifetime if lifetime else (None, None)

    if not resolved:
        lines.append("🦗 <b>quiet week</b> — nothing settled. the grind continues. 🫡")
        if balance is not None:
            lines.append(f"🏦 bank <b>${balance:.2f}</b>")
        return "\n".join(lines)

    wins = [t for t in resolved if t["resolution_status"] == "won"]
    losses = [t for t in resolved if t["resolution_status"] == "lost"]
    net = sum((t.get("pnl") or 0.0) for t in resolved)
    best = _best_win_streak(resolved)

    streak_bit = f"  ·  best run: {_streak_emoji(best)} {best} straight" if best >= 2 else ""
    lines.append(f"{_winbar(len(wins), len(losses))}  <b>{len(wins)}W–{len(losses)}L</b>{streak_bit}")
    lines.append(f"{'📈' if net >= 0 else '📉'} <b>{_money(net)}</b> on the week")
    lines.append("")
    if wins:
        bw = max(wins, key=lambda t: t.get("pnl") or 0.0)
        lines.append(f"🏆 <b>win of the week:</b> {html.escape(str(bw['bet_side']))} @ {_price(bw)*100:.0f}¢ — {_mkt(bw, 40)}  <b>{_money(bw.get('pnl') or 0.0)}</b>")
        w = min(wins, key=_price)
        if _price(w) <= 0.45:
            lines.append(f"🎲 <b>wildest call:</b> {html.escape(str(w['bet_side']))} cashed at <b>{_price(w)*100:.0f}¢</b> 🤯")
    if losses:
        bl = min(losses, key=lambda t: t.get("pnl") or 0.0)
        lines.append(f"💀 <b>worst beat:</b> {html.escape(str(bl['bet_side']))} @ {_price(bl)*100:.0f}¢ — {_mkt(bl, 40)}  <b>{_money(bl.get('pnl') or 0.0)}</b>")
    lines.append("")
    if balance is not None:
        lines.append(f"🏦 bank <b>${balance:.2f}</b>")
    lines.append("")
    if lt_net is not None:
        lines.append(f"<i>{_lifetime_line(lt_net, len(resolved) + int(abs(lt_net)))}</i>")
    else:
        lines.append(f"<i>{_weekly_commentary(net, best)}</i>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Monthly highlights (last Sunday of the month)
# ---------------------------------------------------------------------------

def _active_days(resolved):
    """Distinct UTC calendar days with at least one resolution."""
    days = set()
    for t in resolved:
        ts = t.get("resolved_at")
        if ts:
            days.add(datetime.utcfromtimestamp(ts).date())
    return len(days)


def _monthly_commentary(net, best_streak, wins, losses):
    total = wins + losses
    if total == 0:
        return "a quiet month on the tape. building. 🧱"
    if best_streak >= 6:
        return f"a {best_streak}-round heater this month — peak bot, no notes."
    if net > 5.0:
        return "green month. the syndicate eats well. 🍾"
    if net < -5.0:
        return "red month — tuition paid to the market. we learn. 🎓"
    if best_streak >= 4:
        return f"a {best_streak}-round win run in the books. quietly stacking. 🧊"
    if wins / total >= 0.55:
        return "more right than wrong. the bot is pleased with itself."
    return "a grind of a month — still standing. 🫡"


def format_monthly_highlights(resolved, balance, prev_net=None, lifetime=None):
    """Last-Sunday-of-month review over the prior 30 days, or a quiet-month line.
    prev_net (net of the 30 days before this window) adds optional MoM framing;
    pass None to omit it (e.g. first run, before two full windows of data)."""
    now = datetime.utcnow()
    start = now - timedelta(days=30)
    rng = f"{start.strftime('%b')} {start.day} – {now.strftime('%b')} {now.day}"
    lines = [f"📅 <b>THE MONTH</b> — <i>{rng}</i>", ""]

    if not resolved:
        lines.append("🦗 <b>quiet month</b> — nothing settled. the grind continues. 🫡")
        if balance is not None:
            lines.append(f"🏦 bank <b>${balance:.2f}</b>")
        return "\n".join(lines)

    wins = [t for t in resolved if t["resolution_status"] == "won"]
    losses = [t for t in resolved if t["resolution_status"] == "lost"]
    net = sum((t.get("pnl") or 0.0) for t in resolved)
    best = _best_win_streak(resolved)

    streak_bit = f"  ·  best run: {_streak_emoji(best)} {best} straight" if best >= 2 else ""
    lines.append(f"{_winbar(len(wins), len(losses))}  <b>{len(wins)}W–{len(losses)}L</b> on the month{streak_bit}")
    net_line = f"{'📈' if net >= 0 else '📉'} <b>{_money(net)}</b> on the month"
    if prev_net is not None:
        delta = net - prev_net
        net_line += f"  ·  prev 30d {_money(prev_net)} ({'▲' if delta >= 0 else '▼'}${abs(delta):.2f})"
    lines.append(net_line)
    lines.append(f"🗓 <b>{_active_days(resolved)}</b> active days  ·  {len(resolved)} resolved")
    lines.append("")
    if wins:
        bw = max(wins, key=lambda t: t.get("pnl") or 0.0)
        lines.append(f"🏆 <b>win of the month:</b> {html.escape(str(bw['bet_side']))} @ {_price(bw)*100:.0f}¢ — {_mkt(bw, 40)}  <b>{_money(bw.get('pnl') or 0.0)}</b>")
        w = min(wins, key=_price)
        if _price(w) <= 0.45:
            lines.append(f"🎲 <b>wildest call:</b> {html.escape(str(w['bet_side']))} cashed at <b>{_price(w)*100:.0f}¢</b> 🤯")
    if losses:
        bl = min(losses, key=lambda t: t.get("pnl") or 0.0)
        lines.append(f"💀 <b>worst beat:</b> {html.escape(str(bl['bet_side']))} @ {_price(bl)*100:.0f}¢ — {_mkt(bl, 40)}  <b>{_money(bl.get('pnl') or 0.0)}</b>")
    lines.append("")
    if balance is not None:
        lines.append(f"🏦 bank <b>${balance:.2f}</b>")
    lines.append("")
    lt_record, lt_net = lifetime if lifetime else (None, None)
    if lt_net is not None:
        lines.append(f"<i>{_lifetime_line(lt_net, len(resolved) + int(abs(lt_net)))}</i>")
    else:
        lines.append(f"<i>{_monthly_commentary(net, best, len(wins), len(losses))}</i>")
    return "\n".join(lines)


async def results_recap_loop(dry_run: bool = False) -> None:
    """Daily loop at RECAP_SEND_HOUR_UTC:00 — posts the daily recap (with any
    milestone); on the last Sunday of the month posts the monthly review, on
    other Sundays the weekly highlights — all in the same slot."""
    log.info("[Recap] Started (daily + Sunday weekly + last-Sunday monthly at %02d:00 UTC, dry_run=%s)",
             RECAP_SEND_HOUR_UTC, dry_run)
    sender = TelegramSender(token=config.TELEGRAM_BOT_TOKEN, chat_id=config.TELEGRAM_CHAT_ID)
    async with httpx.AsyncClient() as client:
        while True:
            now = datetime.utcnow()
            target = now.replace(hour=RECAP_SEND_HOUR_UTC, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            log.info("[Recap] Next recap at %s UTC (in %.1fh)", target.strftime("%Y-%m-%d %H:%M"), wait / 3600)
            await asyncio.sleep(wait)
            try:
                db = database.get_db()
                since_ts = int((datetime.utcnow() - timedelta(hours=RECAP_WINDOW_HOURS)).timestamp())
                resolved, open_count, streak = _fetch_recap_data(RECAP_WINDOW_HOURS)
                milestone = _compute_milestone(db, since_ts)
                balance = await _free_balance(client)
                lifetime = _lifetime_stats()

                posts = []  # (kind, text, photo_bytes_or_None)
                daily = format_results_recap(resolved, open_count, streak, balance, milestone,
                                             lifetime=lifetime)
                if daily:
                    nowp = datetime.utcnow()
                    wins_n = sum(1 for t in resolved if t["resolution_status"] == "won")
                    losses_n = sum(1 for t in resolved if t["resolution_status"] == "lost")
                    net = sum((t.get("pnl") or 0.0) for t in resolved)
                    card = render_recap_card(
                        title="the daily",
                        date_str=f"{nowp.strftime('%A, %b')} {nowp.day}",
                        wins=wins_n, losses=losses_n, net=net,
                        curve=_fetch_curve(30),
                        lifetime_net=lifetime[1], balance=balance,
                    )
                    posts.append(("daily", daily, card))
                nowd = datetime.utcnow()
                if nowd.weekday() == 6:  # Sunday
                    # Last Sunday of the month ⇔ the next Sunday falls in a different
                    # month. Holds across the year boundary too (late-Dec → Jan changes
                    # the month), so no explicit year check is needed.
                    if (nowd + timedelta(days=7)).month != nowd.month:
                        # Last Sunday of the month → monthly review (takes the slot;
                        # weekly is skipped this week to avoid two near-identical posts).
                        all60, _, _ = _fetch_recap_data(24 * 60)
                        cut = int((nowd - timedelta(days=30)).timestamp())
                        last30 = [t for t in all60 if (t.get("resolved_at") or 0) >= cut]
                        prev30 = [t for t in all60 if (t.get("resolved_at") or 0) < cut]
                        prev_net = sum((t.get("pnl") or 0.0) for t in prev30) if prev30 else None
                        posts.append(("monthly",
                                      format_monthly_highlights(last30, balance, prev_net,
                                                                lifetime=lifetime), None))
                    else:
                        weekly_resolved, _, _ = _fetch_recap_data(24 * 7)
                        posts.append(("weekly",
                                      format_weekly_highlights(weekly_resolved, balance,
                                                               lifetime=lifetime), None))

                if not posts:
                    log.info("[Recap] Nothing to recap — skipping")
                for kind, text, photo in posts:
                    if dry_run:
                        log.info("[Recap] DRY RUN (%s, card=%s):\n%s",
                                 kind, "yes" if photo else "no", text)
                        continue
                    sent_card = False
                    # Caption hard cap is 1024 — if the text is longer, the card would
                    # truncate it, so prefer the full text message instead.
                    if photo and len(text) <= 1024:
                        sent_card = await sender.send_photo(photo, client, caption=text)
                    sent_text = False
                    if not sent_card:
                        sent_text = await sender.send_message(text, client)
                    if sent_card or sent_text:
                        log.info("[Recap] Posted %s recap (%s)", kind,
                                 "card" if sent_card else "text")
                    else:
                        log.error("[Recap] FAILED to post %s recap (photo and text "
                                  "sends both failed)", kind)
            except Exception as exc:
                log.exception("[Recap] Failed to build/post recap: %s", exc)
