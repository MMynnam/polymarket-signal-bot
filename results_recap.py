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
        "SELECT resolution_status FROM trade_executions WHERE resolution_status IN ('won','lost') "
        "ORDER BY resolved_at DESC LIMIT 50"
    ).fetchall()
    streak_kind, streak_n = "none", 0
    if streak_rows:
        streak_kind = streak_rows[0][0]
        for row in streak_rows:
            if row[0] == streak_kind:
                streak_n += 1
            else:
                break
    return resolved, open_count, (streak_kind, streak_n)


def _price(t) -> float:
    return float(t.get("bet_price_filled") or t.get("bet_price_intended") or 0.0)


def _mkt(t, n: int = 48) -> str:
    q = t.get("market_question") or "?"
    return html.escape(q[:n] + ("…" if len(q) > n else ""))


def _streak_tag(kind: str, n: int) -> str:
    if n < 2:
        return ""
    if kind == "won":
        return f"  ·  🔥 on a {n}-win streak" + (" 🚀" if n >= 5 else "")
    return f"  ·  💀 on a {n}-loss skid"


def _commentary(net: float, wins: int, losses: int, streak_kind: str, streak_n: int) -> str:
    if wins + losses == 0:
        return "crickets. the books are quiet."
    if streak_kind == "won" and streak_n >= 3:
        return "absolute heater — nobody tell him to stop."
    if streak_kind == "lost" and streak_n >= 3:
        return "rough patch. we ride at dawn. 🫡"
    if net > 1.0:
        return "green on the day. chef's kiss. 📈"
    if net < -1.0:
        return "red day. shake it off — tomorrow's a fresh slate."
    return "chop. lived to fight another day."


def format_results_recap(resolved, open_count, streak, balance):
    """Build the recap message (HTML) or None if there's truly nothing to say."""
    streak_kind, streak_n = streak
    if not resolved and open_count == 0:
        return None

    now = datetime.utcnow()
    date_human = f"{now.strftime('%A, %b')} {now.day}"
    lines = [f"📊 <b>DAILY RECAP</b> — <i>{date_human}</i>", ""]

    wins = [t for t in resolved if t["resolution_status"] == "won"]
    losses = [t for t in resolved if t["resolution_status"] == "lost"]
    net = sum((t.get("pnl") or 0.0) for t in resolved)

    if resolved:
        lines.append(f"🏁 <b>{len(wins)}W–{len(losses)}L</b>{_streak_tag(streak_kind, streak_n)}")
        lines.append(f"{'📈' if net >= 0 else '📉'} <b>Net:</b> ${net:+.2f} <i>(notional)</i>")
        lines.append("")
        if wins:
            bw = max(wins, key=lambda t: t.get("pnl") or 0.0)
            lines.append(f"🏆 <b>Biggest win:</b> {html.escape(str(bw['bet_side']))} — {_mkt(bw)}  <b>${(bw.get('pnl') or 0.0):+.2f}</b>")
        if losses:
            bl = min(losses, key=lambda t: t.get("pnl") or 0.0)
            lines.append(f"💸 <b>Biggest loss:</b> {html.escape(str(bl['bet_side']))} — {_mkt(bl)}  <b>${(bl.get('pnl') or 0.0):+.2f}</b>")
        # Wildest call = longest-odds WINNER (most surprising hit); else boldest swing.
        if wins:
            w = min(wins, key=_price)
            lines.append(f"🎲 <b>Wildest call:</b> {html.escape(str(w['bet_side']))} on {_mkt(w)} hit at <b>{_price(w)*100:.0f}¢</b> 🤯")
        elif resolved:
            w = min(resolved, key=_price)
            lines.append(f"🎲 <b>Boldest swing:</b> {html.escape(str(w['bet_side']))} on {_mkt(w)} @ {_price(w)*100:.0f}¢ — didn't land")
        lines.append("")
    else:
        lines.append("🦗 <b>Quiet 24h</b> — nothing resolved, but the kitchen's still open.")
        lines.append("")

    if open_count:
        lines.append(f"📂 <b>{open_count}</b> position{'s' if open_count != 1 else ''} open going into tomorrow")
    if balance is not None:
        lines.append(f"🏦 <b>Wallet:</b> ${balance:.2f} free")
    lines.append("")
    lines.append(f"<i>{_commentary(net if resolved else 0.0, len(wins), len(losses), streak_kind, streak_n)}</i>")
    return "\n".join(lines)


async def results_recap_loop(dry_run: bool = False) -> None:
    """Daily loop: sleep until RECAP_SEND_HOUR_UTC:00, build + post the recap."""
    log.info("[Recap] Started (daily results recap at %02d:00 UTC, dry_run=%s)", RECAP_SEND_HOUR_UTC, dry_run)
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
                resolved, open_count, streak = _fetch_recap_data(RECAP_WINDOW_HOURS)
                balance = await _free_balance(client)
                text = format_results_recap(resolved, open_count, streak, balance)
                if text is None:
                    log.info("[Recap] Nothing to recap — skipping")
                    continue
                if dry_run:
                    log.info("[Recap] DRY RUN:\n%s", text)
                else:
                    await sender.send_message(text, client)
                    log.info("[Recap] Posted daily recap (%d resolved, net=%.2f)",
                             len(resolved), sum((t.get('pnl') or 0.0) for t in resolved))
            except Exception as exc:
                log.exception("[Recap] Failed to build/post recap: %s", exc)
