"""
digest.py — Digest buffer, analytical briefing formatter, and sender for the
60–79 score tier.

Design:
  • DigestBuffer accumulates AlertPayload objects in memory, protected by
    an asyncio.Lock (safe for concurrent writes from the trade processor).
  • digest_loop() drains the buffer every DIGEST_INTERVAL_SECONDS, formats
    an analytical briefing (score distribution, top markets, top 5 signals,
    largest trade), and — when DIGEST_CSV_ENABLED — attaches a full-data CSV.
  • Buffer cap: 200 entries. On overflow the lowest-scoring entry is evicted
    so the digest stays signal-rich.
  • Dedup by trade_id — the same trade will never appear twice.
  • insert_alert_outcome() is called in process_trade() the moment a signal
    is buffered — not here. digest_loop() only formats and sends.

Digest message format (Telegram HTML):
  Header with digest ID and window metadata.
  Score distribution bars (5-point buckets, non-empty only).
  Top 3 markets by signal density, with Polymarket links.
  Top 5 signals by score, with Polymarket links.
  Largest trade in the window.
  Remainder summary (when > 5 entries).
  CSV attachment notice (when DIGEST_CSV_ENABLED).

CSV attachment:
  All entries, sorted score desc then size_usd desc.
  Written to a temp file, sent via sendDocument, then deleted.
"""

import asyncio
import csv
import html
import logging
import os
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import httpx

import config
import database
from alerter import AlertPayload, TelegramSender

log = logging.getLogger("digest")

_BUFFER_MAX = 200


def _actionability_emoji(hours_to_resolution: Optional[float]) -> str:
    """Compact single-emoji actionability indicator for digest entries."""
    if hours_to_resolution is None:
        return "🟢"
    minutes = hours_to_resolution * 60
    if minutes >= 120:
        return "🟢"
    if minutes >= 30:
        return "🟡"
    if minutes >= 10:
        return "🔴"
    return "⚫"

# CSV columns in output order.
_CSV_FIELDNAMES = [
    "score",
    "wallet",
    "market",
    "market_url",
    "bet_side",
    "bet_price",
    "bet_size_usd",
    "timing",
    "funding_velocity",
    "win_rate",
    "size_anomaly",
    "wallet_age",
    "concentration",
    "underdog",
    "cluster",
    "convergence",
    "trade_id",
    "timestamp",
]


# ---------------------------------------------------------------------------
# Buffer
# ---------------------------------------------------------------------------

class DigestBuffer:
    """
    Asyncio-safe FIFO-ish buffer of AlertPayload objects.

    add()   — O(1) amortised; evicts lowest-score on overflow.
    drain() — returns all entries sorted desc by score and clears the buffer.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._entries: list[AlertPayload] = []
        self._seen: set[str] = set()

    async def add(self, payload: AlertPayload) -> None:
        """Buffer a payload. Silently deduplicates by trade_id."""
        trade_id = payload.trade.trade_id
        async with self._lock:
            if trade_id in self._seen:
                log.debug("[Digest] Duplicate trade %s — skipped", trade_id)
                return

            self._entries.append(payload)
            self._seen.add(trade_id)
            log.debug(
                "[Digest] Buffered trade %s (score=%d, depth=%d)",
                trade_id, payload.breakdown.total, len(self._entries),
            )

            # On overflow, evict the lowest-scoring entry.
            if len(self._entries) > _BUFFER_MAX:
                self._entries.sort(key=lambda p: p.breakdown.total, reverse=True)
                dropped = self._entries.pop()
                self._seen.discard(dropped.trade.trade_id)
                log.warning(
                    "[Digest] Buffer full — evicted trade %s (score=%d)",
                    dropped.trade.trade_id, dropped.breakdown.total,
                )

    async def drain(self) -> list[AlertPayload]:
        """
        Return all buffered entries sorted desc by score and clear the buffer.
        Returns an empty list if nothing is buffered.
        """
        async with self._lock:
            if not self._entries:
                return []
            entries = sorted(self._entries, key=lambda p: p.breakdown.total, reverse=True)
            self._entries.clear()
            self._seen.clear()
            return entries


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _market_url(p: AlertPayload) -> str:
    """Return a Polymarket URL for this entry, or empty string if no slug."""
    if p.market_slug:
        return f"https://polymarket.com/event/{p.market_slug}"
    return ""


def _market_link(title: str, url: str, max_len: int = 80) -> str:
    """
    Return an HTML <a> tag if url is non-empty, otherwise escaped plain text.
    Truncates title to max_len characters before escaping.
    """
    if len(title) > max_len:
        title = title[:max_len - 3] + "..."
    escaped = html.escape(title)
    if url:
        return f'<a href="{url}">{escaped}</a>'
    return escaped


def _score_distribution_lines(entries: list[AlertPayload]) -> list[str]:
    """
    Build proportional bar lines for 5-point score buckets derived from config
    thresholds. Generates 5-point intervals from ALERT_DIGEST_THRESHOLD up to
    ALERT_INSTANT_THRESHOLD, plus a combined instant+ bucket.
    Only non-empty buckets are included.
    """
    digest = config.ALERT_DIGEST_THRESHOLD
    instant = config.ALERT_INSTANT_THRESHOLD

    buckets: list[tuple[int, int, str]] = []
    lo = digest
    while lo < instant:
        hi = min(lo + 4, instant - 1)
        buckets.append((lo, hi, f"{lo}-{hi}"))
        lo = hi + 1
    buckets.append((instant, 130, f"{instant}+"))

    counts = {
        label: sum(1 for p in entries if lo <= p.breakdown.total <= hi)
        for lo, hi, label in buckets
    }
    non_empty = [(lo, hi, label) for lo, hi, label in buckets if counts[label] > 0]
    if not non_empty:
        return []

    max_count = max(counts[label] for _, _, label in non_empty)
    lines = []
    for _, _, label in non_empty:
        count = counts[label]
        bar_width = max(1, round(count / max_count * 20))
        lines.append(f"  {label}: {'█' * bar_width} {count} alert{'s' if count != 1 else ''}")
    return lines


def _top_markets_lines(entries: list[AlertPayload], top_n: int = 3) -> list[str]:
    """
    Group entries by market, sort by signal count descending, return top_n
    as HTML lines with Polymarket links and volume totals.
    """
    mdata: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "volume": 0.0, "title": "", "url": "",
    })
    for p in entries:
        mid = p.trade.market_id
        mdata[mid]["count"] += 1
        mdata[mid]["volume"] += p.trade.size_usd
        if not mdata[mid]["title"]:
            mdata[mid]["title"] = p.market_title or mid
        if not mdata[mid]["url"]:
            mdata[mid]["url"] = _market_url(p)

    sorted_markets = sorted(mdata.values(), key=lambda d: d["count"], reverse=True)[:top_n]

    lines = []
    for info in sorted_markets:
        n = info["count"]
        link = _market_link(info["title"], info["url"], max_len=60)
        lines.append(
            f"  {link} — {n} signal{'s' if n != 1 else ''}, ${info['volume']:,.0f} vol"
        )
    return lines


# ---------------------------------------------------------------------------
# Digest message formatter
# ---------------------------------------------------------------------------

def format_digest(
    entries: list[AlertPayload],
    date_human: str,
    bot_activity_line: str = "",
) -> str:
    """
    Build the full Telegram HTML digest message for the daily intelligence brief.
    Entries must be pre-sorted descending by score (drain() guarantees this).
    Returns empty string if entries is empty.
    """
    if not entries:
        return ""

    total = len(entries)
    total_volume = sum(p.trade.size_usd for p in entries)
    unique_markets = len({p.trade.market_id for p in entries})
    top = entries[:5]
    remainder = entries[5:]

    lines: list[str] = [
        "📊 <b>Daily Intelligence Brief</b>",
        f"<i>{html.escape(date_human)} UTC</i>",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        f"<b>Signals detected:</b> {total}",
        f"<b>Volume tracked:</b> ${total_volume:,.0f}",
        f"<b>Markets covered:</b> {unique_markets}",
    ]
    if bot_activity_line:
        lines.append(bot_activity_line)
    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Score distribution bars.
    dist_lines = _score_distribution_lines(entries)
    if dist_lines:
        lines.append("<b>Score distribution:</b>")
        lines.extend(dist_lines)
        lines.append("")

    # Top markets by signal density.
    market_lines = _top_markets_lines(entries)
    if market_lines:
        lines.append("<b>Top markets by signal density:</b>")
        lines.extend(market_lines)
        lines.append("")

    # Top 5 signals.
    lines.append("<b>Top 5 signals by score:</b>")
    for p in top:
        t = p.trade
        b = p.breakdown
        wallet = t.taker_address or t.maker_address or "unknown"
        wallet_short = (
            f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet
        )
        url = _market_url(p)
        m_link = _market_link(p.market_title or "Unknown Market", url, max_len=80)
        side = (t.outcome or "?").upper()
        action_emoji = _actionability_emoji(p.hours_to_resolution)
        conv_emoji = "🔗" if b.convergence_bonus > 0 else ""
        lines.append(f"- {action_emoji}{conv_emoji} <b>{b.total}</b> — <code>{html.escape(wallet_short)}</code>")
        lines.append(f"   {m_link}")
        lines.append(f"   ${t.size_usd:,.0f} {side} @ {t.price:.2f}")
    lines.append("")

    # Largest trade in the full window (all entries, not just top 5).
    largest = max(entries, key=lambda p: p.trade.size_usd)
    lt = largest.trade
    lb = largest.breakdown
    l_url = _market_url(largest)
    l_title = largest.market_title or "Unknown Market"
    if len(l_title) > 60:
        l_title = l_title[:57] + "..."
    if l_url:
        l_market = f'<a href="{l_url}">"{html.escape(l_title)}"</a>'
    else:
        l_market = f'"{html.escape(l_title)}"'
    lines.append("<b>Largest trade in window:</b>")
    lines.append(
        f"${lt.size_usd:,.0f} {(lt.outcome or '?').upper()} "
        f"on {l_market} (score {lb.total})"
    )

    # Remainder line — only when there are entries beyond the top 5.
    if remainder:
        rn = len(remainder)
        rl = max(remainder, key=lambda p: p.trade.size_usd)
        rl_t = rl.trade
        rl_b = rl.breakdown
        rl_m = rl.market_title or "Unknown Market"
        if len(rl_m) > 50:
            rl_m = rl_m[:47] + "..."
        lines.append("")
        lines.append(
            f"<i>+ {rn} more | largest unseen: "
            f"${rl_t.size_usd:,.0f} {(rl_t.outcome or '?').upper()} "
            f"on \"{html.escape(rl_m)}\" (score {rl_b.total})</i>"
        )

    # CSV footer — only shown when the file will actually be attached.
    if config.DIGEST_CSV_ENABLED:
        lines.append("")
        lines.append("📎 Full data attached below")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV generation
# ---------------------------------------------------------------------------

def _write_digest_csv(entries: list[AlertPayload], digest_id: str) -> str:
    """
    Write all entries to a named temp CSV file sorted by score desc then
    bet_size_usd desc. Returns the file path. Caller must delete the file.
    Uses Python's csv module — no manual string concatenation.
    """
    sorted_entries = sorted(
        entries,
        key=lambda p: (p.breakdown.total, p.trade.size_usd),
        reverse=True,
    )

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        newline="",
        encoding="utf-8",
    )

    with tmp:
        writer = csv.DictWriter(tmp, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        for p in sorted_entries:
            t = p.trade
            b = p.breakdown
            wallet = t.taker_address or t.maker_address or ""
            url = _market_url(p)
            ts_str = (
                datetime.utcfromtimestamp(t.timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
                if t.timestamp
                else ""
            )
            writer.writerow({
                "score":        b.total,
                "wallet":       wallet,
                "market":       p.market_title or "",
                "market_url":   url,
                "bet_side":     t.outcome or "",
                "bet_price":    t.price,
                "bet_size_usd": t.size_usd,
                "timing":            b.timing if b.timing is not None else "",
                "funding_velocity":  b.funding_velocity if b.funding_velocity is not None else "",
                "win_rate":          b.win_rate if b.win_rate is not None else "",
                "size_anomaly":      b.size_anomaly if b.size_anomaly is not None else "",
                "wallet_age":   b.wallet_age if b.wallet_age is not None else "",
                "concentration": b.concentration if b.concentration is not None else "",
                "underdog":     b.underdog if b.underdog is not None else "",
                "cluster":      b.cluster_bonus,
                "convergence":  b.convergence_bonus,
                "trade_id":     t.trade_id,
                "timestamp":    ts_str,
            })

    return tmp.name


# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------

async def digest_loop(buffer: DigestBuffer, dry_run: bool = False) -> None:
    """
    Long-running coroutine. Sends a daily intelligence brief at
    DIGEST_SEND_HOUR_UTC:00 UTC each day. Drains the DigestBuffer at that
    time, formats an analytical briefing, and sends it to Telegram. When
    DIGEST_CSV_ENABLED, also attaches a full-data CSV. Empty windows are
    silently skipped. Designed to run as an asyncio.Task under supervised_task.

    Persistence contract: outcome rows are inserted by process_trade() at
    buffer time — nothing to do here.
    """
    log.info(
        "[Digest] Started (daily brief at %02d:00 UTC, dry_run=%s)",
        config.DIGEST_SEND_HOUR_UTC, dry_run,
    )

    sender = TelegramSender(
        token=config.TELEGRAM_BOT_TOKEN,
        chat_id=config.TELEGRAM_CHAT_ID,
    )

    async with httpx.AsyncClient() as client:
        while True:
            # Sleep until the next DIGEST_SEND_HOUR_UTC:00 UTC.
            now = datetime.utcnow()
            target = now.replace(
                hour=config.DIGEST_SEND_HOUR_UTC, minute=0, second=0, microsecond=0,
            )
            if target <= now:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            log.info(
                "[Digest] Next daily brief at %s UTC (in %.1fh)",
                target.strftime("%Y-%m-%d %H:%M"), wait_seconds / 3600,
            )
            await asyncio.sleep(wait_seconds)

            entries = await buffer.drain()

            if not entries:
                log.info("[Digest] No signals buffered — skipping daily brief")
                continue

            log.info("[Digest] Flushing %d buffered signal(s)", len(entries))

            # Metadata at send time.
            now_utc = datetime.utcnow()
            digest_id  = now_utc.strftime("%Y-%m%d-%H%MZ")
            date_human = f"{now_utc.strftime('%A, %B')} {now_utc.day}, {now_utc.year}"

            # Bot activity line: trades executed in the last 24 h.
            bot_activity_line = ""
            if config.TRADING_ENABLED:
                try:
                    day_start_ts = int(
                        now_utc.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
                    )
                    db = database.get_db()
                    row = db.execute(
                        "SELECT COUNT(*), COALESCE(SUM(pnl), 0) FROM trade_executions "
                        "WHERE created_at >= ? AND status = 'filled'",
                        (day_start_ts,),
                    ).fetchone()
                    trades_today = row[0] if row else 0
                    daily_pnl   = row[1] if row else 0.0
                    if trades_today > 0:
                        pnl_emoji = "📈" if daily_pnl >= 0 else "📉"
                        bot_activity_line = (
                            f"🤖 <b>Bot activity:</b> {trades_today} trade"
                            f"{'s' if trades_today != 1 else ''} | "
                            f"P&amp;L: {pnl_emoji}${abs(daily_pnl):.2f}"
                        )
                except Exception as exc:
                    log.warning("[Digest] Bot activity query failed: %s", exc)

            # Build and send the summary message.
            try:
                text = format_digest(entries, date_human, bot_activity_line)
            except Exception as exc:
                log.exception("[Digest] format_digest failed: %s", exc)
                continue

            if not text:
                continue

            if dry_run:
                log.info(
                    "[DRY-RUN] Would send digest #%s (%d signals):\n%s",
                    digest_id, len(entries),
                    text.replace("<b>", "").replace("</b>", "")
                        .replace("<i>", "").replace("</i>", "")
                        .replace("<code>", "").replace("</code>", ""),
                )
            else:
                ok = await sender.send_message(text, client)
                if ok:
                    log.info("[Digest] Sent digest #%s (%d signals)", digest_id, len(entries))
                else:
                    log.error("[Digest] Failed to send digest #%s", digest_id)

            # CSV attachment — generated and sent regardless of text send success.
            if not config.DIGEST_CSV_ENABLED:
                continue

            csv_filename = f"digest-{digest_id}.csv"
            csv_path: Optional[str] = None
            try:
                csv_path = _write_digest_csv(entries, digest_id)

                if dry_run:
                    log.info(
                        "[DRY-RUN] Would send CSV %s (%d rows, path=%s)",
                        csv_filename, len(entries), csv_path,
                    )
                else:
                    ok_csv = await sender.send_document(
                        file_path=csv_path,
                        filename=csv_filename,
                        client=client,
                    )
                    if ok_csv:
                        log.info("[Digest] CSV attached: %s", csv_filename)
                    else:
                        log.error("[Digest] CSV send failed for %s", csv_filename)

            except Exception as exc:
                log.error("[Digest] CSV generation/send error: %s", exc)
            finally:
                if csv_path and os.path.exists(csv_path):
                    try:
                        os.unlink(csv_path)
                        log.debug("[Digest] Deleted temp CSV: %s", csv_path)
                    except Exception as exc:
                        log.warning("[Digest] Failed to delete temp CSV %s: %s", csv_path, exc)
