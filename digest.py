"""
digest.py — Digest buffer and sender for the 60–79 score tier.

Design:
  • DigestBuffer accumulates AlertPayload objects in memory, protected by
    an asyncio.Lock (safe for concurrent writes from the trade processor).
  • digest_loop() drains the buffer every DIGEST_INTERVAL_SECONDS,
    formats a compact summary, and sends it to Telegram via TelegramSender.
  • Buffer cap: 200 entries. On overflow the lowest-scoring entry is evicted
    so the digest stays signal-rich.
  • Dedup by trade_id — the same trade will never appear twice.
  • insert_alert_outcome() is called in process_trade() the moment a signal
    is buffered — not here. digest_loop() only formats and sends.

Digest message format (HTML):
  Top 5 entries by score, each as two compact lines.
  A trailing "+N more" line if > 5 entries were buffered.
"""

import asyncio
import html
import logging
import time
from typing import Optional

import httpx

import config
from alerter import AlertPayload, TelegramSender

log = logging.getLogger("digest")

_BUFFER_MAX = 200


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
# Formatter
# ---------------------------------------------------------------------------

def format_digest(entries: list[AlertPayload]) -> str:
    """
    Build a compact Telegram HTML digest message.
    Shows top 5 entries by score; adds a summary line for the rest.
    """
    if not entries:
        return ""

    top = entries[:5]
    remainder = entries[5:]
    count = len(entries)
    now_str = time.strftime("%H:%M UTC", time.gmtime())

    lines = [
        f"📋 <b>SIGNAL DIGEST</b>  "
        f"({count} signal{'s' if count != 1 else ''} · {now_str})",
        "",
    ]

    for i, p in enumerate(top, 1):
        t = p.trade
        b = p.breakdown

        # Market title — truncate long names.
        market_title = p.market_title or "Unknown Market"
        if len(market_title) > 80:
            market_title = market_title[:77] + "..."

        # Build market link if slug available.
        if p.market_slug:
            url = f"https://polymarket.com/event/{p.market_slug}"
            market_line = f'<a href="{url}">{html.escape(market_title)}</a>'
        else:
            market_line = html.escape(market_title)

        wallet = t.taker_address or t.maker_address or "unknown"
        wallet_short = (
            f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet
        )
        outcome = (t.outcome or "?").upper()
        pct = round(t.price * 100)

        lines.append(f"<b>{i}. Score {b.total}</b> · {market_line}")
        lines.append(
            f"   {outcome} @ {pct}¢ · <b>${t.size_usd:,.0f}</b> · "
            f"<code>{html.escape(wallet_short)}</code>"
        )

    if remainder:
        n = len(remainder)
        lines.append("")
        lines.append(
            f"<i>+{n} more signal{'s' if n != 1 else ''} not shown</i>"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------

async def digest_loop(buffer: DigestBuffer, dry_run: bool = False) -> None:
    """
    Long-running coroutine. Drains the DigestBuffer every
    DIGEST_INTERVAL_SECONDS and sends a digest message to Telegram.
    Designed to run as an asyncio.Task under the supervised_task wrapper.

    Persistence contract: every entry that is drained has insert_alert_outcome()
    called before the Telegram send so that resolution tracking is complete
    regardless of Telegram delivery success.
    """
    interval = config.DIGEST_INTERVAL_SECONDS
    log.info("[Digest] Started (interval=%ds, dry_run=%s)", interval, dry_run)

    sender = TelegramSender(
        token=config.TELEGRAM_BOT_TOKEN,
        chat_id=config.TELEGRAM_CHAT_ID,
    )

    async with httpx.AsyncClient() as client:
        while True:
            await asyncio.sleep(interval)

            entries = await buffer.drain()

            if not entries:
                log.debug("[Digest] No signals buffered this cycle — skipping")
                continue

            log.info("[Digest] Flushing %d buffered signal(s)", len(entries))

            # Outcome rows were already inserted by process_trade() at the
            # moment each signal was buffered — no persistence work needed here.

            text = format_digest(entries)
            if not text:
                continue

            if dry_run:
                log.info(
                    "[DRY-RUN] Would send digest (%d signals):\n%s",
                    len(entries),
                    text.replace("<b>", "").replace("</b>", "")
                        .replace("<i>", "").replace("</i>", "")
                        .replace("<code>", "").replace("</code>", ""),
                )
            else:
                ok = await sender.send_message(text, client)
                if ok:
                    log.info("[Digest] Sent digest (%d signals)", len(entries))
                else:
                    log.error("[Digest] Failed to send digest")
