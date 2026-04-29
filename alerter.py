"""
alerter.py — Telegram alert queue and message formatting.

Design:
  • A single asyncio.Queue feeds a worker that drains it at a rate of
    no more than 1 message per TELEGRAM_RATE_LIMIT_SECONDS (1.5s).
  • Messages are formatted in HTML (parse_mode=HTML) — safer than
    MarkdownV2 because HTML special chars in market names won't break
    the parser. MarkdownV2 requires escaping almost every punctuation
    character and Polymarket market titles are unpredictable.
  • Link previews are disabled (Polygon addresses expand into ugly previews).
  • In --dry-run mode, messages are logged to stdout but not sent.
  • The queue is unbounded; Railway memory is the only hard limit.
    In practice the alert rate is well under 1/minute.
"""

import asyncio
import html
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

import config
import database
from scorer import ScoreBreakdown
from trade_monitor import Trade
from wallet_profiler import WalletProfile

log = logging.getLogger("alerter")

# ---------------------------------------------------------------------------
# Alert message data
# ---------------------------------------------------------------------------

@dataclass
class AlertPayload:
    trade: Trade
    profile: WalletProfile
    breakdown: ScoreBreakdown
    market_title: str
    market_end_date: Optional[str]   # ISO-8601 string or None
    hours_to_resolution: Optional[float]
    market_slug: Optional[str] = None   # Polymarket URL slug for direct link


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_wallet_age(days: Optional[float]) -> str:
    if days is None:
        return "Unknown"
    if days < 7:
        return f"{days:.0f} days (NEW)"
    if days < 30:
        return f"{days:.0f} days"
    if days < 365:
        return f"{days / 30:.1f} months"
    return f"{days / 365:.1f} years"


def _fmt_actionability_label(hours_to_resolution: Optional[float]) -> str:
    """
    Return an HTML line conveying how much time remains to act on the alert.
    Tiers match the pre-scorer filter thresholds so the label is always
    consistent with what gets through the filter.
    """
    if hours_to_resolution is None:
        return "🟢 <b>ACTIONABLE</b> — window unknown"
    minutes = hours_to_resolution * 60
    if minutes < 0:
        return "⚫ <b>EXPIRED</b>"
    if minutes < 10:
        return "⚫ <b>EXPIRED</b>"
    if minutes < 30:
        return f"🔴 <b>CLOSING</b> — {minutes:.0f}m left"
    if minutes < 120:
        h = int(minutes // 60)
        m = int(minutes % 60)
        if h > 0:
            return f"🟡 <b>ACT FAST</b> — {h}h {m}m window"
        return f"🟡 <b>ACT FAST</b> — {minutes:.0f}m window"
    h = int(minutes // 60)
    m = int(minutes % 60)
    return f"🟢 <b>ACTIONABLE</b> — {h}h {m}m window"


def _score_bar(score: int, width: int = 20) -> str:
    """Proportional fill bar scaled to 110 max (100 pts + 10 cluster bonus)."""
    filled = max(0, min(width, round(score / 110 * width)))
    return "█" * filled + "░" * (width - filled)


def _fmt_breakdown(b: "ScoreBreakdown") -> str:
    """
    Monospace breakdown table for insertion in a <code> block.
    Shows all components. Notes are truncated to keep lines short.
    """
    rows = [
        ("Timing",   b.timing,            config.SCORE_MAX_TIMING,            b.timing_note),
        ("Funding",  b.funding_velocity,   config.SCORE_MAX_FUNDING_VELOCITY,  b.funding_velocity_note),
        ("Win Rate", b.win_rate,           config.SCORE_MAX_WIN_RATE,          b.win_rate_note),
        ("Size",     b.size_anomaly,       config.SCORE_MAX_SIZE_ANOMALY,      b.size_anomaly_note),
        ("Age",      b.wallet_age,         config.SCORE_MAX_WALLET_AGE,        b.wallet_age_note),
        ("Conc",     b.concentration,      config.SCORE_MAX_CONCENTRATION,     b.concentration_note),
    ]
    if config.SCORE_MAX_UNDERDOG > 0:
        rows.append(("Underdog", b.underdog, config.SCORE_MAX_UNDERDOG, b.underdog_note))
    lines = []
    for label, score, max_pts, note in rows:
        score_str = f"{score}/{max_pts}" if score is not None else f"—/{max_pts}"
        # Truncate long notes to keep the table readable in Telegram
        note_short = note[:40] if note else ""
        lines.append(f"{label:<10} {score_str:>5}  {note_short}")
    if b.cluster_bonus > 0:
        lines.append(f"{'Cluster':<10}   +{b.cluster_bonus}  {b.cluster_note[:40]}")
    lines.append(f"{'TOTAL':<10} {b.total:>5}")
    return "\n".join(lines)


def format_alert(payload: AlertPayload) -> str:
    """
    Build the full Telegram HTML message for an alert.
    Uses HTML parse_mode — escape user-controlled strings with html.escape().

    Layout:
      • Header: score, resolution countdown, proportional bar
      • Market title + Polymarket link
      • Bet: side, price, size, payout if wins
      • Score breakdown: monospace table (all 6 components + cluster bonus)
      • Wallet: full copyable address, Polygonscan link, age + volume summary
    """
    t = payload.trade
    p = payload.profile
    b = payload.breakdown

    outcome_label = (t.outcome or "UNKNOWN").upper()
    wallet_addr = t.taker_address or t.maker_address or "unknown"
    polygonscan_url = f"https://polygonscan.com/address/{wallet_addr}"
    market_title = html.escape(payload.market_title or "Unknown Market")
    pct = round(t.price * 100)

    # Resolution countdown
    h = payload.hours_to_resolution
    if h is None:
        res_str = "close unknown"
    elif h < 0:
        res_str = "CLOSED"
    elif h < 1:
        res_str = f"{int(h * 60)}m to close"
    elif h < 24:
        res_str = f"{h:.1f}h to close"
    elif h < 168:
        res_str = f"{h / 24:.1f}d to close"
    else:
        res_str = f"{h / 24:.0f}d to close"

    bar = _score_bar(b.total)

    lines: list[str] = [
        f"🚨 <b>INSIDER SIGNAL</b>  •  Score: <b>{b.total}</b>  •  ⏰ {res_str}",
        f"<code>{bar}</code>",
        _fmt_actionability_label(payload.hours_to_resolution),
        "",
        f"<b>{market_title}</b>",
    ]

    if payload.market_slug:
        polymarket_url = f"https://polymarket.com/event/{payload.market_slug}"
        lines.append(f'<a href="{polymarket_url}">🔮 View on Polymarket</a>')
    lines.append("")

    # Bet details with potential payout
    if 0 < t.price < 1:
        profit = t.size_usd * (1 - t.price) / t.price
        payout_str = f"  •  profit ${profit:,.0f} if {outcome_label} wins"
    else:
        payout_str = ""
    lines.append(f"<b>Bet:</b> {outcome_label} @ {pct}¢  •  <b>${t.size_usd:,.0f}</b>{payout_str}")
    lines.append("")

    # Score breakdown — monospace table
    lines.append("<b>Score breakdown:</b>")
    lines.append(f"<code>{_fmt_breakdown(b)}</code>")
    lines.append("")

    # Wallet — full address so users can verify on-chain independently
    lines += [
        "<b>Wallet</b>",
        f"<code>{html.escape(wallet_addr)}</code>",
        f'<a href="{polygonscan_url}">🔍 View on Polygonscan</a>',
        f"<i>{_fmt_wallet_age(p.wallet_age_days)} · {p.total_trades} trades · ${p.total_volume_usd:,.0f} total volume</i>",
    ]

    if not p.profile_complete:
        missing_str = ", ".join(p.missing_components)
        lines.append(f"<i>⚠️ Partial profile — {html.escape(missing_str)} unavailable</i>")

    lines.append("")
    lines.append(f"<i>{t.source.upper()} · {html.escape(t.trade_id[:20])}...</i>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram sender
# ---------------------------------------------------------------------------

class TelegramSender:
    """Low-level async Telegram message sender."""

    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = chat_id
        self._base_url = f"https://api.telegram.org/bot{token}"

    async def send_message(self, text: str, client: httpx.AsyncClient) -> bool:
        """
        Send a Telegram message. Returns True on success, False on failure.
        Logs the error but does NOT raise — a failed alert should never crash
        the bot.
        """
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": True},
        }

        backoff = 2.0
        for attempt in range(1, config.HTTP_MAX_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{self._base_url}/sendMessage",
                    json=payload,
                    timeout=config.HTTP_TIMEOUT_SECONDS,
                )

                if resp.status_code == 429:
                    # Telegram rate limit — honour the retry_after header.
                    retry_after = float(
                        resp.json().get("parameters", {}).get("retry_after", 5)
                    )
                    log.warning(
                        "Telegram rate-limited — sleeping %.1fs (attempt %d/%d)",
                        retry_after, attempt, config.HTTP_MAX_RETRIES,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                resp.raise_for_status()
                return True

            except httpx.HTTPStatusError as exc:
                log.error(
                    "Telegram HTTP error attempt %d/%d: %s — body: %s",
                    attempt, config.HTTP_MAX_RETRIES,
                    exc.response.status_code,
                    exc.response.text[:200],
                )
            except Exception as exc:
                log.error(
                    "Telegram send error attempt %d/%d: %s",
                    attempt, config.HTTP_MAX_RETRIES, exc,
                )

            if attempt < config.HTTP_MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

        log.error("Telegram: all %d send attempts failed", config.HTTP_MAX_RETRIES)
        return False

    async def send_document(
        self,
        file_path: str,
        filename: str,
        client: httpx.AsyncClient,
        caption: Optional[str] = None,
    ) -> bool:
        """
        Send a file to Telegram via sendDocument (multipart POST).
        Returns True on success, False on failure.
        Uses the same retry/backoff pattern as send_message().
        """
        backoff = 2.0
        for attempt in range(1, config.HTTP_MAX_RETRIES + 1):
            try:
                with open(file_path, "rb") as fh:
                    form_data: dict = {"chat_id": self._chat_id}
                    if caption:
                        form_data["caption"] = caption
                        form_data["parse_mode"] = "HTML"
                    resp = await client.post(
                        f"{self._base_url}/sendDocument",
                        data=form_data,
                        files={"document": (filename, fh, "text/csv")},
                        timeout=config.HTTP_TIMEOUT_SECONDS,
                    )

                if resp.status_code == 429:
                    retry_after = float(
                        resp.json().get("parameters", {}).get("retry_after", 5)
                    )
                    log.warning(
                        "Telegram rate-limited (document) — sleeping %.1fs (attempt %d/%d)",
                        retry_after, attempt, config.HTTP_MAX_RETRIES,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                resp.raise_for_status()
                return True

            except httpx.HTTPStatusError as exc:
                log.error(
                    "Telegram document HTTP error attempt %d/%d: %s — body: %s",
                    attempt, config.HTTP_MAX_RETRIES,
                    exc.response.status_code,
                    exc.response.text[:200],
                )
            except Exception as exc:
                log.error(
                    "Telegram document send error attempt %d/%d: %s",
                    attempt, config.HTTP_MAX_RETRIES, exc,
                )

            if attempt < config.HTTP_MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

        log.error("Telegram: all %d document send attempts failed", config.HTTP_MAX_RETRIES)
        return False


# ---------------------------------------------------------------------------
# Alert queue worker
# ---------------------------------------------------------------------------

class AlertQueue:
    """
    FIFO queue with rate-limited drain to Telegram.

    Usage:
        queue = AlertQueue()
        await queue.enqueue(payload)   # from trade processor
        await queue.run()              # long-running drain loop
    """

    def __init__(self, dry_run: bool = False):
        self._queue: asyncio.Queue[AlertPayload] = asyncio.Queue()
        self._dry_run = dry_run
        self._sender = TelegramSender(
            token=config.TELEGRAM_BOT_TOKEN,
            chat_id=config.TELEGRAM_CHAT_ID,
        )
        self._last_sent_at: float = 0.0

    async def enqueue(self, payload: AlertPayload) -> None:
        """Add an alert to the queue."""
        await self._queue.put(payload)
        log.debug("Alert enqueued (queue depth: %d)", self._queue.qsize())

    async def run(self) -> None:
        """
        Drain the queue indefinitely, respecting TELEGRAM_RATE_LIMIT_SECONDS.
        Run this as an asyncio.Task.
        """
        log.info(
            "Alert queue worker started (rate=%.1fs, dry_run=%s)",
            config.TELEGRAM_RATE_LIMIT_SECONDS,
            self._dry_run,
        )
        async with httpx.AsyncClient() as client:
            while True:
                payload = await self._queue.get()
                await self._process(payload, client)
                self._queue.task_done()

    async def _process(self, payload: AlertPayload, client: httpx.AsyncClient) -> None:
        """Format and send (or dry-run log) a single alert."""
        try:
            text = format_alert(payload)
        except Exception as exc:
            log.error("Failed to format alert: %s", exc, exc_info=True)
            return

        t = payload.trade
        b = payload.breakdown

        # Deduplication: never send two alerts for the same trade ID.
        if database.has_alert_been_sent_for_trade(t.trade_id):
            log.warning(
                "Skipping duplicate alert for trade %s (already in history)",
                t.trade_id,
            )
            return

        # Enforce rate limit.
        now = time.monotonic()
        gap = now - self._last_sent_at
        if gap < config.TELEGRAM_RATE_LIMIT_SECONDS:
            sleep_for = config.TELEGRAM_RATE_LIMIT_SECONDS - gap
            log.debug("Rate limiting: sleeping %.2fs before send", sleep_for)
            await asyncio.sleep(sleep_for)

        sent = False

        if self._dry_run:
            log.info(
                "[DRY-RUN] Would send alert (score=%d, trade=%s):\n%s",
                b.total, t.trade_id,
                # Strip HTML tags for clean log output
                text.replace("<b>", "").replace("</b>", "")
                    .replace("<i>", "").replace("</i>", "")
                    .replace("<code>", "").replace("</code>", ""),
            )
            sent = False  # dry-run: recorded as not sent
        else:
            log.info(
                "Sending alert: score=%d market=%s wallet=%s",
                b.total, t.market_id, t.taker_address,
            )
            sent = await self._sender.send_message(text, client)
            if sent:
                log.info("Alert sent successfully for trade %s", t.trade_id)
            else:
                log.error("Alert failed to send for trade %s", t.trade_id)

        self._last_sent_at = time.monotonic()

        # Persist to alert history regardless of send success.
        try:
            database.save_alert(
                trade_id=t.trade_id,
                market_id=t.market_id,
                wallet_address=t.taker_address or t.maker_address or "",
                score=b.total,
                score_breakdown=b.to_dict(),
                alert_text=text,
                sent=sent,
            )
        except Exception as exc:
            log.error("Failed to save alert to database: %s", exc)

        # Record outcome row for closed-loop resolution tracking.
        # Failures here must never affect alert delivery — log and move on.
        try:
            import json as _json
            database.insert_alert_outcome(
                alert_id=t.trade_id,
                market_id=t.market_id,
                market_question=payload.market_title or "",
                wallet_address=t.taker_address or t.maker_address or "",
                score=b.total,
                score_breakdown_json=_json.dumps(b.to_dict()),
                bet_side=t.outcome or "UNKNOWN",
                bet_price_at_alert=t.price,
                bet_size_usd=t.size_usd,
            )
            log.debug(
                "Outcome row inserted for trade %s (market=%s bet=%s @ %.3f)",
                t.trade_id, t.market_id, t.outcome, t.price,
            )
        except Exception as exc:
            log.error("Failed to insert alert outcome for trade %s: %s", t.trade_id, exc)
