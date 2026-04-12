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

def _fmt_hours(hours: Optional[float]) -> str:
    """Format hours-to-resolution as human-readable string."""
    if hours is None:
        return "Unknown"
    if hours < 0:
        return "CLOSED"
    total_minutes = int(abs(hours) * 60)
    h, m = divmod(total_minutes, 60)
    if h >= 48:
        return f"{h // 24}d {h % 24}h"
    return f"{h}h {m}m"


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


def _fmt_address(address: str) -> str:
    """Truncate 0x address to 0x1234...abcd form."""
    if len(address) < 10:
        return address
    return f"{address[:6]}...{address[-4:]}"


def _score_bar(score: int) -> str:
    """Ascii-art score bar for quick visual scanning."""
    filled = round(score / 10)
    empty = 10 - filled
    return "█" * filled + "░" * empty


def _implied_prob_str(price: float) -> str:
    pct = round(price * 100)
    if price <= config.UNDERDOG_MAX_PRICE:
        return f"{pct}% implied underdog"
    if price >= config.UNDERDOG_MIN_PRICE:
        return f"{pct}% implied favorite"
    return f"{pct}% implied"


def _win_rate_str(profile: WalletProfile) -> str:
    if profile.win_rate is None:
        return "No history"
    return (
        f"{profile.resolved_trades} resolved, "
        f"{profile.win_count} wins "
        f"({profile.win_rate:.0%})"
    )


def _size_multiple_str(trade_size: float, profile: WalletProfile) -> str:
    if profile.median_bet_usd and profile.median_bet_usd > 0:
        multiple = trade_size / profile.median_bet_usd
        flag = " 🚩" if multiple >= 3 else ""
        return (
            f"${profile.median_bet_usd:,.0f} → THIS BET: "
            f"{multiple:.0f}x median{flag}"
        )
    return f"${trade_size:,.0f} (no prior history)"


def _score_component_line(label: str, score: Optional[int], max_pts: int, note: str) -> str:
    """Format one score breakdown line."""
    if score is None:
        score_str = "N/A"
    else:
        score_str = f"{score}/{max_pts}"
    return f"• {label}: <b>{score_str}</b> — {html.escape(note)}"


def _fmt_payout(size_usd: float, price: float, outcome: str) -> str:
    """
    Calculate potential profit if the bet wins.
    At price P, buying $S worth gives S/P shares. Profit = S*(1-P)/P.
    """
    if price <= 0 or price >= 1:
        return ""
    profit = size_usd * (1 - price) / price
    multiplier = 1 / price
    return f"+${profit:,.0f} profit ({multiplier:.1f}x return) if {outcome.upper()} wins"


def _fmt_breakdown_table(b: "ScoreBreakdown") -> str:
    """
    Compact monospace table of per-component scores for the <code> block.
    Skipped components (None) show a dash. Cluster bonus appended if non-zero.
    """
    rows = [
        ("Timing",    b.timing,       config.SCORE_MAX_TIMING),
        ("Win rate",  b.win_rate,     config.SCORE_MAX_WIN_RATE),
        ("Size",      b.size_anomaly, config.SCORE_MAX_SIZE_ANOMALY),
        ("Age",       b.wallet_age,   config.SCORE_MAX_WALLET_AGE),
        ("Conc",      b.concentration, config.SCORE_MAX_CONCENTRATION),
        ("Underdog",  b.underdog,     config.SCORE_MAX_UNDERDOG),
    ]
    lines = []
    for label, score, max_pts in rows:
        score_str = f"{score}/{max_pts}" if score is not None else f"—/{max_pts}"
        lines.append(f"{label:<10} {score_str:>5}")
    if b.cluster_bonus > 0:
        lines.append(f"{'Cluster':<10}   +{b.cluster_bonus}")
    lines.append(f"{'TOTAL':<10} {b.total:>5}")
    return "\n".join(lines)


def _signal_reasons(b: "ScoreBreakdown", p: "WalletProfile", t: "Trade") -> list[str]:
    """
    Translate score components into plain-English bullets.
    Only emit a bullet when the component actually contributed meaningfully.
    """
    reasons = []

    # Timing
    if b.timing is not None and b.timing >= 15:
        reasons.append(f"⚡ <b>Near resolution</b> — {html.escape(b.timing_note)}")
    elif b.timing is not None and b.timing >= 8:
        reasons.append(f"🕐 {html.escape(b.timing_note)}")

    # Win rate
    if b.win_rate is not None and b.win_rate >= 10:
        reasons.append(f"🏆 <b>{html.escape(b.win_rate_note)}</b>")
    elif b.win_rate is not None and b.win_rate > 0:
        reasons.append(f"📊 {html.escape(b.win_rate_note)}")

    # Size anomaly
    if b.size_anomaly is not None and b.size_anomaly >= 12:
        reasons.append(f"📈 <b>{html.escape(b.size_anomaly_note)}</b>")
    elif b.size_anomaly is not None and b.size_anomaly > 0:
        reasons.append(f"📈 {html.escape(b.size_anomaly_note)}")

    # Wallet age
    if b.wallet_age is not None and b.wallet_age >= 10:
        reasons.append(f"🆕 <b>Wallet age: {html.escape(b.wallet_age_note)}</b>")
    elif b.wallet_age is not None and b.wallet_age > 0:
        reasons.append(f"🆕 Wallet age: {html.escape(b.wallet_age_note)}")

    # Concentration
    if b.concentration is not None and b.concentration >= 7:
        reasons.append(f"🎯 <b>High conviction: {html.escape(b.concentration_note)}</b>")
    elif b.concentration is not None and b.concentration > 0:
        reasons.append(f"🎯 Concentration: {html.escape(b.concentration_note)}")

    # Underdog
    if b.underdog is not None and b.underdog >= 7:
        reasons.append(f"🐴 <b>Underdog bet: {html.escape(b.underdog_note)}</b>")
    elif b.underdog is not None and b.underdog > 0:
        reasons.append(f"🐴 {html.escape(b.underdog_note)}")

    # Cluster
    if b.cluster_bonus > 0:
        reasons.append(f"🔗 <b>Cluster-funded wallet</b> — coordinated activity detected")

    return reasons


def format_alert(payload: AlertPayload) -> str:
    """
    Build the full Telegram HTML message for an alert.
    Uses HTML parse_mode — escape user-controlled strings with html.escape().

    Design philosophy: every line should either help the reader decide
    whether to act, or help them verify the signal independently.
    """
    t = payload.trade
    p = payload.profile
    b = payload.breakdown

    outcome_label = t.outcome.upper() if t.outcome else "UNKNOWN"
    wallet_addr = t.taker_address or t.maker_address or "unknown"
    polygonscan_url = f"https://polygonscan.com/address/{wallet_addr}"
    market_title = html.escape(payload.market_title or "Unknown Market")
    resolution_str = _fmt_hours(payload.hours_to_resolution)
    bar = _score_bar(b.total)
    pct = round(t.price * 100)

    # --- Header ---
    lines = [
        f"🚨 <b>INSIDER SIGNAL</b>  |  Score: <b>{b.total}/100</b>  |  ⏰ {resolution_str}",
        f"<code>{bar}</code>",
        "",
    ]

    # --- Market ---
    lines.append(f"<b>{market_title}</b>")
    if payload.market_slug:
        polymarket_url = f"https://polymarket.com/event/{payload.market_slug}"
        lines.append(f'<a href="{polymarket_url}">🔮 View on Polymarket</a>')
    lines.append("")

    # --- The bet ---
    payout_str = _fmt_payout(t.size_usd, t.price, outcome_label)
    lines += [
        f"<b>Bet:</b> {outcome_label} @ {pct}¢  |  <b>${t.size_usd:,.0f}</b>",
    ]
    if payout_str:
        lines.append(f"<i>{payout_str}</i>")
    lines.append("")

    # --- Why this fired ---
    reasons = _signal_reasons(b, p, t)
    if reasons:
        lines.append("<b>Why this fired:</b>")
        lines.extend(f"  {r}" for r in reasons)
        lines.append("")

    # --- Score breakdown (numerical per-component table) ---
    lines.append("<b>Score breakdown:</b>")
    lines.append(f"<code>{_fmt_breakdown_table(b)}</code>")
    lines.append("")

    # --- Wallet (full address, copyable) ---
    lines += [
        "<b>Wallet</b>",
        f"<code>{html.escape(wallet_addr)}</code>",
        f'<a href="{polygonscan_url}">🔍 Verify on Polygonscan</a>',
    ]

    # Track record summary inline
    if p.win_rate is not None and p.resolved_trades > 0:
        lines.append(
            f"<i>{p.resolved_trades} resolved bets · {p.win_count} wins · "
            f"{p.win_rate:.0%} win rate · wallet age {_fmt_wallet_age(p.wallet_age_days)}</i>"
        )
    else:
        lines.append(f"<i>Wallet age: {_fmt_wallet_age(p.wallet_age_days)}</i>")

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
