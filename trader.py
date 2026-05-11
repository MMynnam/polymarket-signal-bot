"""
trader.py — Autonomous trading bot for the Polymarket Signal Bot.

Reads from alert_outcomes, applies risk controls, resolves CLOB token IDs,
and executes Fill-or-Kill market orders via py-clob-client.

Safety guarantees:
  • TRADING_ENABLED defaults to false — bot ships disabled.
  • Private key is NEVER logged. Only the derived wallet address is printed.
  • All trade attempts are logged to DB BEFORE execution.
  • Failed FOK orders are never retried — next alert brings fresh signal.
  • One trade per market maximum (is_market_already_traded guard).
  • All py-clob-client calls wrapped in asyncio.to_thread() — the client
    uses a synchronous httpx.Client internally.

Resolution cycle (every 10 minutes inside the main loop):
  Cross-references pending trade_executions against alert_outcomes. When an
  alert resolves, the execution row is updated with won/lost/invalid and P&L.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import config
import database

log = logging.getLogger("trader")

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

# Timestamp after which the consecutive-loss pause expires (0 = not paused).
_pause_until: float = 0.0

# How often (in seconds) to check pending trade resolution within the loop.
_RESOLUTION_CHECK_INTERVAL_SECONDS: int = 600  # 10 minutes


# ---------------------------------------------------------------------------
# CLOB client initialisation
# ---------------------------------------------------------------------------

async def _init_clob_client():
    """
    Build and authenticate a ClobClient.
    ClobClient.__init__ is synchronous and makes no HTTP calls.
    create_or_derive_api_creds() does make HTTP calls — run in thread.
    """
    from py_clob_client.client import ClobClient

    if not config.TRADING_PRIVATE_KEY:
        raise ValueError("TRADING_PRIVATE_KEY is not set")

    client = ClobClient(
        config.TRADING_CLOB_HOST,
        key=config.TRADING_PRIVATE_KEY,
        chain_id=config.TRADING_CHAIN_ID,
        signature_type=0,  # EOA wallet
    )
    creds = await asyncio.to_thread(client.create_or_derive_api_creds)
    client.set_api_creds(creds)
    return client


def _get_wallet_address() -> str:
    """Derive the wallet's public address from the private key (no HTTP)."""
    try:
        from eth_account import Account
        return Account.from_key(config.TRADING_PRIVATE_KEY).address
    except Exception as exc:
        log.warning("[Trader] Could not derive wallet address: %s", exc)
        return "<unknown>"


# ---------------------------------------------------------------------------
# Token ID resolution
# ---------------------------------------------------------------------------

def _resolve_token_id(market_id: str, bet_side: str) -> Optional[str]:
    """
    Map a bet_side label (e.g. "Yes", "No") to the correct CLOB token ID.

    The Gamma raw_json stores parallel arrays:
      outcomes      = ["Yes", "No"]
      clobTokenIds  = ["<token0>", "<token1>"]

    We match bet_side case-insensitively against outcomes and return the
    corresponding token ID. Falls back to token_ids[0] when outcomes list
    is absent (very rare; log a warning so it can be investigated).
    """
    market = database.get_market(market_id)
    if market is None:
        log.error("[Trader] Market %s not found in DB — cannot resolve token ID", market_id)
        return None

    token_ids: list[str] = market.get("clob_token_ids", [])
    if not token_ids:
        log.error("[Trader] Market %s has no clob_token_ids", market_id)
        return None

    raw = market.get("raw_json", {})
    outcomes: list[str] = raw.get("outcomes", [])

    if outcomes:
        target = bet_side.strip().lower()
        for i, outcome in enumerate(outcomes):
            if outcome.strip().lower() == target and i < len(token_ids):
                return token_ids[i]
        log.warning(
            "[Trader] bet_side '%s' not found in outcomes %s for market %s — "
            "falling back to token_ids[0]",
            bet_side, outcomes, market_id,
        )

    # Fallback: assume first token is "Yes" (standard binary market convention).
    return token_ids[0]


# ---------------------------------------------------------------------------
# Risk controls
# ---------------------------------------------------------------------------

async def _check_risk_limits() -> Optional[str]:
    """
    Return a rejection reason string if any risk limit is breached, else None.
    Manages the consecutive-loss pause using module-level _pause_until.
    """
    global _pause_until

    # Consecutive-loss pause.
    now = time.time()
    if _pause_until > 0:
        if now < _pause_until:
            remaining = int(_pause_until - now)
            return f"cooling down after consecutive losses ({remaining}s remaining)"
        else:
            log.info("[Trader] Consecutive-loss pause expired — resuming trading")
            _pause_until = 0.0

    consecutive = database.get_consecutive_losses()
    if consecutive >= config.TRADING_CONSECUTIVE_LOSS_PAUSE:
        _pause_until = now + config.TRADING_PAUSE_DURATION_SECONDS
        log.warning(
            "[Trader] %d consecutive losses — pausing for %ds",
            consecutive, config.TRADING_PAUSE_DURATION_SECONDS,
        )
        return f"pausing after {consecutive} consecutive losses"

    # Daily loss limit.
    daily_loss = database.get_daily_loss()
    if daily_loss >= config.TRADING_MAX_DAILY_LOSS_USDC:
        return (
            f"daily loss limit reached "
            f"(${daily_loss:.2f} >= ${config.TRADING_MAX_DAILY_LOSS_USDC:.2f})"
        )

    # Concurrent position limit.
    open_positions = database.get_open_position_count()
    if open_positions >= config.TRADING_MAX_CONCURRENT_POSITIONS:
        return (
            f"max concurrent positions reached "
            f"({open_positions}/{config.TRADING_MAX_CONCURRENT_POSITIONS})"
        )

    return None


# ---------------------------------------------------------------------------
# Trade execution
# ---------------------------------------------------------------------------

async def _execute_trade(client, alert: dict) -> None:
    """
    Execute a single Fill-or-Kill market order for `alert`.

    Flow:
      1. Resolve CLOB token ID for this market + bet_side.
      2. Log the attempt to DB (before execution — crash safety).
      3. Get current market price to compute intended slippage.
      4. Place FOK market order via py-clob-client.
      5. Parse response → update DB row with fill details.
      6. Send Telegram notification.
    """
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    from py_clob_client.exceptions import PolyApiException

    alert_id    = alert["alert_id"]
    market_id   = alert["market_id"]
    market_q    = alert["market_question"] or market_id
    bet_side    = alert["bet_side"]
    price_alert = alert["bet_price_at_alert"]
    score       = alert["score"]

    # --- Resolve token ID ---
    token_id = _resolve_token_id(market_id, bet_side)
    if token_id is None:
        database.insert_trade_execution(
            alert_id=alert_id, market_id=market_id,
            market_question=market_q, clob_token_id="UNKNOWN",
            bet_side=bet_side, size_usdc=config.TRADING_BET_SIZE_USDC,
            status="failed", error_message="token ID resolution failed",
        )
        return

    # --- Log attempt before execution (crash safety) ---
    database.insert_trade_execution(
        alert_id=alert_id, market_id=market_id,
        market_question=market_q, clob_token_id=token_id,
        bet_side=bet_side, size_usdc=config.TRADING_BET_SIZE_USDC,
        status="pending",
        bet_price_intended=price_alert,
    )

    # --- Get current ask price for slippage calculation ---
    current_price: Optional[float] = None
    try:
        price_resp = await asyncio.to_thread(client.get_price, token_id, "BUY")
        if isinstance(price_resp, dict):
            current_price = float(price_resp.get("price", price_alert))
        elif price_resp is not None:
            current_price = float(price_resp)
    except Exception as exc:
        log.warning("[Trader] Could not fetch current price for %s: %s — proceeding anyway", token_id, exc)
        current_price = price_alert

    slippage = abs(current_price - price_alert) if current_price is not None else None
    if slippage is not None and slippage > 0.05:
        log.warning(
            "[Trader] High slippage for alert %s: intended=%.3f current=%.3f slippage=%.4f",
            alert_id, price_alert, current_price, slippage,
        )

    # --- Execute FOK market order ---
    fill_price: Optional[float] = None
    order_id:   Optional[str]   = None
    status = "error"
    error_msg: Optional[str] = None

    try:
        order = MarketOrderArgs(
            token_id=token_id,
            amount=config.TRADING_BET_SIZE_USDC,
            side=BUY,
        )
        signed = client.create_market_order(order)
        resp   = await asyncio.to_thread(client.post_order, signed, OrderType.FOK)

        log.debug("[Trader] CLOB response for alert %s: %s", alert_id, resp)

        if isinstance(resp, dict):
            success   = resp.get("success", False)
            error_msg = resp.get("errorMsg") or None
            order_id  = resp.get("orderID") or resp.get("id")
            resp_status = (resp.get("status") or "").lower()

            if success or resp_status == "matched":
                status = "filled"
                # Try to extract fill price from response fields.
                fill_price = (
                    resp.get("price")
                    or resp.get("avgPrice")
                    or current_price
                    or price_alert
                )
                if fill_price:
                    fill_price = float(fill_price)
            else:
                status = "rejected"
                if not error_msg:
                    error_msg = f"CLOB returned status={resp.get('status', 'unknown')}"

                # Surface specific actionable errors.
                err_lower = (error_msg or "").lower()
                if "insufficient" in err_lower or "balance" in err_lower:
                    wallet = _get_wallet_address()
                    log.error(
                        "[Trader] Insufficient USDC balance — deposit USDC to %s", wallet
                    )
                elif "allowance" in err_lower or "approval" in err_lower:
                    log.error(
                        "[Trader] Token allowance not set. Run: "
                        "python trader.py --setup-allowances"
                    )
        else:
            status = "rejected"
            error_msg = f"unexpected CLOB response type: {type(resp)}"

    except PolyApiException as exc:
        status = "error"
        error_msg = str(exc)
        log.error("[Trader] CLOB API error for alert %s: %s", alert_id, exc)
    except Exception as exc:
        status = "error"
        error_msg = str(exc)
        log.error("[Trader] Unexpected error executing trade for alert %s: %s", alert_id, exc, exc_info=True)

    # --- Update DB row with execution result ---
    with database.transaction() as db:
        db.execute(
            """
            UPDATE trade_executions
            SET status = ?, bet_price_filled = ?, slippage = ?,
                order_id = ?, error_message = ?
            WHERE alert_id = ?
            """,
            (status, fill_price, slippage, order_id, error_msg, alert_id),
        )

    log.info(
        "[Trader] Trade %s | market='%.40s' | side=%s | score=%d | "
        "size=$%.2f | status=%s | fill_price=%s | slippage=%s",
        alert_id[:12], market_q, bet_side, score,
        config.TRADING_BET_SIZE_USDC, status,
        f"{fill_price:.4f}" if fill_price else "N/A",
        f"{slippage:.4f}" if slippage else "N/A",
    )

    # --- Telegram notification ---
    await _notify_trade(
        market_q=market_q,
        bet_side=bet_side,
        score=score,
        fill_price=fill_price or current_price or price_alert,
        size=config.TRADING_BET_SIZE_USDC,
        slippage=slippage,
        status=status,
        error_msg=error_msg,
    )


async def _notify_trade(
    market_q: str,
    bet_side: str,
    score: int,
    fill_price: float,
    size: float,
    slippage: Optional[float],
    status: str,
    error_msg: Optional[str],
) -> None:
    """Send a Telegram notification about the trade execution."""
    if config.DRY_RUN:
        return

    from alerter import TelegramSender

    status_emoji = "✅" if status == "filled" else "❌"
    slippage_str = f"{slippage:.4f}" if slippage is not None else "N/A"
    err_line = f"\nError: <code>{error_msg}</code>" if error_msg and status != "filled" else ""

    text = (
        f"🤖 <b>TRADE {status.upper()}</b> {status_emoji}\n"
        f"Market: {market_q[:80]}\n"
        f"Side: <b>{bet_side}</b> @ {fill_price:.4f}\n"
        f"Size: <b>${size:.2f} USDC</b>\n"
        f"Score: {score}\n"
        f"Slippage: {slippage_str}{err_line}"
    )

    try:
        sender = TelegramSender()
        await sender.send(text)
    except Exception as exc:
        log.warning("[Trader] Failed to send Telegram notification: %s", exc)


# ---------------------------------------------------------------------------
# Resolution cycle
# ---------------------------------------------------------------------------

async def _resolve_pending_trades() -> None:
    """
    Cross-reference pending trade_executions against alert_outcomes.
    When the alert resolves, update the trade with won/lost/invalid and P&L.
    """
    pending = database.get_pending_trade_executions()
    if not pending:
        return

    db = database.get_db()
    resolved_count = 0

    for te in pending:
        row = db.execute(
            "SELECT resolution_status, winning_outcome FROM alert_outcomes WHERE alert_id = ?",
            (te["alert_id"],),
        ).fetchone()

        if row is None:
            continue

        alert_status = row["resolution_status"]
        winning_outcome = row["winning_outcome"]

        if alert_status == "pending":
            continue  # Not resolved yet.

        fill_price = te.get("bet_price_filled") or te.get("bet_price_intended") or 0.5
        size_usdc  = te.get("size_usdc", config.TRADING_BET_SIZE_USDC)

        if alert_status == "resolved_won":
            resolution_status = "won"
            pnl = size_usdc * (1.0 / fill_price - 1.0)
        elif alert_status == "resolved_lost":
            resolution_status = "lost"
            pnl = -size_usdc
        else:  # resolved_invalid
            resolution_status = "invalid"
            pnl = 0.0

        database.update_trade_resolution(
            row_id=te["id"],
            resolution_status=resolution_status,
            pnl=pnl,
            resolved_at=int(time.time()),
        )
        log.info(
            "[Trader] Resolved trade %s → %s | P&L: $%+.2f",
            te["alert_id"][:12], resolution_status, pnl,
        )
        resolved_count += 1

    if resolved_count:
        log.info("[Trader] Resolution cycle: updated %d trade(s)", resolved_count)


# ---------------------------------------------------------------------------
# Main trading loop
# ---------------------------------------------------------------------------

async def trading_loop() -> None:
    """
    Supervised background task. Polls alert_outcomes every
    TRADING_POLL_INTERVAL_SECONDS for new tradeable alerts.

    Exits immediately when TRADING_ENABLED=false so it consumes no resources.
    """
    if not config.TRADING_ENABLED:
        log.info(
            "[Trader] Trading is DISABLED (TRADING_ENABLED=false). "
            "Set TRADING_ENABLED=true to activate live trading."
        )
        # Block forever so the supervisor doesn't restart this in a tight loop.
        # Task is cancelled cleanly on shutdown.
        while True:
            await asyncio.sleep(3600)

    # Derive and log wallet address — never log the private key.
    wallet_address = _get_wallet_address()
    log.info("[Trader] Wallet address: %s", wallet_address)
    log.info(
        "[Trader] Initialising CLOB client — host=%s chain=%d",
        config.TRADING_CLOB_HOST, config.TRADING_CHAIN_ID,
    )

    try:
        client = await _init_clob_client()
    except Exception as exc:
        log.critical("[Trader] Failed to initialise CLOB client: %s", exc)
        return

    log.info(
        "[Trader] Ready. min_score=%d bet_size=$%.2f max_daily_loss=$%.2f poll=%ds",
        config.TRADING_MIN_SCORE,
        config.TRADING_BET_SIZE_USDC,
        config.TRADING_MAX_DAILY_LOSS_USDC,
        config.TRADING_POLL_INTERVAL_SECONDS,
    )

    # On startup, look back 24h to catch any alerts from before the restart.
    last_processed_ts: int = int(time.time()) - 86400
    last_resolution_check: float = 0.0

    while True:
        try:
            # --- Resolution cycle (every 10 minutes) ---
            if time.time() - last_resolution_check >= _RESOLUTION_CHECK_INTERVAL_SECONDS:
                await _resolve_pending_trades()
                last_resolution_check = time.time()

            # --- Risk check ---
            block_reason = await _check_risk_limits()
            if block_reason:
                log.info("[Trader] Skipping cycle — %s", block_reason)
                await asyncio.sleep(config.TRADING_POLL_INTERVAL_SECONDS)
                continue

            # --- Fetch qualifying alerts ---
            alerts = database.get_tradeable_alerts(
                since_timestamp=last_processed_ts,
                min_score=config.TRADING_MIN_SCORE,
            )

            if alerts:
                log.info("[Trader] Found %d new alert(s) to evaluate", len(alerts))

            for alert in alerts:
                # Track the furthest timestamp we've seen.
                last_processed_ts = max(last_processed_ts, alert["created_at"])

                if database.is_market_already_traded(alert["market_id"]):
                    log.debug(
                        "[Trader] Market %s already has a trade — skipping alert %s",
                        alert["market_id"], alert["alert_id"][:12],
                    )
                    continue

                # Re-check risk before each individual trade (burst of alerts
                # could push us over the concurrent position limit mid-loop).
                block_reason = await _check_risk_limits()
                if block_reason:
                    log.info("[Trader] Mid-loop risk block: %s — halting batch", block_reason)
                    break

                await _execute_trade(client, alert)

        except asyncio.CancelledError:
            log.info("[Trader] Loop cancelled — shutting down")
            raise
        except Exception as exc:
            log.error("[Trader] Unexpected cycle error: %s", exc, exc_info=True)

        await asyncio.sleep(config.TRADING_POLL_INTERVAL_SECONDS)
