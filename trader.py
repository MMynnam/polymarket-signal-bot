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
import html
import json
import logging
import time
from typing import Optional

import httpx

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

# Wallet address derived at startup (never contains the private key).
_wallet_address: str = ""

# Set to True after the graduation-to-dynamic-sizing Telegram notification fires.
# Resets on restart — acceptable per spec ("module-level variable" is explicit).
_graduation_notified: bool = False

# ---------------------------------------------------------------------------
# USDC contract constants (Polygon — bridged USDC.e)
# ---------------------------------------------------------------------------

_USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_USDC_DECIMALS = 6
_USDC_BALANCE_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]
_USDC_TRANSFER_ABI = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Minimum MATIC balance required to send a Polygon transaction.
_SWEEP_MIN_MATIC: float = 0.01


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
# On-chain balance helpers (synchronous cores wrapped in asyncio.to_thread)
# ---------------------------------------------------------------------------

def _get_usdc_balance_sync() -> float:
    """Read USDC.e balanceOf(_wallet_address) on Polygon (free view call)."""
    from web3 import Web3
    rpc = config.ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(_USDC_CONTRACT),
        abi=_USDC_BALANCE_ABI,
    )
    raw = contract.functions.balanceOf(
        Web3.to_checksum_address(_wallet_address)
    ).call()
    return raw / (10 ** _USDC_DECIMALS)


async def _get_usdc_balance() -> float:
    return await asyncio.to_thread(_get_usdc_balance_sync)


def _get_matic_balance_sync() -> float:
    """Read native MATIC balance of _wallet_address (free view call)."""
    from web3 import Web3
    rpc = config.ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    raw = w3.eth.get_balance(Web3.to_checksum_address(_wallet_address))
    return float(w3.from_wei(raw, "ether"))


# ---------------------------------------------------------------------------
# Dynamic position sizing
# ---------------------------------------------------------------------------

async def _calculate_bet_size() -> float:
    """
    Auto-graduating bet size. Three states (no manual toggle):

    WARMUP  — fewer than TRADING_DYNAMIC_MIN_RESOLVED resolved trades.
              Uses fixed TRADING_BET_SIZE_USDC.

    FIXED   — enough resolved trades but cumulative P&L ≤ 0.
              Uses fixed TRADING_BET_SIZE_USDC (circuit breaker).

    DYNAMIC — enough resolved trades AND cumulative P&L > 0.
              Uses balance × TRADING_BET_PERCENTAGE, clamped to [MIN, MAX].
              Fires a one-time graduation Telegram notification on first entry.
    """
    global _graduation_notified

    stats = database.get_trade_stats()
    resolved = stats.get("resolved", 0)
    pnl = stats.get("total_pnl") or 0.0

    # --- WARMUP ---
    if resolved < config.TRADING_DYNAMIC_MIN_RESOLVED:
        log.info(
            "[Trader] Warmup phase: %d/%d resolved trades. Fixed $%.2f sizing.",
            resolved, config.TRADING_DYNAMIC_MIN_RESOLVED, config.TRADING_BET_SIZE_USDC,
        )
        return config.TRADING_BET_SIZE_USDC

    # --- FIXED (circuit breaker: unprofitable despite sufficient data) ---
    if pnl <= 0:
        log.info(
            "[Trader] %d resolved trades but P&L is $%.2f. Staying on fixed $%.2f until profitable.",
            resolved, pnl, config.TRADING_BET_SIZE_USDC,
        )
        return config.TRADING_BET_SIZE_USDC

    # --- DYNAMIC ---
    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning(
            "[Trader] USDC balance fetch failed for dynamic sizing: %s — using fixed $%.2f",
            exc, config.TRADING_BET_SIZE_USDC,
        )
        return config.TRADING_BET_SIZE_USDC

    raw_size = balance * config.TRADING_BET_PERCENTAGE
    clamped = max(config.TRADING_MIN_BET_USDC, min(config.TRADING_MAX_BET_USDC, raw_size))

    log.info(
        "[Trader] Dynamic sizing active: $%.2f × %.1f%% = $%.2f",
        balance, config.TRADING_BET_PERCENTAGE * 100, clamped,
    )

    # One-time graduation notification (module-level flag resets on restart).
    if not _graduation_notified:
        _graduation_notified = True
        await _notify_graduation(resolved, pnl, clamped)

    return clamped


async def _notify_graduation(resolved: int, pnl: float, bet_size: float) -> None:
    """Fire once when the bot graduates from fixed to dynamic sizing."""
    if config.DRY_RUN:
        return

    from alerter import TelegramSender

    try:
        text = (
            "📈 <b>TRADING UPGRADE</b>\n\n"
            "The bot has graduated to dynamic position sizing.\n"
            f"✅ {resolved} trades resolved\n"
            f"✅ Cumulative P&amp;L: +${pnl:.2f}\n"
            f"📊 Now sizing at {config.TRADING_BET_PERCENTAGE * 100:.1f}% of bankroll per trade\n\n"
            "<i>Bet sizes will scale with performance.</i>"
        )
        sender = TelegramSender(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
        async with httpx.AsyncClient() as client:
            await sender.send_message(text, client)
    except Exception as exc:
        log.warning("[Trader] Failed to send graduation notification: %s", exc)


# ---------------------------------------------------------------------------
# Vault sweep
# ---------------------------------------------------------------------------

def _send_usdc_sync(to_address: str, amount_usdc: float) -> str:
    """Transfer USDC.e on Polygon. Returns tx hash hex string."""
    from web3 import Web3
    from eth_account import Account

    rpc = config.ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))

    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(_USDC_CONTRACT),
        abi=_USDC_TRANSFER_ABI,
    )
    amount_raw = int(amount_usdc * (10 ** _USDC_DECIMALS))
    from_addr = Web3.to_checksum_address(_wallet_address)

    tx = usdc.functions.transfer(
        Web3.to_checksum_address(to_address),
        amount_raw,
    ).build_transaction({
        "from":     from_addr,
        "nonce":    w3.eth.get_transaction_count(from_addr),
        "gas":      100_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  config.TRADING_CHAIN_ID,
    })

    account = Account.from_key(config.TRADING_PRIVATE_KEY)
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return tx_hash.hex()


async def _check_and_sweep() -> None:
    """
    If USDC balance exceeds VAULT_SWEEP_THRESHOLD_USDC, transfer the excess
    (balance − VAULT_SWEEP_FLOOR_USDC) to VAULT_WALLET_ADDRESS.

    Safety checks before any transfer:
      1. VAULT_WALLET_ADDRESS must be set — the only gate (no separate enable flag).
      2. Sweep amount is always balance − floor, never the full balance.
      3. Open-position check: floor must cover worst-case committed capital.
      4. MATIC gas check: skip if wallet cannot pay gas.
    """
    if not config.VAULT_WALLET_ADDRESS:
        return

    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning("[Vault] Could not fetch USDC balance: %s — skipping sweep", exc)
        return

    if balance <= config.VAULT_SWEEP_THRESHOLD_USDC:
        log.debug("[Vault] Balance $%.2f ≤ threshold $%.2f — no sweep", balance, config.VAULT_SWEEP_THRESHOLD_USDC)
        return

    sweep_amount = balance - config.VAULT_SWEEP_FLOOR_USDC
    if sweep_amount <= 0:
        return

    # Open-position safety: ensure floor still covers worst-case committed capital.
    open_positions = database.get_open_position_count()
    committed = open_positions * config.TRADING_MAX_BET_USDC
    remaining_after = balance - sweep_amount
    if remaining_after < committed:
        reduced = max(0.0, balance - max(config.VAULT_SWEEP_FLOOR_USDC, committed))
        if reduced <= 0:
            log.info(
                "[Vault] Skipping sweep — $%.2f committed across %d open positions, floor insufficient",
                committed, open_positions,
            )
            return
        log.info("[Vault] Reducing sweep from $%.2f to $%.2f due to open positions", sweep_amount, reduced)
        sweep_amount = reduced

    # Gas check.
    try:
        matic = await asyncio.to_thread(_get_matic_balance_sync)
        if matic < _SWEEP_MIN_MATIC:
            log.warning("[Vault] Insufficient MATIC for gas (%.4f MATIC < %.4f) — skipping sweep", matic, _SWEEP_MIN_MATIC)
            return
    except Exception as exc:
        log.warning("[Vault] MATIC balance check failed: %s — skipping sweep", exc)
        return

    log.info(
        "[Vault] Balance $%.2f exceeds threshold $%.2f. Sweeping $%.2f to vault.",
        balance, config.VAULT_SWEEP_THRESHOLD_USDC, sweep_amount,
    )

    try:
        is_first_sweep = database.get_vault_sweep_stats()["sweep_count"] == 0
        tx_hash = await asyncio.to_thread(_send_usdc_sync, config.VAULT_WALLET_ADDRESS, sweep_amount)
        remaining = balance - sweep_amount
        log.info("[Vault] Sweep complete: $%.2f → %s... (tx: %s)", sweep_amount, config.VAULT_WALLET_ADDRESS[:10], tx_hash)

        database.log_vault_sweep(
            amount_usdc=sweep_amount,
            balance_before=balance,
            balance_after=remaining,
            vault_address=config.VAULT_WALLET_ADDRESS,
            tx_hash=tx_hash,
        )
        if is_first_sweep:
            await _notify_first_sweep(sweep_amount, remaining, tx_hash)
        else:
            await _notify_sweep(sweep_amount, remaining, tx_hash)
    except Exception as exc:
        log.error("[Vault] Sweep failed: %s", exc, exc_info=True)


async def _notify_sweep(sweep_amount: float, remaining: float, tx_hash: str) -> None:
    """Send a Telegram notification about a completed vault sweep."""
    if config.DRY_RUN:
        return

    from alerter import TelegramSender

    try:
        def _short(addr: str) -> str:
            return f"{addr[:10]}...{addr[-6:]}" if len(addr) > 16 else addr

        vault_addr = config.VAULT_WALLET_ADDRESS
        text = (
            "🏦 <b>PROFIT SWEEP</b>\n\n"
            f"💸 <b>${sweep_amount:.2f}</b> USDC transferred to vault\n"
            f"📤 From: <code>{_short(_wallet_address)}</code>\n"
            f"📥 To: <code>{_short(vault_addr)}</code>\n\n"
            f"💰 Trading wallet balance: ${remaining:.2f}\n"
            f'🔗 <a href="https://polygonscan.com/tx/{tx_hash}">Verify transaction →</a>\n\n'
            f"<i>Profits secured. Trading continues with ${remaining:.2f}.</i>"
        )
        sender = TelegramSender(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
        async with httpx.AsyncClient() as client:
            await sender.send_message(text, client)
    except Exception as exc:
        log.warning("[Vault] Failed to send sweep notification: %s", exc)


async def _notify_first_sweep(sweep_amount: float, remaining: float, tx_hash: str) -> None:
    """Special notification for the very first vault sweep."""
    if config.DRY_RUN:
        return

    from alerter import TelegramSender

    try:
        text = (
            "🏦 <b>FIRST PROFIT SWEEP</b>\n\n"
            f"The bot's bankroll exceeded ${config.VAULT_SWEEP_THRESHOLD_USDC:.0f} for the first time.\n"
            "Profits are now being automatically secured.\n\n"
            f"💸 ${sweep_amount:.2f} transferred to vault\n"
            f"💰 Trading continues with ${remaining:.2f}\n"
            f'🔗 <a href="https://polygonscan.com/tx/{tx_hash}">Verify transaction →</a>\n\n'
            f"<i>Future sweeps happen automatically every hour when balance exceeds ${config.VAULT_SWEEP_THRESHOLD_USDC:.0f}.</i>"
        )
        sender = TelegramSender(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
        async with httpx.AsyncClient() as client:
            await sender.send_message(text, client)
    except Exception as exc:
        log.warning("[Vault] Failed to send first-sweep notification: %s", exc)


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

    from market_discovery import get_market_slug

    alert_id    = alert["alert_id"]
    market_id   = alert["market_id"]
    market_q    = alert["market_question"] or market_id
    bet_side    = alert["bet_side"]
    price_alert = alert["bet_price_at_alert"]
    score       = alert["score"]
    market_slug = get_market_slug(market_id) or ""

    # --- Calculate bet size (dynamic or fixed) ---
    bet_size = await _calculate_bet_size()

    # --- Resolve token ID ---
    token_id = _resolve_token_id(market_id, bet_side)
    if token_id is None:
        database.insert_trade_execution(
            alert_id=alert_id, market_id=market_id,
            market_question=market_q, clob_token_id="UNKNOWN",
            bet_side=bet_side, size_usdc=bet_size,
            status="failed", error_message="token ID resolution failed",
        )
        return

    # --- Log attempt before execution (crash safety) ---
    database.insert_trade_execution(
        alert_id=alert_id, market_id=market_id,
        market_question=market_q, clob_token_id=token_id,
        bet_side=bet_side, size_usdc=bet_size,
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
            amount=bet_size,
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
        bet_size, status,
        f"{fill_price:.4f}" if fill_price else "N/A",
        f"{slippage:.4f}" if slippage else "N/A",
    )

    # --- Telegram notification ---
    await _notify_trade(
        market_q=market_q,
        market_id=market_id,
        market_slug=market_slug,
        bet_side=bet_side,
        score=score,
        fill_price=fill_price or current_price or price_alert,
        size=bet_size,
        slippage=slippage,
        status=status,
        error_msg=error_msg,
    )


async def _notify_trade(
    market_q: str,
    market_id: str,
    market_slug: str,
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

    slippage_str = f"{slippage:.4f}" if slippage is not None else "N/A"

    if status == "filled":
        open_count = database.get_open_position_count()
        try:
            remaining_balance = await _get_usdc_balance()
        except Exception:
            remaining_balance = (
                config.TRADING_MAX_CONCURRENT_POSITIONS - open_count
            ) * config.TRADING_BET_SIZE_USDC

        addr = _wallet_address
        wallet_display = (
            f"{addr[:10]}...{addr[-6:]}" if len(addr) > 16 else addr
        ) if addr else "unknown"

        if market_slug:
            link_line = f'🔗 <a href="https://polymarket.com/event/{market_slug}">View on Polymarket</a>\n\n'
        else:
            link_line = ""

        text = (
            "💰💰💰 <b>LIVE TRADE</b> 💰💰💰\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📋 <b>Market:</b> {html.escape(str(market_q)[:120])}\n"
            f"{link_line}"
            f"🎯 <b>Position:</b> <b>{html.escape(str(bet_side))}</b> @ {fill_price:.3f}\n"
            f"💵 <b>Size:</b> ${size:.2f} USDC\n"
            f"📊 <b>Signal score:</b> {score}\n"
            f"📉 <b>Slippage:</b> {slippage_str}\n\n"
            f"💼 <b>Bankroll available:</b> ${remaining_balance:.2f}\n"
            f"📂 <b>Open positions:</b> {open_count}/{config.TRADING_MAX_CONCURRENT_POSITIONS}\n"
            f"👛 <b>Wallet:</b> <code>{wallet_display}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ <i>Automated trade. Not financial advice.</i>"
        )
    else:
        err_line = f"\n❗ <code>{html.escape(str(error_msg))}</code>" if error_msg else ""
        text = (
            f"❌ <b>TRADE {status.upper()}</b>\n"
            f"📋 {html.escape(str(market_q)[:100])}\n"
            f"🎯 {html.escape(str(bet_side))} @ {fill_price:.3f} | Score: {score}{err_line}"
        )

    try:
        # Feed v2: this terminal-style card is research/ops output (the fly trader posts
        # the audience-facing message). Ops channel or nothing — never the friends' feed.
        from alerter import make_research_sender
        sender = make_research_sender()
        if sender is None:
            log.info("[Trader] Trade notification suppressed (no research channel)")
            return
        async with httpx.AsyncClient() as client:
            await sender.send_message(text, client)
    except Exception as exc:
        log.warning("[Trader] Failed to send trade notification: %s", exc)


async def _notify_trade_resolution(te: dict, resolution_status: str, pnl: float, winning_outcome: Optional[str]) -> None:
    """Send a Telegram notification when a pending trade resolves."""
    if config.DRY_RUN:
        return

    from alerter import TelegramSender

    try:
        market_q    = te.get("market_question") or te.get("market_id", "")
        bet_side    = te.get("bet_side", "")
        fill_price  = te.get("bet_price_filled") or te.get("bet_price_intended") or 0.0

        if resolution_status == "won":
            result_emoji = "✅"
        elif resolution_status == "lost":
            result_emoji = "❌"
        else:
            result_emoji = "↩️"

        outcome_line = f"🏆 <b>Outcome:</b> {html.escape(str(winning_outcome))}\n" if winning_outcome else ""

        stats = database.get_trade_stats()
        if stats.get("total", 0) > 0 and stats.get("total_pnl") is not None:
            total_capital = stats["total"] * config.TRADING_BET_SIZE_USDC
            roi = stats["total_pnl"] / total_capital if total_capital > 0 else 0.0
            stats_line = (
                f"\n📈 <b>All-time stats:</b>\n"
                f"  Trades: {stats['total']} | "
                f"Won: {stats.get('won', 0)} | "
                f"Lost: {stats.get('lost', 0)}\n"
                f"  ROI: {roi:+.1%} | Net P&amp;L: ${stats['total_pnl']:+.2f}"
            )
        else:
            stats_line = ""

        text = (
            "🏁 <b>TRADE RESOLVED</b>\n\n"
            f"📋 <b>Market:</b> {html.escape(str(market_q)[:120])}\n"
            f"🎯 <b>Position:</b> {html.escape(str(bet_side))} @ {fill_price:.3f}\n"
            f"{outcome_line}"
            f"{result_emoji} <b>Result:</b> {resolution_status.upper()}\n"
            f"💰 <b>P&amp;L:</b> ${pnl:+.2f} USDC"
            f"{stats_line}"
        )

        # Feed v2: this 🏁 TRADE RESOLVED card DUPLICATED the fly trader's audience
        # settle for every resolution, in trading-terminal voice. It is research/ops
        # output now — ops channel or nothing, never the friends' feed.
        from alerter import make_research_sender
        sender = make_research_sender()
        if sender is None:
            log.info("[Trader] Resolution notification suppressed (no research channel)")
            return
        async with httpx.AsyncClient() as client:
            await sender.send_message(text, client)
    except Exception as exc:
        log.warning("[Trader] Failed to send resolution notification: %s", exc)


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
        await _notify_trade_resolution(te, resolution_status, pnl, winning_outcome)

    if resolved_count:
        log.info("[Trader] Resolution cycle: updated %d trade(s)", resolved_count)


# ---------------------------------------------------------------------------
# Startup status summary
# ---------------------------------------------------------------------------

async def _log_startup_status() -> None:
    """Log a human-readable summary of the current bot state at startup."""
    stats = database.get_trade_stats()
    resolved = stats.get("resolved", 0)
    pnl = stats.get("total_pnl") or 0.0
    sweep_stats = database.get_vault_sweep_stats()

    # Wallet display
    addr = _wallet_address
    wallet_display = f"{addr[:6]}...{addr[-4:]}" if len(addr) > 10 else addr

    # On-chain balance (best-effort)
    try:
        balance = await _get_usdc_balance()
        balance_str = f"${balance:.2f}"
    except Exception:
        balance_str = "(unavailable)"

    # Sizing state
    if resolved < config.TRADING_DYNAMIC_MIN_RESOLVED:
        sizing_str = (
            f"WARMUP (fixed ${config.TRADING_BET_SIZE_USDC:.2f}) — "
            f"{resolved}/{config.TRADING_DYNAMIC_MIN_RESOLVED} trades resolved"
        )
    elif pnl <= 0:
        sizing_str = (
            f"FIXED (${config.TRADING_BET_SIZE_USDC:.2f}) — "
            f"{resolved} resolved but P&L ${pnl:+.2f}, waiting for profitability"
        )
    else:
        try:
            bal = await _get_usdc_balance()
            bet = max(config.TRADING_MIN_BET_USDC,
                      min(config.TRADING_MAX_BET_USDC, bal * config.TRADING_BET_PERCENTAGE))
            sizing_str = (
                f"DYNAMIC ({config.TRADING_BET_PERCENTAGE * 100:.1f}% = ${bet:.2f} per trade)"
            )
        except Exception:
            sizing_str = f"DYNAMIC ({config.TRADING_BET_PERCENTAGE * 100:.1f}% of bankroll)"

    # Vault state
    if config.VAULT_WALLET_ADDRESS:
        total_swept = sweep_stats["total_swept"]
        vault_addr = config.VAULT_WALLET_ADDRESS
        vault_short = f"{vault_addr[:6]}...{vault_addr[-4:]}"
        if total_swept > 0:
            vault_str = f"ACTIVE — ${total_swept:.2f} swept to date (vault: {vault_short})"
        else:
            vault_str = (
                f"ACTIVE (vault: {vault_short}, "
                f"sweep above ${config.VAULT_SWEEP_THRESHOLD_USDC:.0f}, "
                f"floor ${config.VAULT_SWEEP_FLOOR_USDC:.0f})"
            )
    else:
        vault_str = "INACTIVE (no VAULT_WALLET_ADDRESS configured)"

    log.info("[Trader] === Trading Bot Status ===")
    log.info("[Trader] Wallet:  %s", wallet_display)
    log.info("[Trader] Balance: %s USDC", balance_str)
    log.info("[Trader] Sizing:  %s", sizing_str)
    log.info("[Trader] Vault:   %s", vault_str)
    log.info(
        "[Trader] Risk:    max $%.0f/day loss, max %d positions, pause after %d losses",
        config.TRADING_MAX_DAILY_LOSS_USDC,
        config.TRADING_MAX_CONCURRENT_POSITIONS,
        config.TRADING_CONSECUTIVE_LOSS_PAUSE,
    )


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
    global _wallet_address
    _wallet_address = _get_wallet_address()
    log.info("[Trader] Wallet address: %s", _wallet_address)
    log.info(
        "[Trader] Initialising CLOB client — host=%s chain=%d",
        config.TRADING_CLOB_HOST, config.TRADING_CHAIN_ID,
    )

    try:
        client = await _init_clob_client()
    except Exception as exc:
        log.critical("[Trader] Failed to initialise CLOB client: %s", exc)
        return

    await _log_startup_status()

    # On startup, look back 24h to catch any alerts from before the restart.
    last_processed_ts: int = int(time.time()) - 86400
    last_resolution_check: float = 0.0
    last_sweep_check: float = 0.0

    while True:
        try:
            # --- Resolution cycle (every 10 minutes) ---
            if time.time() - last_resolution_check >= _RESOLUTION_CHECK_INTERVAL_SECONDS:
                await _resolve_pending_trades()
                last_resolution_check = time.time()

            # --- Vault sweep cycle ---
            if time.time() - last_sweep_check >= config.VAULT_SWEEP_INTERVAL_SECONDS:
                await _check_and_sweep()
                last_sweep_check = time.time()

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
