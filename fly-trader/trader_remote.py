"""
trader_remote.py — Standalone Fly.io trader for Polymarket Signal Bot.

Polls the Railway API for tradeable alerts, executes FOK orders via
py-clob-client, and reports results back to Railway. Runs from Dublin,
Ireland (primary_region = "dub") to avoid Polymarket's US geo-block.

All state lives in Railway's SQLite DB accessed through the HTTP API —
this script has no local database.

Flow per cycle:
  1. Poll GET /api/alerts/tradeable for new qualifying alerts.
  2. Check risk limits via GET /api/stats/trading.
  3. Execute FOK market order via py-clob-client.
  4. Report result via POST /api/trades.
  5. Every 10 min: poll GET /api/trades/pending, detect resolutions,
     send Telegram notification, and PATCH the resolution back to Railway.
  6. Every VAULT_SWEEP_INTERVAL_SECONDS: check on-chain balance and sweep.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RAILWAY_API_URL: str = os.getenv("RAILWAY_API_URL", "").rstrip("/")
API_SECRET_KEY: str = os.getenv("API_SECRET_KEY", "")
TRADING_PRIVATE_KEY: str = os.getenv("TRADING_PRIVATE_KEY", "")
TRADING_BET_SIZE_USDC: float = float(os.getenv("TRADING_BET_SIZE_USDC", "2.0"))
TRADING_BET_PERCENTAGE: float = float(os.getenv("TRADING_BET_PERCENTAGE", "0.02"))
TRADING_MIN_BET_USDC: float = float(os.getenv("TRADING_MIN_BET_USDC", "1.0"))
TRADING_MAX_BET_USDC: float = float(os.getenv("TRADING_MAX_BET_USDC", "10.0"))
TRADING_MAX_DAILY_LOSS_USDC: float = float(os.getenv("TRADING_MAX_DAILY_LOSS_USDC", "10.0"))
TRADING_MAX_CONCURRENT_POSITIONS: int = int(os.getenv("TRADING_MAX_CONCURRENT_POSITIONS", "10"))
TRADING_CONSECUTIVE_LOSS_PAUSE: int = int(os.getenv("TRADING_CONSECUTIVE_LOSS_PAUSE", "3"))
TRADING_PAUSE_DURATION_SECONDS: int = int(os.getenv("TRADING_PAUSE_DURATION_SECONDS", "7200"))
TRADING_MIN_SCORE: int = int(os.getenv("TRADING_MIN_SCORE", "65"))
TRADING_DYNAMIC_MIN_RESOLVED: int = int(os.getenv("TRADING_DYNAMIC_MIN_RESOLVED", "20"))
POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", "30"))
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
ALCHEMY_RPC_URL: str = os.getenv("ALCHEMY_RPC_URL", "")

VAULT_WALLET_ADDRESS: str = os.getenv("VAULT_WALLET_ADDRESS", "")
VAULT_SWEEP_THRESHOLD_USDC: float = float(os.getenv("VAULT_SWEEP_THRESHOLD_USDC", "150.0"))
VAULT_SWEEP_FLOOR_USDC: float = float(os.getenv("VAULT_SWEEP_FLOOR_USDC", "110.0"))
VAULT_SWEEP_INTERVAL_SECONDS: int = int(os.getenv("VAULT_SWEEP_INTERVAL_SECONDS", "3600"))

REDEMPTION_CHECK_INTERVAL: int = int(os.getenv("REDEMPTION_CHECK_INTERVAL", "600"))
LOW_BALANCE_WARN_USD: float = float(os.getenv("LOW_BALANCE_WARN_USD", "10.0"))

TRADING_CLOB_HOST: str = "https://clob.polymarket.com"
TRADING_CHAIN_ID: int = 137

# Polymarket wallet type configuration.
# signature_type=0  EOA — use for a raw private key wallet created outside Polymarket
# signature_type=2  POLY_GNOSIS_SAFE — use for wallets created via Polymarket's web UI.
#   Requires TRADING_FUNDER_ADDRESS = the proxy/safe wallet address that holds USDC.
#   The private key signs on behalf of that address; TRADING_PRIVATE_KEY is the signer.
TRADING_SIGNATURE_TYPE: int = int(os.getenv("TRADING_SIGNATURE_TYPE", "0"))
TRADING_FUNDER_ADDRESS: str = os.getenv("TRADING_FUNDER_ADDRESS", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("trader_remote")

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_pause_until: float = 0.0
_wallet_address: str = ""
_graduation_notified: bool = False
_notified_resolutions: set[str] = set()
_last_resolution_check: float = 0.0
_last_sweep_check: float = 0.0
_last_redemption_check: float = 0.0
_redeemed_positions: set[str] = set()
_low_balance_warned: bool = False
_cached_usdc_balance: float = -1.0  # -1 = not yet fetched
_RESOLUTION_POLL_INTERVAL: int = 600

# ---------------------------------------------------------------------------
# USDC / web3 constants (Polygon — bridged USDC.e)
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
_SWEEP_MIN_MATIC: float = 0.01

# Polymarket CTF (Conditional Token Framework) contract — used for redemption.
# Same address for both regular and neg-risk markets on Polygon.
_CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_CTF_REDEEM_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {"X-API-Key": API_SECRET_KEY, "Content-Type": "application/json"}


async def _api_get(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[dict] = None,
) -> Optional[dict | list]:
    try:
        resp = await client.get(
            f"{RAILWAY_API_URL}{path}", headers=_headers(), params=params, timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        log.warning("[API] GET %s → %d: %s", path, resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("[API] GET %s failed: %s", path, exc)
    return None


async def _api_post(
    client: httpx.AsyncClient,
    path: str,
    body: dict,
) -> Optional[dict]:
    try:
        resp = await client.post(
            f"{RAILWAY_API_URL}{path}", headers=_headers(), json=body, timeout=15,
        )
        return resp.json()
    except Exception as exc:
        log.warning("[API] POST %s failed: %s", path, exc)
    return None


async def _api_patch(
    client: httpx.AsyncClient,
    path: str,
    body: dict,
) -> Optional[dict]:
    try:
        resp = await client.patch(
            f"{RAILWAY_API_URL}{path}", headers=_headers(), json=body, timeout=15,
        )
        return resp.json()
    except Exception as exc:
        log.warning("[API] PATCH %s failed: %s", path, exc)
    return None


# ---------------------------------------------------------------------------
# CLOB client
# ---------------------------------------------------------------------------

async def _init_clob_client():
    from py_clob_client_v2.client import ClobClient

    if not TRADING_PRIVATE_KEY:
        raise ValueError("TRADING_PRIVATE_KEY is not set")

    # signature_type=0 (EOA): raw private-key wallet, no proxy.
    # signature_type=2 (POLY_GNOSIS_SAFE): Polymarket web-UI wallet.
    #   Set TRADING_FUNDER_ADDRESS to the proxy/safe address that holds the USDC;
    #   TRADING_PRIVATE_KEY is the signer key that controls it.
    #   Without TRADING_FUNDER_ADDRESS, orders will carry the wrong maker address
    #   and will be rejected with order_version_mismatch.
    client = ClobClient(
        TRADING_CLOB_HOST,
        key=TRADING_PRIVATE_KEY,
        chain_id=TRADING_CHAIN_ID,
        signature_type=TRADING_SIGNATURE_TYPE,
        funder=TRADING_FUNDER_ADDRESS or None,
    )
    creds = await asyncio.to_thread(client.create_or_derive_api_key)
    client.set_api_creds(creds)
    log.info(
        "CLOB client initialised (sig_type=%d, funder=%s)",
        TRADING_SIGNATURE_TYPE,
        TRADING_FUNDER_ADDRESS[:10] + "..." if TRADING_FUNDER_ADDRESS else "self",
    )
    return client


def _get_wallet_address() -> str:
    try:
        from eth_account import Account
        return Account.from_key(TRADING_PRIVATE_KEY).address
    except Exception as exc:
        log.warning("Could not derive wallet address: %s", exc)
        return "<unknown>"


# ---------------------------------------------------------------------------
# On-chain balance helpers
# ---------------------------------------------------------------------------

def _get_usdc_balance_sync() -> float:
    from web3 import Web3
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
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
    from web3 import Web3
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    raw = w3.eth.get_balance(Web3.to_checksum_address(_wallet_address))
    return float(w3.from_wei(raw, "ether"))


# ---------------------------------------------------------------------------
# Dynamic position sizing
# ---------------------------------------------------------------------------

async def _calculate_bet_size(http_client: httpx.AsyncClient, stats: dict) -> float:
    """
    Three-state auto-graduating bet size using stats from the Railway API.
    WARMUP → FIXED (circuit breaker) → DYNAMIC (balance × percentage).
    """
    global _graduation_notified

    resolved = stats.get("resolved", 0)
    pnl = stats.get("cumulative_pnl") or 0.0

    if resolved < TRADING_DYNAMIC_MIN_RESOLVED:
        log.info(
            "[Sizing] Warmup: %d/%d resolved. Fixed $%.2f.",
            resolved, TRADING_DYNAMIC_MIN_RESOLVED, TRADING_BET_SIZE_USDC,
        )
        return TRADING_BET_SIZE_USDC

    if pnl <= 0:
        log.info(
            "[Sizing] %d resolved, P&L $%.2f. Fixed $%.2f until profitable.",
            resolved, pnl, TRADING_BET_SIZE_USDC,
        )
        return TRADING_BET_SIZE_USDC

    # DYNAMIC
    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning("[Sizing] USDC balance fetch failed: %s — fixed $%.2f", exc, TRADING_BET_SIZE_USDC)
        return TRADING_BET_SIZE_USDC

    raw_size = balance * TRADING_BET_PERCENTAGE
    clamped = max(TRADING_MIN_BET_USDC, min(TRADING_MAX_BET_USDC, raw_size))

    log.info(
        "[Sizing] Dynamic: $%.2f × %.1f%% = $%.2f",
        balance, TRADING_BET_PERCENTAGE * 100, clamped,
    )

    if not _graduation_notified:
        _graduation_notified = True
        await _notify_graduation(http_client, resolved, pnl, clamped)

    return clamped


# ---------------------------------------------------------------------------
# Risk check (state from Railway API)
# ---------------------------------------------------------------------------

async def _check_risk_limits(stats: dict) -> Optional[str]:
    global _pause_until

    now = time.time()
    if _pause_until > 0:
        if now < _pause_until:
            return f"cooling down after consecutive losses ({int(_pause_until - now)}s remaining)"
        log.info("[Risk] Consecutive-loss pause expired — resuming")
        _pause_until = 0.0

    consecutive = stats.get("consecutive_losses", 0)
    if consecutive >= TRADING_CONSECUTIVE_LOSS_PAUSE:
        _pause_until = now + TRADING_PAUSE_DURATION_SECONDS
        log.warning("[Risk] %d consecutive losses — pausing %ds", consecutive, TRADING_PAUSE_DURATION_SECONDS)
        return f"pausing after {consecutive} consecutive losses"

    daily_loss = stats.get("daily_loss", 0.0)
    if daily_loss >= TRADING_MAX_DAILY_LOSS_USDC:
        return f"daily loss limit reached (${daily_loss:.2f} >= ${TRADING_MAX_DAILY_LOSS_USDC:.2f})"

    open_positions = stats.get("open_positions", 0)
    if open_positions >= TRADING_MAX_CONCURRENT_POSITIONS:
        return f"max concurrent positions ({open_positions}/{TRADING_MAX_CONCURRENT_POSITIONS})"

    return None


# ---------------------------------------------------------------------------
# Vault sweep
# ---------------------------------------------------------------------------

def _send_usdc_sync(to_address: str, amount_usdc: float) -> str:
    from web3 import Web3
    from eth_account import Account

    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
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
        "chainId":  TRADING_CHAIN_ID,
    })
    account = Account.from_key(TRADING_PRIVATE_KEY)
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return tx_hash.hex()


def _redeem_positions_sync(condition_id: str) -> str:
    """
    Call CTF.redeemPositions(usdc, 0x0, conditionId, [1, 2]) on Polygon.
    Burns all outcome tokens held by the wallet for this market and returns
    the collateral (USDC) owed for the winning side. Safe to call with [1, 2]
    (both slots) regardless of which outcome won — losing tokens return 0.
    """
    from web3 import Web3
    from eth_account import Account

    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(_CTF_CONTRACT),
        abi=_CTF_REDEEM_ABI,
    )

    # Normalise condition_id to exactly 32 bytes (pad left with zeros if short)
    hex_str = condition_id.replace("0x", "").zfill(64)
    condition_bytes32 = bytes.fromhex(hex_str)

    from_addr = Web3.to_checksum_address(_wallet_address)
    tx = ctf.functions.redeemPositions(
        Web3.to_checksum_address(_USDC_CONTRACT),  # collateralToken
        b"\x00" * 32,                              # parentCollectionId = bytes32(0)
        condition_bytes32,                          # conditionId
        [1, 2],                                    # indexSets: YES slot + NO slot
    ).build_transaction({
        "from":     from_addr,
        "nonce":    w3.eth.get_transaction_count(from_addr),
        "gas":      200_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  TRADING_CHAIN_ID,
    })
    account = Account.from_key(TRADING_PRIVATE_KEY)
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return tx_hash.hex()


async def _check_and_sweep(http_client: httpx.AsyncClient) -> None:
    if not VAULT_WALLET_ADDRESS:
        return

    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning("[Vault] Balance fetch failed: %s — skipping sweep", exc)
        return

    if balance <= VAULT_SWEEP_THRESHOLD_USDC:
        return

    sweep_amount = balance - VAULT_SWEEP_FLOOR_USDC
    if sweep_amount <= 0:
        return

    # Gas check
    try:
        matic = await asyncio.to_thread(_get_matic_balance_sync)
        if matic < _SWEEP_MIN_MATIC:
            log.warning("[Vault] Insufficient MATIC (%.4f) — skipping sweep", matic)
            return
    except Exception as exc:
        log.warning("[Vault] MATIC check failed: %s — skipping sweep", exc)
        return

    log.info("[Vault] Sweeping $%.2f to vault (balance $%.2f)", sweep_amount, balance)
    try:
        tx_hash = await asyncio.to_thread(_send_usdc_sync, VAULT_WALLET_ADDRESS, sweep_amount)
        remaining = balance - sweep_amount
        log.info("[Vault] Sweep complete: tx=%s", tx_hash)
        await _notify_sweep(http_client, sweep_amount, remaining, tx_hash)
    except Exception as exc:
        log.error("[Vault] Sweep failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Telegram notifications
# ---------------------------------------------------------------------------

async def _send_telegram(http_client: httpx.AsyncClient, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = await http_client.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        if not resp.is_success:
            log.warning("[Telegram] Send failed %d: %s", resp.status_code, resp.text[:100])
    except Exception as exc:
        log.warning("[Telegram] Send error: %s", exc)


async def _notify_trade_filled(
    http_client: httpx.AsyncClient,
    market_q: str,
    bet_side: str,
    fill_price: float,
    size: float,
    score: int,
    slippage: Optional[float],
    stats: dict,
) -> None:
    addr = _wallet_address
    wallet_display = f"{addr[:10]}...{addr[-6:]}" if len(addr) > 16 else addr
    open_count = stats.get("open_positions", 0)

    try:
        remaining_balance = await _get_usdc_balance()
    except Exception:
        remaining_balance = (TRADING_MAX_CONCURRENT_POSITIONS - open_count) * TRADING_BET_SIZE_USDC

    slip_str = f"{slippage:.4f}" if slippage is not None else "N/A"
    text = (
        "💰💰💰 <b>LIVE TRADE</b> 💰💰💰\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 <b>Market:</b> {market_q[:120]}\n\n"
        f"🎯 <b>Position:</b> <b>{bet_side}</b> @ {fill_price:.3f}\n"
        f"💵 <b>Size:</b> ${size:.2f} USDC\n"
        f"📊 <b>Signal score:</b> {score}\n"
        f"📉 <b>Slippage:</b> {slip_str}\n\n"
        f"💼 <b>Bankroll available:</b> ${remaining_balance:.2f}\n"
        f"📂 <b>Open positions:</b> {open_count}/{TRADING_MAX_CONCURRENT_POSITIONS}\n"
        f"👛 <b>Wallet:</b> <code>{wallet_display}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <i>Automated trade. Not financial advice.</i>"
    )
    await _send_telegram(http_client, text)


async def _notify_trade_error(
    http_client: httpx.AsyncClient,
    market_q: str,
    bet_side: str,
    price: float,
    score: int,
    status: str,
    error_msg: Optional[str],
) -> None:
    err_line = f"\n❗ <code>{error_msg}</code>" if error_msg else ""
    text = (
        f"❌ <b>TRADE {status.upper()}</b>\n"
        f"📋 {market_q[:100]}\n"
        f"🎯 {bet_side} @ {price:.3f} | Score: {score}{err_line}"
    )
    await _send_telegram(http_client, text)


async def _notify_graduation(
    http_client: httpx.AsyncClient,
    resolved: int,
    pnl: float,
    bet_size: float,
) -> None:
    text = (
        "📈 <b>TRADING UPGRADE</b>\n\n"
        "The bot has graduated to dynamic position sizing.\n"
        f"✅ {resolved} trades resolved\n"
        f"✅ Cumulative P&amp;L: +${pnl:.2f}\n"
        f"📊 Now sizing at {TRADING_BET_PERCENTAGE * 100:.1f}% of bankroll per trade\n\n"
        "<i>Bet sizes will scale with performance.</i>"
    )
    await _send_telegram(http_client, text)


async def _notify_sweep(
    http_client: httpx.AsyncClient,
    sweep_amount: float,
    remaining: float,
    tx_hash: str,
) -> None:
    def _short(addr: str) -> str:
        return f"{addr[:10]}...{addr[-6:]}" if len(addr) > 16 else addr

    text = (
        "🏦 <b>PROFIT SWEEP</b>\n\n"
        f"💸 <b>${sweep_amount:.2f}</b> USDC transferred to vault\n"
        f"📤 From: <code>{_short(_wallet_address)}</code>\n"
        f"📥 To: <code>{_short(VAULT_WALLET_ADDRESS)}</code>\n\n"
        f"💰 Trading wallet balance: ${remaining:.2f}\n"
        f'🔗 <a href="https://polygonscan.com/tx/{tx_hash}">Verify transaction →</a>\n\n'
        f"<i>Profits secured. Trading continues with ${remaining:.2f}.</i>"
    )
    await _send_telegram(http_client, text)


async def _notify_redemption(
    http_client: httpx.AsyncClient,
    count: int,
    recovered: float,
    new_balance: float,
) -> None:
    plural = "positions" if count > 1 else "position"
    text = (
        f"💵 <b>POSITION REDEEMED</b>\n\n"
        f"✅ {count} winning {plural} settled\n"
        f"💰 <b>Recovered: ${recovered:.2f} USDC</b>\n"
        f"🏦 <b>Wallet balance: ${new_balance:.2f}</b>"
    )
    await _send_telegram(http_client, text)


async def _notify_low_balance(
    http_client: httpx.AsyncClient,
    balance: float,
    open_positions: int,
) -> None:
    text = (
        "⚠️ <b>LOW BALANCE</b>\n\n"
        f"Trading wallet has <b>${balance:.2f} USDC</b> remaining.\n"
        f"📂 {open_positions} position(s) still open awaiting resolution.\n"
        f"Bot will pause new trades until redemptions replenish balance above "
        f"${TRADING_MIN_BET_USDC:.2f}."
    )
    await _send_telegram(http_client, text)


async def _notify_trade_resolution(
    http_client: httpx.AsyncClient,
    trade: dict,
    resolution_status: str,
    pnl: float,
) -> None:
    market_q = trade.get("market_question") or trade.get("market_id", "")
    bet_side = trade.get("bet_side", "")
    fill_price = trade.get("bet_price_filled") or trade.get("bet_price_intended") or 0.0
    winning_outcome = trade.get("winning_outcome")

    if resolution_status == "won":
        result_emoji = "✅"
    elif resolution_status == "lost":
        result_emoji = "❌"
    else:
        result_emoji = "↩️"

    outcome_line = f"🏆 <b>Outcome:</b> {winning_outcome}\n" if winning_outcome else ""

    text = (
        "🏁 <b>TRADE RESOLVED</b>\n\n"
        f"📋 <b>Market:</b> {market_q[:120]}\n"
        f"🎯 <b>Position:</b> {bet_side} @ {fill_price:.3f}\n"
        f"{outcome_line}"
        f"{result_emoji} <b>Result:</b> {resolution_status.upper()}\n"
        f"💰 <b>P&amp;L:</b> ${pnl:+.2f} USDC"
    )
    await _send_telegram(http_client, text)


# ---------------------------------------------------------------------------
# Trade execution
# ---------------------------------------------------------------------------

async def _execute_trade(
    clob_client,
    http_client: httpx.AsyncClient,
    alert: dict,
    stats: dict,
) -> None:
    from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
    from py_clob_client_v2.order_utils.model.side import Side
    from py_clob_client_v2.exceptions import PolyException

    alert_id  = alert["alert_id"]
    market_id = alert["market_id"]
    market_q  = alert.get("market_question") or market_id
    bet_side  = alert["bet_side"]
    price_alert = float(alert["bet_price_at_alert"])
    score     = int(alert["score"])
    token_id  = alert.get("clob_token_id")

    if not token_id:
        log.error("[Trade] No clob_token_id for alert %s — skipping", alert_id[:12])
        await _api_post(http_client, "/api/trades", {
            "alert_id": alert_id, "market_id": market_id,
            "market_question": market_q, "clob_token_id": "UNKNOWN",
            "bet_side": bet_side, "bet_price_intended": price_alert,
            "size_usdc": TRADING_BET_SIZE_USDC,
            "status": "failed", "error_message": "no clob_token_id",
        })
        return

    bet_size = await _calculate_bet_size(http_client, stats)

    # Current ask price for slippage measurement
    current_price: Optional[float] = price_alert
    try:
        price_resp = await asyncio.to_thread(clob_client.get_price, token_id, "BUY")
        if isinstance(price_resp, dict):
            current_price = float(price_resp.get("price", price_alert))
        elif price_resp is not None:
            current_price = float(price_resp)
    except Exception:
        pass

    slippage = abs(current_price - price_alert) if current_price is not None else None

    if slippage is not None and slippage > 0.05:
        log.warning(
            "[Trade] High slippage for alert %s: intended=%.3f current=%.3f",
            alert_id[:12], price_alert, current_price,
        )

    fill_price: Optional[float] = None
    order_id:   Optional[str]   = None
    status = "error"
    error_msg: Optional[str] = None

    try:
        # Fetch neg_risk in a thread before building the order.
        # create_market_order() auto-calls get_neg_risk() internally, but it does
        # so synchronously in the async context which can silently fail and default
        # to False — causing order_version_mismatch on neg-risk markets.
        # Fetching it explicitly in asyncio.to_thread() and passing via options
        # guarantees the order is signed for the correct exchange contract.
        try:
            neg_risk = await asyncio.to_thread(clob_client.get_neg_risk, token_id)
        except Exception as _nr_exc:
            log.warning("[Trade] get_neg_risk failed for %s: %s — defaulting to False", token_id, _nr_exc)
            neg_risk = False

        log.debug("[Trade] token=%s neg_risk=%s sig_type=%d funder=%s",
                  token_id[:16], neg_risk, TRADING_SIGNATURE_TYPE,
                  TRADING_FUNDER_ADDRESS[:10] + "..." if TRADING_FUNDER_ADDRESS else "self")

        order = MarketOrderArgs(token_id=token_id, amount=bet_size, side=Side.BUY)
        options = PartialCreateOrderOptions(neg_risk=neg_risk)
        resp = await asyncio.to_thread(
            clob_client.create_and_post_market_order, order, options
        )

        if isinstance(resp, dict):
            success = resp.get("success", False)
            error_msg = resp.get("errorMsg") or None
            order_id = resp.get("orderID") or resp.get("id")
            resp_status = (resp.get("status") or "").lower()

            if success or resp_status == "matched":
                status = "filled"
                fill_price = float(
                    resp.get("price") or resp.get("avgPrice") or current_price or price_alert
                )
            else:
                status = "rejected"
                if not error_msg:
                    error_msg = f"CLOB status={resp.get('status', 'unknown')}"
        else:
            status = "rejected"
            error_msg = f"unexpected CLOB response type: {type(resp)}"

    except PolyException as exc:
        status = "error"
        error_msg = str(exc)
        log.error("[Trade] CLOB API error for alert %s: %s", alert_id[:12], exc)
    except Exception as exc:
        status = "error"
        error_msg = str(exc)
        log.error("[Trade] Unexpected error for alert %s: %s", alert_id[:12], exc, exc_info=True)

    # Report to Railway
    await _api_post(http_client, "/api/trades", {
        "alert_id":           alert_id,
        "market_id":          market_id,
        "market_question":    market_q,
        "clob_token_id":      token_id,
        "bet_side":           bet_side,
        "bet_price_intended": price_alert,
        "bet_price_filled":   fill_price,
        "slippage":           slippage,
        "size_usdc":          bet_size,
        "order_id":           order_id,
        "status":             status,
        "error_message":      error_msg,
    })

    log.info(
        "[Trade] %s | %s | side=%s | $%.2f | %s | fill=%s",
        alert_id[:12], market_q[:40], bet_side, bet_size, status,
        f"{fill_price:.4f}" if fill_price else "N/A",
    )

    if status == "filled":
        await _notify_trade_filled(
            http_client, market_q, bet_side,
            fill_price or current_price or price_alert,
            bet_size, score, slippage, stats,
        )
    else:
        await _notify_trade_error(
            http_client, market_q, bet_side, price_alert, score, status, error_msg,
        )


# ---------------------------------------------------------------------------
# Resolution polling
# ---------------------------------------------------------------------------

async def _check_pending_resolutions(http_client: httpx.AsyncClient) -> None:
    """
    Poll /api/trades/pending, detect alert_outcomes resolutions, send
    Telegram notifications, and PATCH resolution back to Railway so that
    /api/stats/trading reflects correct P&L for future risk checks.
    """
    data = await _api_get(http_client, "/api/trades/pending")
    if not isinstance(data, list):
        return

    for trade in data:
        alert_id = trade.get("alert_id")
        if not alert_id:
            continue

        alert_status = trade.get("alert_resolution_status", "pending")
        if alert_status == "pending" or alert_id in _notified_resolutions:
            continue

        fill_price = trade.get("bet_price_filled") or trade.get("bet_price_intended") or 0.5
        size_usdc  = trade.get("size_usdc") or TRADING_BET_SIZE_USDC

        if alert_status == "resolved_won":
            resolution_status = "won"
            pnl = size_usdc * (1.0 / fill_price - 1.0)
        elif alert_status == "resolved_lost":
            resolution_status = "lost"
            pnl = -size_usdc
        else:
            resolution_status = "invalid"
            pnl = 0.0

        # Write resolution back to Railway so stats stay accurate
        await _api_patch(http_client, f"/api/trades/{alert_id}/resolution", {
            "resolution_status": resolution_status,
            "pnl": pnl,
            "resolved_at": int(time.time()),
        })

        _notified_resolutions.add(alert_id)
        await _notify_trade_resolution(http_client, trade, resolution_status, pnl)

        log.info(
            "[Resolution] %s → %s | P&L: $%+.2f",
            alert_id[:12], resolution_status, pnl,
        )


# ---------------------------------------------------------------------------
# Redemption and balance monitoring
# ---------------------------------------------------------------------------

async def _check_and_redeem(http_client: httpx.AsyncClient) -> None:
    """
    Poll /api/trades/pending for filled+resolved_won positions not yet redeemed,
    call CTF.redeemPositions for each, and update the cached USDC balance.
    Also fires a low-balance warning when the wallet drops below LOW_BALANCE_WARN_USD.
    """
    global _low_balance_warned, _cached_usdc_balance

    data = await _api_get(http_client, "/api/trades/pending")
    if not isinstance(data, list):
        return

    # Positions that are filled and won, not yet redeemed this session
    redeemable = [
        t for t in data
        if t.get("status") == "filled"
        and t.get("alert_resolution_status") == "resolved_won"
        and t.get("alert_id") not in _redeemed_positions
        and t.get("market_id")
    ]

    if redeemable:
        # MATIC gas check before any on-chain calls
        try:
            matic = await asyncio.to_thread(_get_matic_balance_sync)
            if matic < _SWEEP_MIN_MATIC:
                log.warning("[Redeem] Insufficient MATIC (%.4f) — skipping redemption", matic)
                return
        except Exception as exc:
            log.warning("[Redeem] MATIC check failed: %s — skipping redemption", exc)
            return

        try:
            balance_before = await _get_usdc_balance()
        except Exception as exc:
            log.warning("[Redeem] Pre-redeem balance fetch failed: %s", exc)
            balance_before = 0.0

        for trade in redeemable:
            alert_id = trade["alert_id"]
            market_id = trade["market_id"]
            log.info("[Redeem] Calling redeemPositions for market %s", market_id[:16])
            try:
                tx_hash = await asyncio.to_thread(_redeem_positions_sync, market_id)
                _redeemed_positions.add(alert_id)
                log.info("[Redeem] %s redeemed → tx=%s", alert_id[:12], tx_hash)
            except Exception as exc:
                log.error("[Redeem] redeemPositions failed for %s (%s): %s",
                          alert_id[:12], market_id[:16], exc)

        try:
            balance_after = await _get_usdc_balance()
        except Exception as exc:
            log.warning("[Redeem] Post-redeem balance fetch failed: %s", exc)
            balance_after = balance_before

        recovered = max(0.0, balance_after - balance_before)
        _cached_usdc_balance = balance_after
        log.info("[Redeem] %d position(s) processed | recovered $%.2f | balance $%.2f",
                 len(redeemable), recovered, balance_after)

        if recovered > 0.01:
            await _notify_redemption(http_client, len(redeemable), recovered, balance_after)

    else:
        # No redemptions due — still refresh cached balance for the low-balance check
        try:
            _cached_usdc_balance = await _get_usdc_balance()
        except Exception as exc:
            log.warning("[Redeem] Balance refresh failed: %s", exc)
            return

    # Low-balance warning + auto-pause logic (threshold is LOW_BALANCE_WARN_USD)
    if _cached_usdc_balance >= 0:
        open_count = sum(
            1 for t in data
            if t.get("status") == "filled" and t.get("alert_resolution_status") == "pending"
        )
        if _cached_usdc_balance < LOW_BALANCE_WARN_USD and not _low_balance_warned:
            _low_balance_warned = True
            log.warning("[Balance] Low balance: $%.2f — sending warning", _cached_usdc_balance)
            await _notify_low_balance(http_client, _cached_usdc_balance, open_count)
        elif _cached_usdc_balance >= LOW_BALANCE_WARN_USD:
            _low_balance_warned = False  # reset so warning re-fires if balance drops again


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main() -> None:
    global _wallet_address, _last_resolution_check, _last_sweep_check, _last_redemption_check

    if not RAILWAY_API_URL:
        log.critical("RAILWAY_API_URL is not set — exiting")
        sys.exit(1)
    if not API_SECRET_KEY:
        log.critical("API_SECRET_KEY is not set — exiting")
        sys.exit(1)
    if not TRADING_PRIVATE_KEY:
        log.critical("TRADING_PRIVATE_KEY is not set — exiting")
        sys.exit(1)

    _wallet_address = _get_wallet_address()

    log.info("=" * 60)
    log.info("Polymarket Remote Trader starting")
    log.info("Railway API: %s", RAILWAY_API_URL)
    log.info("Wallet:      %s", _wallet_address)
    log.info("Poll:        every %ds | Min score: %d", POLL_INTERVAL, TRADING_MIN_SCORE)
    log.info("Vault:       %s", VAULT_WALLET_ADDRESS or "disabled")
    log.info("=" * 60)

    try:
        clob_client = await _init_clob_client()
        log.info("CLOB client initialised (host=%s)", TRADING_CLOB_HOST)
    except Exception as exc:
        log.critical("CLOB client init failed: %s", exc)
        sys.exit(1)

    # Look back 2h on startup — long enough to catch alerts from a brief restart,
    # short enough to avoid re-trading pre-filter stale alerts from before a deploy.
    last_processed_ts = int(time.time()) - 7200

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        while True:
            try:
                now = time.time()

                # Resolution poll (every 10 min)
                if now - _last_resolution_check >= _RESOLUTION_POLL_INTERVAL:
                    await _check_pending_resolutions(http_client)
                    _last_resolution_check = now

                # Vault sweep check
                if VAULT_WALLET_ADDRESS and now - _last_sweep_check >= VAULT_SWEEP_INTERVAL_SECONDS:
                    await _check_and_sweep(http_client)
                    _last_sweep_check = now

                # Redemption check (also refreshes _cached_usdc_balance for the pause below)
                if now - _last_redemption_check >= REDEMPTION_CHECK_INTERVAL:
                    await _check_and_redeem(http_client)
                    _last_redemption_check = now

                # Pause trading when balance is too low to cover the minimum bet.
                # Resumes automatically once redemptions replenish the wallet.
                if _cached_usdc_balance >= 0 and _cached_usdc_balance < TRADING_MIN_BET_USDC:
                    log.info(
                        "[Risk] Balance $%.2f < min $%.2f — pausing new trades, "
                        "waiting for redemptions",
                        _cached_usdc_balance, TRADING_MIN_BET_USDC,
                    )
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # Fetch stats for risk management
                stats = await _api_get(http_client, "/api/stats/trading") or {}

                block_reason = await _check_risk_limits(stats)
                if block_reason:
                    log.info("[Risk] Skipping cycle — %s", block_reason)
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # Fetch tradeable alerts
                alerts = await _api_get(
                    http_client,
                    "/api/alerts/tradeable",
                    params={
                        "min_score": TRADING_MIN_SCORE,
                        "since":     last_processed_ts,
                        "limit":     20,
                    },
                )
                if not isinstance(alerts, list):
                    alerts = []

                if alerts:
                    log.info("[Trader] %d alert(s) to evaluate", len(alerts))

                for alert in alerts:
                    ts = alert.get("created_at", 0)
                    last_processed_ts = max(last_processed_ts, ts)

                    # Re-check risk before each individual trade
                    stats = await _api_get(http_client, "/api/stats/trading") or {}
                    block_reason = await _check_risk_limits(stats)
                    if block_reason:
                        log.info("[Risk] Mid-loop block: %s — halting batch", block_reason)
                        break

                    await _execute_trade(clob_client, http_client, alert, stats)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("[Trader] Cycle error: %s", exc, exc_info=True)

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
