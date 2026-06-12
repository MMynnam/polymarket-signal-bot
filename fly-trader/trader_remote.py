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
  6. Daily at VAULT_SWEEP_HOUR_UTC: if balance ≥ threshold, pause deposit wallet,
     wait 1h timelock, withdrawERC20 to vault, unpause. Trading continues during pause.
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
# Safety rail (fun-bot): absolute per-trade hard cap. Raised 2→5 for the ~$145 wallet
# (2026-05-31 top-up). At $145 the 10% balance cap in _calculate_bet_size binds at
# $14.50, so this $5 hard cap sits well below it and is the binding per-trade limit;
# if the wallet shrinks the % cap auto-scales below $5 (e.g. $30 → $3) and takes over.
TRADING_MAX_BET_USDC: float = float(os.getenv("TRADING_MAX_BET_USDC", "5.0"))
# Daily realized-loss pause. Raised 10→15 (= ~10.3% of the $145 wallet, matching the
# prior rail's spirit when it was 14% of a $70 wallet) so normal variance at the higher
# capital doesn't trip it; a genuinely bad day still pauses new entries.
TRADING_MAX_DAILY_LOSS_USDC: float = float(os.getenv("TRADING_MAX_DAILY_LOSS_USDC", "15.0"))
TRADING_MAX_CONCURRENT_POSITIONS: int = int(os.getenv("TRADING_MAX_CONCURRENT_POSITIONS", "10"))
TRADING_CONSECUTIVE_LOSS_PAUSE: int = int(os.getenv("TRADING_CONSECUTIVE_LOSS_PAUSE", "6"))
TRADING_PAUSE_DURATION_SECONDS: int = int(os.getenv("TRADING_PAUSE_DURATION_SECONDS", "1800"))
TRADING_LOSS_STREAK_WARNING: int = int(os.getenv("TRADING_LOSS_STREAK_WARNING", "4"))
# Geoblock circuit-breaker: when Polymarket returns 403 "Trading restricted in your
# region" on POST /order (datacenter-IP block, not a country block), pause new trade
# attempts for this long and alert ONCE — instead of hammering every signal with a
# 403 and spamming a TRADE ERROR per attempt. The fix for the underlying block is to
# rotate the fly egress IP (region move); this just stops the bleeding gracefully.
GEOBLOCK_PAUSE_SECONDS: int = int(os.getenv("GEOBLOCK_PAUSE_SECONDS", "1800"))
# Magnitude-based circuit breaker: pause when rolling realized loss over the last
# TRADING_CB_WINDOW_HOURS exceeds TRADING_CB_DRAWDOWN_PCT of current bankroll.
# Replaces the consecutive-count auto-pause; TRADING_CONSECUTIVE_LOSS_PAUSE is now
# a warning-only heads-up (no pause). Rolling state is in-process only — resets on restart.
TRADING_CB_WINDOW_HOURS: float = float(os.getenv("TRADING_CB_WINDOW_HOURS", "6"))
TRADING_CB_DRAWDOWN_PCT: float = float(os.getenv("TRADING_CB_DRAWDOWN_PCT", "0.15"))
TRADING_MIN_SCORE: int = int(os.getenv("TRADING_MIN_SCORE", "65"))
TRADING_DYNAMIC_MIN_RESOLVED: int = int(os.getenv("TRADING_DYNAMIC_MIN_RESOLVED", "20"))
POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", "30"))
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
# Optional separate "ops" channel for operational noise (skips, errors, sweeps, tier/cap
# changes, low-balance, geoblock, redemptions). The audience channel (TELEGRAM_CHAT_ID) then
# only carries the SHOW: bets placed, resolutions, recaps. If TELEGRAM_OPS_CHAT_ID is unset,
# ops messages still go to the main channel but SILENTLY (no push notification), so the
# friends' phones only buzz for the fun. 2026-06-03.
TELEGRAM_OPS_CHAT_ID: str = os.getenv("TELEGRAM_OPS_CHAT_ID", "")
ALCHEMY_RPC_URL: str = os.getenv("ALCHEMY_RPC_URL", "")

# ---------------------------------------------------------------------------
# Core scaling parameters — change ONLY these three to re-calibrate the system.
# ---------------------------------------------------------------------------

# How much USDC to keep in the trading wallet at all times (= sweep floor).
TRADING_WORKING_CAPITAL_USDC: float = float(os.getenv("TRADING_WORKING_CAPITAL_USDC", "110.0"))
# How much above working capital triggers a sweep (threshold = working_capital + headroom).
TRADING_SWEEP_HEADROOM_USDC: float = float(os.getenv("TRADING_SWEEP_HEADROOM_USDC", "40.0"))
# Fraction of bankroll to keep deployed across open positions at once.
TRADING_TARGET_EXPOSURE_PCT: float = float(os.getenv("TRADING_TARGET_EXPOSURE_PCT", "0.50"))

# Position count safety clamps — floor/ceiling regardless of exposure calc.
TRADING_MAX_POSITIONS_FLOOR: int = int(os.getenv("TRADING_MAX_POSITIONS_FLOOR", "10"))
TRADING_MAX_POSITIONS_CEILING: int = int(os.getenv("TRADING_MAX_POSITIONS_CEILING", "50"))

# Tiered position cap.
# Normal mode: all qualifying alerts admitted up to TRADING_NORMAL_POSITIONS_MAX (or the
# bankroll-derived cap if lower — whichever is smaller is the effective normal ceiling).
# Premium mode: between normal cap and TRADING_PREMIUM_POSITIONS_MAX, only alerts with
# score >= TRADING_PREMIUM_SCORE_THRESHOLD are admitted.
# Hard ceiling: TRADING_PREMIUM_POSITIONS_MAX is an absolute block — no trades at all.
TRADING_NORMAL_POSITIONS_MAX: int = int(os.getenv("TRADING_NORMAL_POSITIONS_MAX", "70"))
TRADING_PREMIUM_POSITIONS_MAX: int = int(os.getenv("TRADING_PREMIUM_POSITIONS_MAX", "85"))
TRADING_PREMIUM_SCORE_THRESHOLD: int = int(os.getenv("TRADING_PREMIUM_SCORE_THRESHOLD", "85"))
# Hysteresis band: once in premium, don't return to normal until positions drop
# to (effective_normal - TRADING_TIER_DEADBAND). Prevents ±1 flapping at the cap boundary.
TRADING_TIER_DEADBAND: int = int(os.getenv("TRADING_TIER_DEADBAND", "5"))

VAULT_WALLET_ADDRESS: str = os.getenv("VAULT_WALLET_ADDRESS", "")
# Derived from core scaling params; direct env override still works for backward compat.
VAULT_SWEEP_THRESHOLD_USDC: float = float(os.getenv("VAULT_SWEEP_THRESHOLD_USDC", str(TRADING_WORKING_CAPITAL_USDC + TRADING_SWEEP_HEADROOM_USDC)))
VAULT_SWEEP_FLOOR_USDC: float = float(os.getenv("VAULT_SWEEP_FLOOR_USDC", str(TRADING_WORKING_CAPITAL_USDC)))
# Hour (0–23 UTC) at which the daily sweep check fires in production mode.
VAULT_SWEEP_HOUR_UTC: int = int(os.getenv("VAULT_SWEEP_HOUR_UTC", "4"))
# Set to a positive value (e.g. 1.0) to sweep exactly that amount for first-run testing.
# Bypasses the time-of-day check — fires on the next poll cycle. Set to 0 for normal operation.
VAULT_SWEEP_TEST_AMOUNT_USDC: float = float(os.getenv("VAULT_SWEEP_TEST_AMOUNT_USDC", "0"))

REDEMPTION_CHECK_INTERVAL: int = int(os.getenv("REDEMPTION_CHECK_INTERVAL", "600"))
LOW_BALANCE_WARN_USD: float = float(os.getenv("LOW_BALANCE_WARN_USD", "10.0"))
POSITIONS_SUMMARY_INTERVAL_SECONDS: int = int(os.getenv("POSITIONS_SUMMARY_INTERVAL_SECONDS", "21600"))

TRADING_CLOB_HOST: str = "https://clob.polymarket.com"
TRADING_CHAIN_ID: int = 137

# Polymarket wallet type configuration.
# signature_type=0  EOA — use for a raw private key wallet created outside Polymarket
# signature_type=2  POLY_GNOSIS_SAFE — use for wallets created via Polymarket's web UI.
#   Requires TRADING_FUNDER_ADDRESS = the proxy/safe wallet address that holds USDC.
#   The private key signs on behalf of that address; TRADING_PRIVATE_KEY is the signer.
TRADING_SIGNATURE_TYPE: int = int(os.getenv("TRADING_SIGNATURE_TYPE", "0"))
TRADING_FUNDER_ADDRESS: str = os.getenv("TRADING_FUNDER_ADDRESS", "")

# Dynamic slippage gate — default OFF (observation-only until May 31 decision).
# With TRADING_DYNAMIC_SLIPPAGE_ENABLED=false, trading is byte-identical to today.
TRADING_SLIPPAGE_THRESHOLD: float = float(os.getenv("TRADING_SLIPPAGE_THRESHOLD", "0.05"))
# Source-2 fix: discard signals older than this many seconds at trade time.
# Provisional first cut (600s) — expected to tighten after forward data accumulates.
MAX_SIGNAL_AGE_S: int = int(os.getenv("MAX_SIGNAL_AGE_S", "600"))

TRADING_DYNAMIC_SLIPPAGE_ENABLED: bool = os.getenv("TRADING_DYNAMIC_SLIPPAGE_ENABLED", "false").lower() == "true"
TRADING_SLIPPAGE_MAX_EXPANSION: float = float(os.getenv("TRADING_SLIPPAGE_MAX_EXPANSION", "0.03"))
TRADING_MAX_ENTRY_PRICE: float = float(os.getenv("TRADING_MAX_ENTRY_PRICE", "0.85"))

# Soccer-favorites filter: skip sports-category alerts where price > this threshold.
# Entertainment-mode (2026-05-31): DISABLED (default OFF). The -39.9% evidence was
# inversion-era; clean data shows sports favorites only ~-3.5%, and sports are the
# most entertainment-dense markets (daily matches). Letting them trade for the
# spectator feed; bleed bounded by ≤$2/trade + $10 daily-loss cap + circuit breaker.
FILTER_SOCCER_FAVORITES_ENABLED: bool = os.getenv("FILTER_SOCCER_FAVORITES_ENABLED", "false").lower() == "true"
FILTER_SOCCER_FAVORITES_MAX_PRICE: float = float(os.getenv("FILTER_SOCCER_FAVORITES_MAX_PRICE", "0.50"))

# Position sizing as a % of bankroll.
#
# 2026-06-03 — score-weighted sizing RETIRED by default. A 6,848-alert edge study (joined
# to on-chain outcomes) showed the confidence score is NON-predictive of edge, and mildly
# ANTI-predictive within the favorites band the bot actually trades (price >= 0.50). Scaling
# stake UP with score therefore amplified misranking. Default is now a FLAT, score-independent
# % of bankroll, set risk-neutral to ~ the prior average stake at typical scores. The clamps
# below ([MIN,MAX] bet + 10% balance cap) are unchanged. Restore the legacy linear ramp with
# TRADING_SCORE_WEIGHTED_SIZING=true.
TRADING_SCORE_WEIGHTED_SIZING: bool = os.getenv("TRADING_SCORE_WEIGHTED_SIZING", "false").lower() == "true"
TRADING_FLAT_PCT_PER_TRADE: float = float(os.getenv("TRADING_FLAT_PCT_PER_TRADE", "0.020"))
# Legacy score-weighted ramp params (only used when TRADING_SCORE_WEIGHTED_SIZING=true):
# pct ramps linearly from BASE_PCT at score=TRADING_MIN_SCORE to MAX_PCT at SCORE_CEILING.
TRADING_SCORE_BASE_PCT: float = float(os.getenv("TRADING_SCORE_BASE_PCT", "0.010"))
TRADING_MAX_PCT_PER_TRADE: float = float(os.getenv("TRADING_MAX_PCT_PER_TRADE", "0.040"))
TRADING_SCORE_CEILING: int = int(os.getenv("TRADING_SCORE_CEILING", "90"))

# Hard minimum bankroll for vault sweep — effective floor = max(this, VAULT_SWEEP_FLOOR_USDC).
VAULT_BANKROLL_FLOOR_USDC: float = float(os.getenv("VAULT_BANKROLL_FLOOR_USDC", "80.0"))

# Master arm switch for the vault sweep. Default false — sweep logic runs in dry-run mode
# (computes and logs intended amounts) but moves zero funds. Only an explicit
# VAULT_SWEEP_ENABLED=true set by the operator can ever arm it.
VAULT_SWEEP_ENABLED: bool = os.getenv("VAULT_SWEEP_ENABLED", "false").lower() == "true"

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
_geoblock_pause_until: float = 0.0  # set when a 403 region-restricted order is seen
_cb_triggered_at_loss: float = 0.0   # USD loss that last triggered magnitude CB; re-trigger guard
_warning_sent: bool = False           # True once the mid-streak warning fires; resets after a win
_heavy_warning_sent: bool = False     # True once the level-CONSECUTIVE_LOSS_PAUSE warning fires
_cb_pnl_history: list = []            # [(unix_ts, pnl_float), ...] accumulated from resolutions
_wallet_address: str = ""
_graduation_notified: bool = False
_notified_resolutions: set[str] = set()
_last_resolution_check: float = 0.0
_last_redemption_check: float = 0.0
_last_positions_summary: float = 0.0
_redeemed_positions: set[str] = set()
_low_balance_warned: bool = False
_cached_usdc_balance: float = -1.0  # -1 = not yet fetched
_RESOLUTION_POLL_INTERVAL: int = 600
_skip_notified: set[tuple[str, str]] = set()  # (alert_id, reason_key); one notification per alert per lifetime
_skip_batch: dict = {}  # (market_id, bet_side, reason_key) → batch entry; flushed by background task
_SKIP_BATCH_WINDOW_SECONDS: int = 60  # window for collapsing repeated same-market skips
_alert_skip_cache: dict[str, float] = {}  # alert_id -> expiry timestamp; avoids re-evaluating
# Strong references to background telemetry tasks — prevents GC before completion.
_background_tasks: set[asyncio.Task] = set()
_SKIP_DECISION_TTL_SECONDS: int = 300     # 5 min: re-evaluate after price may have stabilised
_session_avg_bet: float = TRADING_BET_SIZE_USDC  # EMA of actual fill sizes; used for cap estimation
_sweep_state: str = "idle"      # idle | pause_pending | pause_ready
_sweep_paused_at: float = 0.0
_sweep_intended_amount: float = 0.0  # calculated at pause time; rechecked at withdraw time
_sweep_last_date: str = ""          # "YYYY-MM-DD" UTC; prevents double-firing on restart
_current_max_positions: int = TRADING_MAX_CONCURRENT_POSITIONS  # updated each cycle by _compute_max_positions
_legacy_max_positions_ceiling: Optional[int] = None  # set at startup if old env var is detected
_current_tier: str = "normal"  # "normal" | "premium" | "hardcap"; drives transition notifications
_held_positions: set[tuple[str, str]] = set()  # (market_id, bet_side); per-market dedup
_held_positions_seeded: bool = False  # False until startup seed succeeds

# ---------------------------------------------------------------------------
# Collateral token constants (Polygon — Polymarket USD / pUSD)
# ---------------------------------------------------------------------------

_USDC_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
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

# DepositWallet (ERC-1967 proxy implementation: 0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB).
# execute() is onlyFactory — cannot be called by EOA. The only EOA-accessible path for moving
# funds out is: pause() → wait timelockDelay → withdrawERC20() → unpause().
# pause() does NOT block execute() (trading continues during the sweep window).
_DEPOSIT_WALLET_ABI = [
    {
        "inputs": [],
        "name": "paused",
        "outputs": [{"internalType": "uint256", "name": "paused_", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {"inputs": [], "name": "pause",   "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [], "name": "unpause", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    {
        "inputs": [
            {"name": "_token",  "type": "address"},
            {"name": "_to",     "type": "address"},
            {"name": "_amount", "type": "uint256"},
        ],
        "name": "withdrawERC20",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]
# Verified: DepositWalletFactory.timelockDelay() == 3600 on Polygon mainnet.
_DEPOSIT_WALLET_TIMELOCK_SECONDS: int = 3600

# CollateralOfframp: unwraps pUSD → USDC.e at 1:1 (no fee on unwrap direction).
# IMPORTANT: _asset is the OUTPUT token address (USDC.e), not pUSD.
# The caller must pre-approve the offramp to pull pUSD via transferFrom.
_OFFRAMP_ADDRESS = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"
_USDCE_CONTRACT  = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_OFFRAMP_ABI = [
    {
        "inputs": [
            {"name": "_asset",  "type": "address"},
            {"name": "_to",     "type": "address"},
            {"name": "_amount", "type": "uint256"},
        ],
        "name": "unwrap",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]
_PUSD_APPROVE_ABI = [
    {
        "inputs": [
            {"name": "owner",   "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount",  "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
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
    # USDC lives in the proxy/safe wallet when TRADING_FUNDER_ADDRESS is set.
    # Fall back to the EOA only when running in plain EOA mode (sig_type=0, no funder).
    target = TRADING_FUNDER_ADDRESS if TRADING_FUNDER_ADDRESS else _wallet_address
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(_USDC_CONTRACT),
        abi=_USDC_BALANCE_ABI,
    )
    raw = contract.functions.balanceOf(
        Web3.to_checksum_address(target)
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

async def _calculate_bet_size(http_client: httpx.AsyncClient, stats: dict, score: int = 0) -> float:
    """
    Three-state auto-graduating bet size using stats from the Railway API.
    WARMUP → FIXED → DYNAMIC (balance × score-weighted percentage).

    Dynamic pct scales linearly from TRADING_SCORE_BASE_PCT at score=TRADING_MIN_SCORE
    to TRADING_MAX_PCT_PER_TRADE at score=TRADING_SCORE_CEILING, then flat above.
    Result is clamped to [TRADING_MIN_BET_USDC, TRADING_MAX_BET_USDC].

    Graduation requires BOTH:
      1. resolved >= TRADING_DYNAMIC_MIN_RESOLVED (DB count)
      2. prospective_pnl > 0 (from _cb_pnl_history, seeded prospective-only on restart)
    This prevents historical backfill artifacts from triggering dynamic sizing.
    """
    global _graduation_notified

    resolved = stats.get("resolved", 0)

    if resolved < TRADING_DYNAMIC_MIN_RESOLVED:
        log.info(
            "[Sizing] Warmup: %d/%d resolved. Fixed $%.2f.",
            resolved, TRADING_DYNAMIC_MIN_RESOLVED, TRADING_BET_SIZE_USDC,
        )
        return TRADING_BET_SIZE_USDC

    # Gate on prospective P&L from _cb_pnl_history (never populated by backfill).
    # Use a 7-day lookback to accumulate enough signal across sessions.
    prospective_pnl = _get_rolling_pnl(window_hours=168.0)
    if prospective_pnl <= 0:
        log.info(
            "[Sizing] %d resolved, prospective P&L $%.2f ≤ 0. Fixed $%.2f.",
            resolved, prospective_pnl, TRADING_BET_SIZE_USDC,
        )
        return TRADING_BET_SIZE_USDC

    # DYNAMIC — bet size as a % of balance (flat by default; legacy score-ramp opt-in).
    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning("[Sizing] USDC balance fetch failed: %s — fixed $%.2f", exc, TRADING_BET_SIZE_USDC)
        return TRADING_BET_SIZE_USDC

    if TRADING_SCORE_WEIGHTED_SIZING:
        # Legacy: linear ramp from BASE_PCT at MIN_SCORE to MAX_PCT at CEILING (flat above).
        score_range = max(1, TRADING_SCORE_CEILING - TRADING_MIN_SCORE)
        score_excess = max(0, min(score - TRADING_MIN_SCORE, score_range))
        pct = TRADING_SCORE_BASE_PCT + score_excess * (TRADING_MAX_PCT_PER_TRADE - TRADING_SCORE_BASE_PCT) / score_range
        mode = f"score-weighted (score={score})"
    else:
        # Default: flat, score-independent % — the score is non-predictive of edge.
        pct = TRADING_FLAT_PCT_PER_TRADE
        mode = "flat"

    raw_size = balance * pct
    clamped = max(TRADING_MIN_BET_USDC, min(TRADING_MAX_BET_USDC, raw_size))
    # Safety rail (fun-bot, thin wallet): never stake >10% of free balance on one trade.
    clamped = min(clamped, max(TRADING_MIN_BET_USDC, 0.10 * balance))

    log.info(
        "[Sizing] Dynamic %s: $%.2f × %.2f%% = $%.2f (clamp [$%.2f, $%.2f])",
        mode, balance, pct * 100, clamped, TRADING_MIN_BET_USDC, TRADING_MAX_BET_USDC,
    )

    if not _graduation_notified:
        _graduation_notified = True
        await _notify_graduation(http_client, resolved, prospective_pnl, clamped)

    return clamped


# ---------------------------------------------------------------------------
# Skip notification
# ---------------------------------------------------------------------------

def _skip_reason_key(reason: str) -> str:
    r = reason.lower()
    if "max concurrent" in r:
        return "max_concurrent"
    if "consecutive loss" in r or "cooling down" in r:
        return "consecutive_loss"
    if "daily loss" in r:
        return "daily_loss"
    if "slippage" in r:
        return "slippage"
    if "balance" in r or "bankroll" in r:
        return "balance"
    if "already" in r:
        return "already_traded"
    if "too old" in r:
        return "stale_signal"
    return reason[:40]


def _skip_reason_plain(reason: str) -> str:
    import re
    r = reason.lower()
    if "max concurrent" in r:
        m = re.search(r'\((\d+/\d+)\)', reason)
        count = m.group(1) if m else str(_current_max_positions)
        return f"at max concurrent positions ({count})"
    if "consecutive loss" in r or "cooling down" in r:
        return "paused after consecutive losses"
    if "daily loss" in r:
        return "daily loss limit reached"
    if "slippage" in r:
        return "price moved too much since alert"
    if "balance" in r or "bankroll" in r:
        return "bankroll below minimum bet"
    if "already" in r:
        return "already holding a position on this market"
    if "too old" in r:
        return reason
    return reason[:80]


def _score_bar(score: int) -> str:
    fraction = max(0.0, min(1.0, (score - 65) / 35))
    filled = round(fraction * 10)
    return "▓" * filled + "░" * (10 - filled)


def _fmt_score_breakdown(score_breakdown_json: Optional[str]) -> str:
    import html as _html, json as _json
    try:
        bd = _json.loads(score_breakdown_json or "{}")
    except Exception:
        return ""
    fields = [
        ("Timing",       "timing",           "timing_note"),
        ("Wallet age",   "wallet_age",        "wallet_age_note"),
        ("Size anomaly", "size_anomaly",      "size_anomaly_note"),
        ("Concentration","concentration",     "concentration_note"),
        ("Funding vel.", "funding_velocity",  "funding_velocity_note"),
        ("Win rate",     "win_rate",          "win_rate_note"),
        ("Cluster",      "cluster_bonus",     "cluster_note"),
        ("Convergence",  "convergence_bonus", "convergence_note"),
    ]
    lines = []
    for label, key, note_key in fields:
        val = bd.get(key) or 0
        note = _html.escape((bd.get(note_key) or "")[:32])
        sign = "+" if val >= 0 else ""
        note_str = f"  {note}" if note else ""
        lines.append(f"{label:<14} {sign}{val:2d}{note_str}")
    total = bd.get("total") or 0
    lines.append("─" * 30)
    lines.append(f"{'Total':<14}  {total:2d}")
    return "\n".join(lines)


def _evaluate_dynamic_gate(
    price_intended: float,
    price_current: float,
    slippage: float,
) -> str:
    """
    Pure function: classify whether the dynamic gate would allow trading.
    Returns one of:
      would_have_traded          — within expanded tolerance AND below price ceiling
      rejected_expansion_bound   — slippage > threshold + max_expansion
      rejected_price_ceiling     — within expansion but entry price > max entry ceiling
    Always called; gate_outcome is used for telemetry regardless of flag state.
    """
    expanded_limit = TRADING_SLIPPAGE_THRESHOLD + TRADING_SLIPPAGE_MAX_EXPANSION
    if slippage > expanded_limit:
        return "rejected_expansion_bound"
    if price_current > TRADING_MAX_ENTRY_PRICE:
        return "rejected_price_ceiling"
    return "would_have_traded"


async def _post_skip_telemetry(
    http_client: httpx.AsyncClient,
    alert: dict,
    slippage: float,
    price_current: float,
    gate_outcome: str,
    shadow_size_usdc: Optional[float],
) -> None:
    """
    Fire-and-forget: POST skip telemetry to Railway. Exception-isolated.
    Never called from the hot path in a way that blocks the trade decision.
    """
    try:
        price_intended: float = alert.get("bet_price_at_alert") or price_current
        price_delta_abs: float = slippage
        price_delta_frac: float = (
            slippage / price_intended if price_intended > 0 else 0.0
        )
        shadow_entry = price_current if gate_outcome == "would_have_traded" else None
        payload = {
            "alert_id":        alert.get("alert_id", ""),
            "market_id":       alert.get("market_id", ""),
            "market_question": (alert.get("market_question") or "")[:120],
            "bet_side":        alert.get("bet_side", ""),
            "score":           alert.get("score", 0),
            "market_type":     alert.get("market_type", ""),
            "price_intended":  price_intended,
            "price_current":   price_current,
            "price_delta_abs": price_delta_abs,
            "price_delta_frac": price_delta_frac,
            "static_threshold": TRADING_SLIPPAGE_THRESHOLD,
            "gate_outcome":    gate_outcome,
            "shadow_entry_price": shadow_entry,
            "shadow_size_usdc":  shadow_size_usdc if gate_outcome == "would_have_traded" else None,
        }
        result = await _api_post(http_client, "/api/skips/telemetry", payload)
        if result is None:
            log.warning("[Shadow] skip telemetry POST failed for alert %s (pipeline may be broken)",
                        payload.get("alert_id", "")[:12])
    except Exception as exc:
        log.warning("[Shadow] _post_skip_telemetry exception for alert %s: %s",
                    alert.get("alert_id", "")[:12], exc)


async def _post_stale_telemetry(
    http_client: httpx.AsyncClient,
    alert: dict,
) -> None:
    """Fire-and-forget telemetry for age-discarded signals. INSERT OR IGNORE on alert_id."""
    try:
        price_at_alert: float = float(alert.get("bet_price_at_alert") or 0.0)
        payload = {
            "alert_id":         alert.get("alert_id", ""),
            "market_id":        alert.get("market_id", ""),
            "market_question":  (alert.get("market_question") or "")[:120],
            "bet_side":         alert.get("bet_side", ""),
            "score":            alert.get("score", 0),
            "market_type":      alert.get("market_type", ""),
            "price_intended":   price_at_alert,
            "price_current":    price_at_alert,
            "price_delta_abs":  0.0,
            "price_delta_frac": 0.0,
            "static_threshold": TRADING_SLIPPAGE_THRESHOLD,
            "gate_outcome":     "rejected_stale_signal",
            "shadow_entry_price": None,
            "shadow_size_usdc":   None,
        }
        result = await _api_post(http_client, "/api/skips/telemetry", payload)
        if result is None:
            log.warning("[Stale] telemetry POST failed for alert %s", payload["alert_id"][:12])
    except Exception as exc:
        log.warning("[Stale] telemetry exception for alert %s: %s",
                    alert.get("alert_id", "")[:12], exc)


async def _flush_skip_batch(http_client: httpx.AsyncClient, batch_key: tuple) -> None:
    """Sleep SKIP_BATCH_WINDOW_SECONDS then send a collapsed summary if multiple skips accumulated."""
    import html as _html
    await asyncio.sleep(_SKIP_BATCH_WINDOW_SECONDS)
    batch = _skip_batch.pop(batch_key, None)
    if batch is None or batch["count"] <= 1:
        return
    count = batch["count"]
    market_q = batch.get("market_q", "")
    bet_side = batch.get("bet_side", "")
    p_int = batch.get("last_price_intended")
    p_cur = batch.get("last_price_current")
    price_str = ""
    if p_int is not None and p_cur is not None:
        price_str = f"\nLatest:  {p_int*100:.0f}¢ → {p_cur*100:.0f}¢"
    text = (
        f"— SKIP ×{count} in {_SKIP_BATCH_WINDOW_SECONDS}s —\n"
        f"{_html.escape(market_q[:70])}  [{_html.escape(bet_side)}]\n"
        f"Same signal, price kept moving.{price_str}"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_skip(
    http_client: httpx.AsyncClient,
    alert: dict,
    reason: str,
    *,
    price_intended: Optional[float] = None,
    price_current: Optional[float] = None,
    slippage_delta: Optional[float] = None,
) -> None:
    import html as _html
    global _skip_notified, _skip_batch

    alert_id   = alert.get("alert_id", "")
    score      = int(alert.get("score") or 0)
    market_q   = (alert.get("market_question") or alert.get("market_id", ""))
    market_id  = alert.get("market_id", "")
    bet_side   = alert.get("bet_side", "")
    created_at = int(alert.get("created_at") or 0)

    # Alert-level dedup: never fire twice for the same alert+reason.
    cache_key = (alert_id, _skip_reason_key(reason))
    if cache_key in _skip_notified:
        return
    _skip_notified.add(cache_key)

    now = time.time()

    # Market-level batch dedup: collapse rapid-fire same-market same-reason skips.
    batch_key = (market_id, bet_side, _skip_reason_key(reason))
    batch = _skip_batch.get(batch_key)
    if batch is not None and now - batch["first_ts"] < _SKIP_BATCH_WINDOW_SECONDS:
        batch["count"] += 1
        batch["last_price_intended"] = price_intended
        batch["last_price_current"] = price_current
        return  # suppressed; background task will flush the summary

    # Start a new batch window and fire the message immediately.
    _skip_batch[batch_key] = {
        "count": 1,
        "first_ts": now,
        "market_q": market_q,
        "bet_side": bet_side,
        "last_price_intended": price_intended,
        "last_price_current": price_current,
    }
    _t = asyncio.create_task(_flush_skip_batch(http_client, batch_key))
    _background_tasks.add(_t)
    _t.add_done_callback(_background_tasks.discard)

    # Build the skip message.
    bar          = _score_bar(score)
    detected_str = time.strftime("%H:%M:%S UTC", time.gmtime(created_at)) if created_at else "unknown"
    attempted_str = time.strftime("%H:%M:%S UTC", time.gmtime(int(now)))
    latency_s    = int(now - created_at) if created_at else 0

    if price_intended is not None and price_current is not None and slippage_delta is not None:
        price_line = (
            f"\nPrice:  {price_intended*100:.0f}¢ → {price_current*100:.0f}¢"
            f"  (moved {slippage_delta*100:.0f}¢, band {TRADING_SLIPPAGE_THRESHOLD*100:.0f}¢)"
        )
    else:
        price_line = f"\nReason: {_html.escape(_skip_reason_plain(reason))}"

    text = (
        f"<b>— SKIP  ·  score {score}  {bar}</b>\n"
        f"{_html.escape(market_q[:80])}\n"
        f"\nDetected  <code>{detected_str}</code>\n"
        f"Attempted <code>{attempted_str}</code>  (Δ {latency_s}s)"
        f"{price_line}"
    )
    await _send_telegram(http_client, text, ops=True)


# ---------------------------------------------------------------------------
# Positions summary
# ---------------------------------------------------------------------------

async def _send_positions_summary(http_client: httpx.AsyncClient) -> None:
    import html as _html

    positions = await _api_get(http_client, "/api/positions/open") or []
    if not positions:
        return

    now = time.time()
    bankroll = _cached_usdc_balance if _cached_usdc_balance >= 0 else 0.0
    total_size = sum(p.get("size_usdc") or 0.0 for p in positions)
    total_potential = sum(
        (p.get("size_usdc") or 0.0) / max(p.get("bet_price_filled") or p.get("bet_price_intended") or 0.5, 0.001)
        - (p.get("size_usdc") or 0.0)
        for p in positions
    )

    # Telegram caps a message at 4096 chars; the full list overflows once there are
    # ~25+ open positions (400 "message is too long"). List only the largest N by
    # stake and summarise the rest; the totals below still cover ALL positions.
    _MAX_LISTED = 20
    shown = sorted(positions, key=lambda p: p.get("size_usdc") or 0.0, reverse=True)[:_MAX_LISTED]
    lines = []
    for p in shown:
        market_q = (p.get("market_question") or p.get("market_id", ""))[:50]
        side = p.get("bet_side", "")
        fill = p.get("bet_price_filled") or p.get("bet_price_intended") or 0.0
        size = p.get("size_usdc") or 0.0
        score = p.get("score") or 0
        hours_ago = (now - (p.get("created_at") or now)) / 3600
        lines.append(
            f"- {_html.escape(market_q)}\n"
            f"  {_html.escape(side)} @ ${fill:.3f}  •  ${size:.2f}\n"
            f"  Score: {score}  •  Opened: {hours_ago:.1f}h ago"
        )
    _more = len(positions) - len(shown)
    if _more > 0:
        lines.append(f"  …and <b>{_more}</b> more (top {_MAX_LISTED} by stake shown)")

    vault_footer = ""
    try:
        vault_stats = await _api_get(http_client, "/api/stats/vault") or {}
        sweep_count = vault_stats.get("sweep_count") or 0
        total_swept = vault_stats.get("total_swept") or 0.0
        if sweep_count > 0:
            vault_footer = (
                f"\n\n🏦 <b>Vault history:</b> {sweep_count} sweep(s) · "
                f"${total_swept:.2f} total"
            )
    except Exception:
        pass

    text = (
        f"📊 <b>Open Positions ({len(positions)}/{_current_max_positions})</b>\n\n"
        + "\n\n".join(lines) + "\n\n"
        f"💼 <b>Total at risk:</b> ${total_size:.2f}\n"
        f"💰 <b>Potential profit if all win:</b> ${total_potential:.2f}\n"
        f"💵 <b>Bankroll available:</b> ${bankroll:.2f}"
        f"{vault_footer}\n\n"
        f"🎯 <b>System config:</b>\n"
        f"   Working capital: ${TRADING_WORKING_CAPITAL_USDC:.2f}\n"
        f"   Sweep at: ${VAULT_SWEEP_THRESHOLD_USDC:.2f} → floor ${VAULT_SWEEP_FLOOR_USDC:.2f}\n"
        f"   Target exposure: {TRADING_TARGET_EXPOSURE_PCT * 100:.0f}%  •  Current cap: {_current_max_positions}"
    )
    await _send_telegram(http_client, text, ops=True)


# ---------------------------------------------------------------------------
# Risk check (state from Railway API)
# ---------------------------------------------------------------------------

def _compute_max_positions(bankroll: float, bet_size: float, open_positions: int = 0) -> int:
    """Cap = existing open positions + how many new ones the free balance can fund.
    Using open_positions in the formula means the cap grows as positions resolve and
    the free balance rises — correctly reflecting real fundable capacity."""
    if bet_size <= 0:
        return TRADING_MAX_POSITIONS_FLOOR
    ceiling = (
        min(_legacy_max_positions_ceiling, TRADING_NORMAL_POSITIONS_MAX)
        if _legacy_max_positions_ceiling is not None
        else TRADING_NORMAL_POSITIONS_MAX
    )
    fundable_new = int((bankroll * TRADING_TARGET_EXPOSURE_PCT) / bet_size)
    raw = open_positions + fundable_new
    return max(TRADING_MAX_POSITIONS_FLOOR, min(raw, ceiling))


def _get_tier(open_positions: int, current_tier: str) -> str:
    """Return 'normal', 'premium', or 'hardcap' with deadband hysteresis.

    Enter premium when open_positions >= effective_normal.
    Exit premium only when open_positions <= effective_normal - TRADING_TIER_DEADBAND.
    The deadband zone [(effective_normal - deadband), effective_normal) is sticky:
    the tier stays whatever it was, preventing ±1 flapping at the cap boundary.

    Hardcap has no deadband — one resolved position immediately drops to premium.
    Dynamic floor interaction: if effective_normal drops due to bankroll erosion,
    positions >= new effective_normal are still correctly caught by the first check.
    """
    if open_positions >= TRADING_PREMIUM_POSITIONS_MAX:
        return "hardcap"
    effective_normal = min(_current_max_positions, TRADING_NORMAL_POSITIONS_MAX)
    if open_positions >= effective_normal:
        return "premium"
    if open_positions <= effective_normal - TRADING_TIER_DEADBAND:
        return "normal"
    # In the deadband: maintain the incoming tier (sticky)
    return "premium" if current_tier in ("premium", "hardcap") else "normal"


def _check_tier_for_alert(open_positions: int, score: int) -> Optional[str]:
    """Return a skip reason if this alert is blocked by the tier system, or None to proceed."""
    tier = _get_tier(open_positions, _current_tier)
    if tier == "premium" and score < TRADING_PREMIUM_SCORE_THRESHOLD:
        effective_normal = min(_current_max_positions, TRADING_NORMAL_POSITIONS_MAX)
        return (
            f"premium tier: score {score} < {TRADING_PREMIUM_SCORE_THRESHOLD} "
            f"({open_positions}/{effective_normal} positions filled)"
        )
    return None


def _get_rolling_pnl(window_hours: float) -> float:
    """Sum realized P&L from _cb_pnl_history entries within the last window_hours."""
    cutoff = time.time() - window_hours * 3600
    return sum(pnl for ts, pnl in _cb_pnl_history if ts >= cutoff)


async def _backfill_cb_pnl_history(http_client: httpx.AsyncClient) -> int:
    """Seed _cb_pnl_history from Railway on startup so the CB isn't blind after a restart."""
    global _cb_pnl_history
    since_ts = int(time.time() - TRADING_CB_WINDOW_HOURS * 3600)
    try:
        data = await _api_get(http_client, "/api/trades/resolved-recent", params={"since": since_ts})
    except Exception as exc:
        log.warning("[CB] Backfill fetch failed: %s", exc)
        return 0
    if not isinstance(data, list):
        log.warning("[CB] Backfill returned unexpected type: %s", type(data))
        return 0
    added = 0
    for entry in data:
        if entry.get("alert_id") and entry.get("pnl") is not None and entry.get("resolved_at"):
            _cb_pnl_history.append((float(entry["resolved_at"]), float(entry["pnl"])))
            added += 1
    if added:
        rolling_loss = max(0.0, -_get_rolling_pnl(TRADING_CB_WINDOW_HOURS))
        log.info("[CB] Backfilled %d entries; rolling loss=$%.2f over last %.0fh",
                 added, rolling_loss, TRADING_CB_WINDOW_HOURS)
    else:
        log.info("[CB] Backfill: no resolved trades in last %.0fh", TRADING_CB_WINDOW_HOURS)
    return added


async def _check_risk_limits(
    stats: dict,
    http_client: Optional[httpx.AsyncClient] = None,
) -> Optional[str]:
    global _pause_until, _cb_triggered_at_loss, _warning_sent, _heavy_warning_sent

    now = time.time()
    if _geoblock_pause_until > now:
        return f"geoblocked — order endpoint region-restricted ({int((_geoblock_pause_until - now) / 60)}m cooldown)"
    if _pause_until > 0:
        if now < _pause_until:
            return f"CB cooling down ({int(_pause_until - now)}s remaining)"
        log.info("[Risk] CB pause expired — resuming")
        _pause_until = 0.0
        # _cb_triggered_at_loss intentionally NOT reset here: prevents re-triggering
        # at the same loss level once the pause expires (re-trigger guard).

    consecutive = stats.get("consecutive_losses", 0)

    # Reset per-episode warning flags once a win has broken the streak.
    if consecutive < TRADING_LOSS_STREAK_WARNING:
        _warning_sent = False
        _heavy_warning_sent = False

    # Level-1 warning at TRADING_LOSS_STREAK_WARNING (default 4) — heads-up, no pause.
    if (TRADING_LOSS_STREAK_WARNING > 0
            and consecutive >= TRADING_LOSS_STREAK_WARNING
            and not _warning_sent
            and http_client is not None):
        _warning_sent = True
        recent = await _api_get(http_client, "/api/trades/recent-losses") or []
        await _notify_loss_streak_warning(http_client, consecutive, recent)

    # Level-2 warning at TRADING_CONSECUTIVE_LOSS_PAUSE (default 6) — heads-up only, no pause.
    # The actual pause is now magnitude-based (below).
    if (TRADING_CONSECUTIVE_LOSS_PAUSE > 0
            and consecutive >= TRADING_CONSECUTIVE_LOSS_PAUSE
            and not _heavy_warning_sent
            and http_client is not None):
        _heavy_warning_sent = True
        recent = await _api_get(http_client, "/api/trades/recent-losses") or []
        await _notify_loss_streak_warning(http_client, consecutive, recent)

    # Magnitude-based circuit breaker: pause when rolling realized loss over
    # TRADING_CB_WINDOW_HOURS exceeds TRADING_CB_DRAWDOWN_PCT of bankroll.
    # Re-trigger guard: only fires when rolling_loss worsens past the level that
    # last triggered (analogous to _pause_triggered_at_streak for streak-based CB).
    # Guard resets when loss recovers below half the threshold.
    bankroll = max(_cached_usdc_balance, 1.0)
    threshold_usdc = TRADING_CB_DRAWDOWN_PCT * bankroll
    rolling_pnl = _get_rolling_pnl(TRADING_CB_WINDOW_HOURS)
    rolling_loss = max(0.0, -rolling_pnl)

    if rolling_loss < threshold_usdc * 0.5:
        _cb_triggered_at_loss = 0.0  # reset re-trigger guard on meaningful recovery

    if rolling_loss >= threshold_usdc and rolling_loss > _cb_triggered_at_loss:
        _cb_triggered_at_loss = rolling_loss
        _warning_sent = True
        _heavy_warning_sent = True
        _pause_until = now + TRADING_PAUSE_DURATION_SECONDS
        resume_str = time.strftime("%H:%M UTC", time.gmtime(int(_pause_until)))
        log.warning(
            "[Risk] CB: rolling loss $%.2f >= $%.2f (%.0f%% of $%.2f bankroll) over %.0fh — "
            "pausing %ds until %s",
            rolling_loss, threshold_usdc, TRADING_CB_DRAWDOWN_PCT * 100,
            bankroll, TRADING_CB_WINDOW_HOURS, TRADING_PAUSE_DURATION_SECONDS, resume_str,
        )
        if http_client is not None:
            recent = await _api_get(http_client, "/api/trades/recent-losses") or []
            await _notify_cb_drawdown_pause(
                http_client, rolling_loss, threshold_usdc, bankroll, int(_pause_until), recent
            )
        return f"CB: drawdown ${rolling_loss:.2f} >= ${threshold_usdc:.2f} over {TRADING_CB_WINDOW_HOURS:.0f}h"

    daily_loss = stats.get("daily_loss", 0.0)
    if daily_loss >= TRADING_MAX_DAILY_LOSS_USDC:
        return f"daily loss limit reached (${daily_loss:.2f} >= ${TRADING_MAX_DAILY_LOSS_USDC:.2f})"

    # Hard ceiling — absolute block regardless of tier or score.
    # The 50–60 premium tier is handled per-alert in the main loop.
    open_positions = stats.get("open_positions", 0)
    if open_positions >= TRADING_PREMIUM_POSITIONS_MAX:
        return f"hard cap reached ({open_positions}/{TRADING_PREMIUM_POSITIONS_MAX})"

    return None


# ---------------------------------------------------------------------------
# Vault sweep
# ---------------------------------------------------------------------------

def _redeem_positions_sync(condition_id: str) -> str:
    """
    Call CTF.redeemPositions(usdc, 0x0, conditionId, [1, 2]) on Polygon.
    Burns all outcome tokens held by the wallet for this market and returns
    the collateral (USDC) owed for the winning side. Safe to call with [1, 2]
    (both slots) regardless of which outcome won — losing tokens return 0.

    NOTE (proxy wallet): outcome tokens are held by the proxy wallet
    (TRADING_FUNDER_ADDRESS), not the EOA. This call is sent from the EOA
    (_wallet_address) as msg.sender. Whether CTF honours it depends on whether
    the EOA is the registered owner of the proxy. Behaviour is untested —
    Polymarket may also auto-redeem via their own backend. We'll observe the
    first resolved winning trade before deciding if this needs reworking.
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


def _get_wallet_paused_timestamp_sync() -> int:
    """Return the paused timestamp from the deposit wallet (0 if not paused)."""
    from web3 import Web3
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    wallet = w3.eth.contract(
        address=Web3.to_checksum_address(TRADING_FUNDER_ADDRESS),
        abi=_DEPOSIT_WALLET_ABI,
    )
    return int(wallet.functions.paused().call())


def _initiate_sweep_pause_sync() -> str:
    """Call pause() on the deposit wallet from the EOA. Returns tx hash."""
    from web3 import Web3
    from eth_account import Account as _Account
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    wallet = w3.eth.contract(
        address=Web3.to_checksum_address(TRADING_FUNDER_ADDRESS),
        abi=_DEPOSIT_WALLET_ABI,
    )
    eoa_cs = Web3.to_checksum_address(_wallet_address)
    tx = wallet.functions.pause().build_transaction({
        "from":     eoa_cs,
        "nonce":    w3.eth.get_transaction_count(eoa_cs),
        "gas":      80_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  TRADING_CHAIN_ID,
    })
    signed = _Account.from_key(TRADING_PRIVATE_KEY).sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.status != 1:
        raise RuntimeError(f"pause() reverted: {tx_hash.hex()}")
    return tx_hash.hex()


def _execute_sweep_withdraw_sync(sweep_amount: float) -> str:
    """
    Pre-flight simulate then call withdrawERC20(pUSD, EOA, amount) on the deposit wallet.
    Sends pUSD to the EOA (not the vault) so the offramp can pull it via transferFrom.
    Requires wallet to be paused and timelockDelay elapsed. Returns tx hash.
    """
    from web3 import Web3
    from eth_account import Account as _Account
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    wallet = w3.eth.contract(
        address=Web3.to_checksum_address(TRADING_FUNDER_ADDRESS),
        abi=_DEPOSIT_WALLET_ABI,
    )
    eoa_cs  = Web3.to_checksum_address(_wallet_address)
    pusd_cs = Web3.to_checksum_address(_USDC_CONTRACT)
    amount_raw = int(sweep_amount * (10 ** _USDC_DECIMALS))

    # Pre-flight simulation — revert here means no gas burned on a broken call
    wallet.functions.withdrawERC20(pusd_cs, eoa_cs, amount_raw).call({"from": eoa_cs})

    tx = wallet.functions.withdrawERC20(pusd_cs, eoa_cs, amount_raw).build_transaction({
        "from":     eoa_cs,
        "nonce":    w3.eth.get_transaction_count(eoa_cs, "pending"),
        "gas":      120_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  TRADING_CHAIN_ID,
    })
    signed = _Account.from_key(TRADING_PRIVATE_KEY).sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    if receipt.status != 1:
        raise RuntimeError(f"withdrawERC20() reverted: {tx_hash.hex()}")
    return tx_hash.hex()


def _execute_sweep_unwrap_sync(sweep_amount: float) -> str:
    """
    Approve CollateralOfframp to spend pUSD (first sweep only, uses infinity approval),
    then call unwrap(USDC.e, vault, amount) to convert pUSD in EOA to USDC.e in vault.
    Returns the unwrap tx hash.
    """
    from web3 import Web3
    from eth_account import Account as _Account
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))

    eoa_cs      = Web3.to_checksum_address(_wallet_address)
    pusd_cs     = Web3.to_checksum_address(_USDC_CONTRACT)
    usdce_cs    = Web3.to_checksum_address(_USDCE_CONTRACT)
    offramp_cs  = Web3.to_checksum_address(_OFFRAMP_ADDRESS)
    vault_cs    = Web3.to_checksum_address(VAULT_WALLET_ADDRESS)
    amount_raw  = int(sweep_amount * (10 ** _USDC_DECIMALS))
    account     = _Account.from_key(TRADING_PRIVATE_KEY)

    pusd    = w3.eth.contract(address=pusd_cs,    abi=_PUSD_APPROVE_ABI)
    offramp = w3.eth.contract(address=offramp_cs, abi=_OFFRAMP_ABI)

    # Approve offramp to pull pUSD (infinity; one-time cost on first sweep)
    allowance = pusd.functions.allowance(eoa_cs, offramp_cs).call()
    if allowance < amount_raw:
        log.info("[Vault] Approving offramp to spend pUSD (current allowance %d < needed %d)...", allowance, amount_raw)
        approve_tx = pusd.functions.approve(offramp_cs, 2**256 - 1).build_transaction({
            "from":     eoa_cs,
            "nonce":    w3.eth.get_transaction_count(eoa_cs, "pending"),
            "gas":      60_000,
            "gasPrice": w3.eth.gas_price,
            "chainId":  TRADING_CHAIN_ID,
        })
        signed_approve = account.sign_transaction(approve_tx)
        approve_hash = w3.eth.send_raw_transaction(signed_approve.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(approve_hash, timeout=120)
        if receipt.status != 1:
            raise RuntimeError(f"pUSD approve() reverted: {approve_hash.hex()}")
        log.info("[Vault] pUSD approve success: tx=%s", approve_hash.hex())

    # Unwrap pUSD (in EOA) → USDC.e (sent directly to vault)
    tx = offramp.functions.unwrap(usdce_cs, vault_cs, amount_raw).build_transaction({
        "from":     eoa_cs,
        "nonce":    w3.eth.get_transaction_count(eoa_cs, "pending"),
        "gas":      150_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  TRADING_CHAIN_ID,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    if receipt.status != 1:
        raise RuntimeError(f"unwrap() reverted: {tx_hash.hex()}")
    return tx_hash.hex()


def _unpause_deposit_wallet_sync() -> str:
    """Call unpause() on the deposit wallet from the EOA. Returns tx hash."""
    from web3 import Web3
    from eth_account import Account as _Account
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    wallet = w3.eth.contract(
        address=Web3.to_checksum_address(TRADING_FUNDER_ADDRESS),
        abi=_DEPOSIT_WALLET_ABI,
    )
    eoa_cs = Web3.to_checksum_address(_wallet_address)
    tx = wallet.functions.unpause().build_transaction({
        "from":     eoa_cs,
        "nonce":    w3.eth.get_transaction_count(eoa_cs, "pending"),
        "gas":      80_000,
        "gasPrice": w3.eth.gas_price,
        "chainId":  TRADING_CHAIN_ID,
    })
    signed = _Account.from_key(TRADING_PRIVATE_KEY).sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt.status != 1:
        raise RuntimeError(f"unpause() reverted: {tx_hash.hex()}")
    return tx_hash.hex()


def _get_vault_usdce_balance_sync() -> float:
    """Return the current USDC.e balance of the vault wallet."""
    from web3 import Web3
    rpc = ALCHEMY_RPC_URL or "https://polygon-rpc.com"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(_USDCE_CONTRACT),
        abi=_USDC_BALANCE_ABI,
    )
    raw = contract.functions.balanceOf(
        Web3.to_checksum_address(VAULT_WALLET_ADDRESS)
    ).call()
    return raw / (10 ** _USDC_DECIMALS)


def _get_eoa_pusd_balance_sync() -> float:
    """Return the EOA's pUSD balance — detects orphaned funds from a failed previous unwrap."""
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


async def _check_and_sweep(http_client: httpx.AsyncClient) -> None:
    """
    Daily vault sweep: pause() → 1h timelock → withdrawERC20() → unpause().

    Phase 1 (idle):  Daily trigger at VAULT_SWEEP_HOUR_UTC (or immediately in test mode).
                     Records intended_amount at pause time for later recalculation.
    Phase 2 (pause_pending): Polls each cycle; advances once timelock elapses.
    Phase 3 (pause_ready):  Re-reads balance, recalculates amount (min of intended vs
                     available), pre-flight sim, withdrawERC20, unpause, notify.
                     If balance dropped below floor: cancel sweep, unpause, notify.

    Trading is never blocked — DepositWallet.execute() ignores paused state.
    On phase-3 failure: attempt unpause, log once, reset to idle. No auto-retry same day.
    If unpause itself fails: send stuck-wallet alert for manual intervention.
    """
    global _sweep_state, _sweep_paused_at, _sweep_intended_amount, _sweep_last_date

    if not VAULT_WALLET_ADDRESS or not TRADING_FUNDER_ADDRESS:
        return

    # ------------------------------------------------------------------
    # Master arm switch — abort any in-progress sweep if flag was flipped off.
    # ------------------------------------------------------------------
    if not VAULT_SWEEP_ENABLED and _sweep_state in ("pause_pending", "pause_ready"):
        log.warning(
            "[Vault] VAULT_SWEEP_ENABLED=false but state=%s — aborting sweep and unpausing",
            _sweep_state,
        )
        try:
            await asyncio.to_thread(_unpause_deposit_wallet_sync)
            log.info("[Vault] Deposit wallet unpaused after sweep abort")
        except Exception as _ue:
            log.error("[Vault] Unpause after abort failed: %s", _ue)
            await _notify_sweep_stuck(http_client)
        _sweep_state = "idle"
        return

    # ------------------------------------------------------------------
    # Phase 2 — wait for timelock
    # ------------------------------------------------------------------
    if _sweep_state == "pause_pending":
        elapsed = time.time() - _sweep_paused_at
        remaining_s = _DEPOSIT_WALLET_TIMELOCK_SECONDS - elapsed
        if remaining_s > 0:
            log.info("[Vault] Pause timelock: %.0fs remaining", remaining_s)
            return
        log.info("[Vault] Timelock elapsed — advancing to pause_ready")
        _sweep_state = "pause_ready"
        return  # execute withdraw on next cycle

    # ------------------------------------------------------------------
    # Phase 3 — recalculate amount, withdraw, unpause
    # ------------------------------------------------------------------
    if _sweep_state == "pause_ready":
        try:
            balance = await _get_usdc_balance()

            if VAULT_SWEEP_TEST_AMOUNT_USDC > 0:
                actual_amount   = VAULT_SWEEP_TEST_AMOUNT_USDC
                intended_amount = VAULT_SWEEP_TEST_AMOUNT_USDC
            else:
                _p3_floor = max(VAULT_SWEEP_FLOOR_USDC, VAULT_BANKROLL_FLOOR_USDC)
                available = balance - _p3_floor
                if available <= 0:
                    log.warning(
                        "[Vault] Balance $%.2f dropped below floor $%.2f during timelock — cancelling",
                        balance, _p3_floor,
                    )
                    try:
                        await asyncio.to_thread(_unpause_deposit_wallet_sync)
                        log.info("[Vault] Deposit wallet unpaused after cancellation")
                    except Exception as ue:
                        log.error("[Vault] Unpause after cancellation failed: %s", ue)
                        await _notify_sweep_stuck(http_client)
                    _sweep_state = "idle"
                    await _notify_sweep_cancelled(http_client, balance)
                    return
                intended_amount = _sweep_intended_amount
                actual_amount   = min(intended_amount, available)

            # Check for orphaned pUSD in EOA left by a previous failed unwrap
            try:
                orphaned_pusd = await asyncio.to_thread(_get_eoa_pusd_balance_sync)
                if orphaned_pusd > 0:
                    log.info("[Vault] Recovering orphaned pUSD in EOA: $%.4f", orphaned_pusd)
            except Exception as exc:
                log.warning("[Vault] Could not check EOA pUSD balance: %s — assuming 0", exc)
                orphaned_pusd = 0.0

            log.info(
                "[Vault] Step 1 — withdrawERC20: $%.2f pUSD → EOA (intended $%.2f)",
                actual_amount, intended_amount,
            )
            withdraw_tx = await asyncio.to_thread(_execute_sweep_withdraw_sync, actual_amount)
            log.info("[Vault] withdrawERC20 to EOA success: tx=%s", withdraw_tx)

            # Step 2: vault USDC.e balance before unwrap (for received-amount verification)
            try:
                vault_before = await asyncio.to_thread(_get_vault_usdce_balance_sync)
            except Exception:
                vault_before = 0.0

            # Step 3: approve (first sweep only) + unwrap all pUSD in EOA → USDC.e to vault
            # unwrap_amount = this sweep + any orphaned pUSD from a prior failed sweep
            unwrap_amount = actual_amount + orphaned_pusd
            try:
                log.info(
                    "[Vault] Step 2 — unwrap $%.4f pUSD → USDC.e to vault %s",
                    unwrap_amount, VAULT_WALLET_ADDRESS[:12],
                )
                unwrap_tx = await asyncio.to_thread(_execute_sweep_unwrap_sync, unwrap_amount)
                log.info("[Vault] unwrap success: tx=%s", unwrap_tx)
            except Exception as exc:
                log.error(
                    "[Vault] pUSD withdrawn to EOA but unwrap failed — manual intervention required. "
                    "Call unwrap(%s, %s, %d) on offramp from EOA. Error: %s",
                    _USDCE_CONTRACT, VAULT_WALLET_ADDRESS,
                    int(unwrap_amount * 10 ** _USDC_DECIMALS), exc,
                )
                try:
                    await asyncio.to_thread(_unpause_deposit_wallet_sync)
                except Exception as ue:
                    log.error("[Vault] Unpause after unwrap failure: %s", ue)
                    await _notify_sweep_stuck(http_client)
                _sweep_state = "idle"
                return

            # Step 4: verify USDC.e received at vault
            try:
                vault_after = await asyncio.to_thread(_get_vault_usdce_balance_sync)
                usdce_received = vault_after - vault_before
                if unwrap_amount > 0 and usdce_received < unwrap_amount * 0.995:
                    log.warning(
                        "[Vault] USDC.e received $%.4f < expected $%.4f (%.2f%% shortfall) — check offramp",
                        usdce_received, unwrap_amount,
                        (1 - usdce_received / unwrap_amount) * 100,
                    )
                else:
                    log.info("[Vault] USDC.e vault: $%.4f → $%.4f (+$%.4f)", vault_before, vault_after, usdce_received)
                vault_total = vault_after
            except Exception:
                usdce_received = unwrap_amount  # assume 1:1 if balance check fails
                vault_total    = 0.0

            remaining_balance = await _get_usdc_balance()

            try:
                await asyncio.to_thread(_unpause_deposit_wallet_sync)
                log.info("[Vault] Deposit wallet unpaused")
            except Exception as ue:
                log.error("[Vault] Unpause failed — wallet stays paused: %s", ue)
                await _notify_sweep_stuck(http_client)

            _sweep_state = "idle"
            await _notify_sweep_completed(
                http_client, actual_amount, intended_amount, usdce_received,
                remaining_balance, vault_total, unwrap_tx,
            )

        except Exception as exc:
            log.error(
                "[Vault] Phase 3 failed: %s — resetting to idle. Manual investigation required.",
                exc, exc_info=True,
            )
            try:
                await asyncio.to_thread(_unpause_deposit_wallet_sync)
            except Exception as ue:
                log.error("[Vault] Unpause on error failed: %s", ue)
                await _notify_sweep_stuck(http_client)
            _sweep_state = "idle"
        return

    # ------------------------------------------------------------------
    # Phase 1 (idle) — daily time-of-day trigger
    # ------------------------------------------------------------------
    from datetime import datetime, timezone
    now_utc  = datetime.now(timezone.utc)
    today    = now_utc.strftime("%Y-%m-%d")
    is_test  = VAULT_SWEEP_TEST_AMOUNT_USDC > 0

    # Per-day guard prevents double-firing on restart
    if _sweep_last_date == today:
        return

    # Production: fire only at the configured hour. Test mode: fire immediately.
    if not is_test and now_utc.hour != VAULT_SWEEP_HOUR_UTC:
        return

    try:
        balance = await _get_usdc_balance()
    except Exception as exc:
        log.warning("[Vault] Balance fetch failed: %s — skipping sweep check", exc)
        return

    threshold = VAULT_SWEEP_TEST_AMOUNT_USDC if is_test else VAULT_SWEEP_THRESHOLD_USDC
    if balance < threshold:
        # Mark as checked so we don't log on every poll during the sweep hour
        _sweep_last_date = today
        log.info("[Vault] Daily check: balance $%.2f < threshold $%.2f — skip", balance, threshold)
        return

    try:
        matic = await asyncio.to_thread(_get_matic_balance_sync)
        if matic < _SWEEP_MIN_MATIC:
            log.warning("[Vault] Insufficient MATIC (%.4f) — skipping sweep", matic)
            return  # don't mark today — retry next poll in case MATIC recovers
    except Exception as exc:
        log.warning("[Vault] MATIC check failed: %s — skipping sweep", exc)
        return

    _effective_floor = max(VAULT_SWEEP_FLOOR_USDC, VAULT_BANKROLL_FLOOR_USDC)
    intended_amount = VAULT_SWEEP_TEST_AMOUNT_USDC if is_test else (balance - _effective_floor)
    if intended_amount <= 0:
        _sweep_last_date = today
        log.info("[Vault] Intended sweep amount $%.2f <= 0 — skip", intended_amount)
        return

    if not VAULT_SWEEP_ENABLED:
        _sweep_last_date = today
        log.info(
            "[Vault] DRY RUN (VAULT_SWEEP_ENABLED=false): "
            "balance $%.2f | would move $%.2f | would retain $%.2f | threshold $%.2f | floor $%.2f",
            balance, intended_amount, balance - intended_amount,
            VAULT_SWEEP_THRESHOLD_USDC, max(VAULT_SWEEP_FLOOR_USDC, VAULT_BANKROLL_FLOOR_USDC),
        )
        return

    log.info("[Vault] Initiating daily sweep (balance $%.2f, intended $%.2f)...", balance, intended_amount)
    try:
        await asyncio.to_thread(_initiate_sweep_pause_sync)
        _sweep_paused_at       = time.time()
        _sweep_intended_amount = intended_amount
        _sweep_state           = "pause_pending"
        _sweep_last_date       = today
        log.info("[Vault] pause() submitted. Withdraw unlocks in %ds.", _DEPOSIT_WALLET_TIMELOCK_SECONDS)

        from datetime import timedelta
        unpause_utc = datetime.now(timezone.utc) + timedelta(seconds=_DEPOSIT_WALLET_TIMELOCK_SECONDS)
        await _notify_sweep_initiated(http_client, intended_amount, unpause_utc.strftime("%H:%M UTC"))
    except Exception as exc:
        log.error("[Vault] pause() failed: %s — sweep aborted", exc, exc_info=True)
        _sweep_state = "idle"


# ---------------------------------------------------------------------------
# Telegram notifications
# ---------------------------------------------------------------------------

async def _send_telegram(http_client: httpx.AsyncClient, text: str, *, ops: bool = False) -> None:
    """Send a Telegram message.

    ops=False (default) → the AUDIENCE feed (TELEGRAM_CHAT_ID), with a push notification.
    ops=True            → operational noise. Goes to TELEGRAM_OPS_CHAT_ID if configured
                          (loud, for the operator); otherwise stays in the main channel but
                          SILENT (disable_notification) so the friends' phones don't buzz.
    """
    if not TELEGRAM_BOT_TOKEN:
        return
    if ops and TELEGRAM_OPS_CHAT_ID:
        chat_id, silent = TELEGRAM_OPS_CHAT_ID, False
    else:
        chat_id, silent = TELEGRAM_CHAT_ID, ops  # ops w/o ops-channel → silent in main feed
    if not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = await http_client.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": silent,
            },
            timeout=15,
        )
        if not resp.is_success:
            log.warning("[Telegram] Send failed %d: %s", resp.status_code, resp.text[:100])
    except Exception as exc:
        log.warning("[Telegram] Send error: %s", exc)


def _odds_read(price: float) -> tuple[str, str]:
    """Plain-English read of the entry price — the only honest signal (the insider score
    was shown to be non-predictive, so we lead with the odds the market itself gives).
    Returns (odds_word, house_banter). Pure presentation; no trading state."""
    if price is None or price <= 0:
        return "mystery line", "no read on this one — buckle up."
    if price <= 0.15:
        return "big longshot", "the bot is feeling BRAVE. 🫡"
    if price <= 0.35:
        return "underdog", "taking the dog. spicy. 🌶"
    if price <= 0.60:
        return "coin-flip", "genuine toss-up — nobody knows anything."
    if price <= 0.80:
        return "favorite", "leaning chalk. quietly confident."
    return "heavy chalk", "boring but usually banks. 🥱"


async def _notify_trade_filled(
    http_client: httpx.AsyncClient,
    market_q: str,
    bet_side: str,
    fill_price: float,
    size: float,
    score: int,
    slippage: Optional[float],
    market_url: Optional[str],
    *,
    alert_created_at: int = 0,
    score_breakdown_json: Optional[str] = None,
) -> None:
    # AUDIENCE message — story-first. We deliberately DROP the score, score-bar, signal
    # breakdown, detection latency, slippage, and the wallet/Polygonscan line: the edge study
    # showed the score and its components are non-predictive noise, and the rest is ops
    # forensics nobody in the feed reads. The price (as plain odds) is the only honest signal.
    # (score / slippage / alert_created_at / score_breakdown_json kept in the signature for
    # callers + the ops channel, intentionally unused here.)
    import html as _html

    try:
        remaining_balance = await _get_usdc_balance()
    except Exception:
        remaining_balance = 0.0

    profit_if_win = (size / fill_price) - size if fill_price and fill_price > 0 else 0.0
    safe_q        = _html.escape(str(market_q)[:90])
    safe_side     = _html.escape(str(bet_side))
    word, banter  = _odds_read(fill_price)
    price_c       = f"{fill_price*100:.0f}¢" if fill_price and fill_price > 0 else "?"

    text = (
        f"🟢 <b>BET IN</b> — {word}\n"
        f"{safe_q}\n\n"
        f"<b>{safe_side}</b> at {price_c}  ·  ${size:.2f} to win <b>${profit_if_win:.2f}</b>\n"
        f"🏦 ${remaining_balance:.2f} in the bank\n"
        f"<i>{banter}</i>"
    )
    if market_url:
        safe_url = _html.escape(str(market_url), quote=True)
        text += f'\n\n<a href="{safe_url}">watch it live →</a>'
    await _send_telegram(http_client, text)


def _is_geoblock(error_msg: Optional[str]) -> bool:
    """True if a CLOB error is the region/geoblock 403 (datacenter-IP block)."""
    if not error_msg:
        return False
    m = error_msg.lower()
    return "restricted in your region" in m or "geoblock" in m


async def _notify_geoblock_pause(http_client: httpx.AsyncClient, resume_ts: int) -> None:
    resume_str = time.strftime("%H:%M UTC", time.gmtime(resume_ts))
    text = (
        "🚫 <b>GEOBLOCKED — trading paused</b>\n\n"
        "Polymarket rejected order placement with <code>403 Trading restricted in "
        "your region</code> — the trading host's IP is being geoblocked.\n\n"
        f"⏸ Pausing new trades until <b>{resume_str}</b>, then retrying automatically.\n"
        "🛠 If this persists, rotate the trader's egress IP (move fly region).\n"
        "<i>Open positions are unaffected; reads/resolutions continue.</i>"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_trade_error(
    http_client: httpx.AsyncClient,
    market_q: str,
    bet_side: str,
    price: float,
    score: int,
    status: str,
    error_msg: Optional[str],
) -> None:
    import html as _html
    # HTML-escape all user/market/error-derived strings: CLOB/geoblock errors and market
    # titles routinely contain '&', '<', '>' (e.g. "price < min", "S&P 500"), which would
    # otherwise break Telegram HTML parsing and 400 the whole message.
    err_line = f"\n❗ <code>{_html.escape(str(error_msg))}</code>" if error_msg else ""
    text = (
        f"❌ <b>TRADE {status.upper()}</b>\n"
        f"📋 {_html.escape(market_q[:100])}\n"
        f"🎯 {_html.escape(str(bet_side))} @ {price:.3f} | Score: {score}{err_line}"
    )
    await _send_telegram(http_client, text, ops=True)


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
        f"📊 Now sizing at {TRADING_SCORE_BASE_PCT * 100:.1f}–{TRADING_MAX_PCT_PER_TRADE * 100:.1f}% of bankroll, scaled by signal score\n\n"
        "<i>Bet sizes scale with signal confidence.</i>"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_sweep_initiated(
    http_client: httpx.AsyncClient,
    intended_amount: float,
    unpause_utc_str: str,
) -> None:
    text = (
        "🏦 <b>VAULT SWEEP INITIATED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💸 <b>Intended sweep:</b> ${intended_amount:.2f} pUSD\n"
        f"🛡️ <b>Floor:</b> ${VAULT_SWEEP_FLOOR_USDC:.2f} pUSD kept in wallet\n"
        f"⏳ <b>Withdraw unlocks at:</b> ~{unpause_utc_str}\n\n"
        "Trading continues normally during the 1h timelock.\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_sweep_completed(
    http_client: httpx.AsyncClient,
    actual_amount: float,
    intended_amount: float,
    usdce_received: float,
    remaining: float,
    vault_total: float,
    unwrap_tx: str,
) -> None:
    adjusted = actual_amount < intended_amount - 0.01
    adjusted_line = (
        f"\n⚠️ <i>Adjusted down from ${intended_amount:.2f} (balance dropped during timelock)</i>"
        if adjusted else ""
    )
    text = (
        "🏦 <b>VAULT SWEEP COMPLETED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💸 <b>Swept:</b> ${actual_amount:.2f} pUSD → ${usdce_received:.2f} USDC.e{adjusted_line}\n"
        f"💰 <b>Trading wallet remaining:</b> ${remaining:.2f}\n"
        f"🏛️ <b>Vault total received:</b> ${vault_total:.2f} USDC.e\n\n"
        f'🔍 <a href="https://polygonscan.com/tx/{unwrap_tx}">View unwrap transaction</a>\n'
        f'🔍 <a href="https://polygonscan.com/address/{VAULT_WALLET_ADDRESS}">View vault</a>\n\n'
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_sweep_cancelled(
    http_client: httpx.AsyncClient,
    balance: float,
) -> None:
    text = (
        "🏦 <b>VAULT SWEEP CANCELLED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ Balance dropped to ${balance:.2f} during the timelock.\n"
        f"🛡️ Floor is ${VAULT_SWEEP_FLOOR_USDC:.2f} — nothing to sweep.\n\n"
        "Deposit wallet has been unpaused. Trading resumed.\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_cap_change(
    http_client: httpx.AsyncClient,
    old_cap: int,
    new_cap: int,
    bankroll: float,
    bet_size: float,
) -> None:
    text = (
        "📊 <b>Position cap adjusted</b>\n\n"
        f"{old_cap} → <b>{new_cap}</b> concurrent positions\n"
        f"💵 Bankroll: ${bankroll:.2f}  •  Bet size: ${bet_size:.2f}\n"
        f"🎯 Target exposure: {TRADING_TARGET_EXPOSURE_PCT * 100:.0f}%"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_tier_transition(
    http_client: httpx.AsyncClient,
    new_tier: str,
    open_positions: int,
) -> None:
    effective_normal = min(_current_max_positions, TRADING_NORMAL_POSITIONS_MAX)
    if new_tier == "premium":
        text = (
            f"📈 <b>Position cap: entering premium tier ({open_positions}/{TRADING_PREMIUM_POSITIONS_MAX} filled)</b>\n\n"
            f"Only alerts ≥{TRADING_PREMIUM_SCORE_THRESHOLD} will fire until positions drop below {effective_normal}.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    elif new_tier == "hardcap":
        text = (
            f"🛑 <b>Position cap: {TRADING_PREMIUM_POSITIONS_MAX}/{TRADING_PREMIUM_POSITIONS_MAX} reached</b>\n\n"
            f"All new alerts skipped until a position resolves.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    else:  # normal
        text = (
            f"📉 <b>Position cap: back to normal tier ({open_positions}/{effective_normal})</b>\n\n"
            f"All qualifying alerts admitted.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
    await _send_telegram(http_client, text, ops=True)


async def _notify_sweep_stuck(http_client: httpx.AsyncClient) -> None:
    text = (
        "⚠️ <b>VAULT SWEEP — MANUAL ACTION REQUIRED</b>\n\n"
        "Deposit wallet is stuck in a paused state.\n"
        "Automated unpause failed. Trading may be affected.\n\n"
        f"<code>Wallet: {VAULT_WALLET_ADDRESS}</code>\n\n"
        "Run <code>unpause()</code> manually via the contract."
    )
    await _send_telegram(http_client, text, ops=True)


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
    await _send_telegram(http_client, text, ops=True)


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
    await _send_telegram(http_client, text, ops=True)


def _format_loss_lines(recent_losses: list) -> str:
    """Format the recent-loss list into Telegram HTML lines."""
    if not recent_losses:
        return "  (no resolved trades available)"
    lines = []
    for t in recent_losses:
        q     = (t.get("market_question") or "Unknown")[:60]
        side  = t.get("bet_side") or "?"
        price = t.get("bet_price_intended") or 0.0
        score = t.get("score") or "?"
        lines.append(f"  • {q}\n    {side} @ {price:.3f} · Score {score}")
    return "\n".join(lines)


async def _notify_loss_streak_warning(
    http_client: httpx.AsyncClient,
    consecutive: int,
    recent_losses: list,
) -> None:
    loss_lines = _format_loss_lines(recent_losses)
    balance = _cached_usdc_balance if _cached_usdc_balance >= 0 else 0.0
    text = (
        f"⚠️ <b>Loss streak: {consecutive} consecutive losses</b>\n\n"
        f"💵 Bankroll: <b>${balance:.2f} USDC</b>\n"
        f"📊 No pause yet — circuit breaker fires at {TRADING_CONSECUTIVE_LOSS_PAUSE}.\n\n"
        f"<b>Recent losses:</b>\n{loss_lines}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await _send_telegram(http_client, text, ops=True)


async def _notify_loss_streak_pause(
    http_client: httpx.AsyncClient,
    consecutive: int,
    balance: float,
    resume_ts: int,
    recent_losses: list,
) -> None:
    resume_str = time.strftime("%H:%M UTC", time.gmtime(resume_ts))
    loss_lines = _format_loss_lines(recent_losses)
    bal_str = f"${balance:.2f} USDC" if balance >= 0 else "unknown"
    text = (
        f"🚨 <b>CIRCUIT BREAKER — TRADING PAUSED</b>\n\n"
        f"<b>{consecutive} consecutive losses detected.</b>\n"
        f"⏸ Paused for {TRADING_PAUSE_DURATION_SECONDS // 60} min · resumes at {resume_str}\n"
        f"💵 Bankroll: <b>{bal_str}</b>\n\n"
        f"<b>Recent losses:</b>\n{loss_lines}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Investigate if unexpected. Bot resumes automatically."
    )
    await _send_telegram(http_client, text)


async def _notify_cb_drawdown_pause(
    http_client: httpx.AsyncClient,
    rolling_loss: float,
    threshold_usdc: float,
    bankroll: float,
    resume_ts: int,
    recent_losses: list,
) -> None:
    resume_str = time.strftime("%H:%M UTC", time.gmtime(resume_ts))
    loss_lines = _format_loss_lines(recent_losses)
    pct = (rolling_loss / max(bankroll, 1.0)) * 100
    text = (
        f"🚨 <b>CIRCUIT BREAKER — TRADING PAUSED</b>\n\n"
        f"Rolling {TRADING_CB_WINDOW_HOURS:.0f}h loss: <b>${rolling_loss:.2f}</b> "
        f"({pct:.1f}% of bankroll, threshold {TRADING_CB_DRAWDOWN_PCT * 100:.0f}%)\n"
        f"⏸ Paused for {TRADING_PAUSE_DURATION_SECONDS // 60} min · resumes at {resume_str}\n"
        f"💵 Bankroll: <b>${bankroll:.2f} USDC</b>\n\n"
        f"<b>Recent losses:</b>\n{loss_lines}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Investigate if unexpected. Bot resumes automatically."
    )
    await _send_telegram(http_client, text)


# Running win/loss streak for spectator-feed flavor (in-memory; resets on restart,
# like the CB rolling state). Positive = win run, negative = loss run.
# Running win/loss streak in ROUNDS — one poll-cycle grading batch = one round, so a
# big catch-up batch can't read as "32 STRAIGHT". Positive = win rounds, negative =
# loss rounds, 0 = neutral/just-snapped. In-memory; resets on restart like CB state.
_result_streak: int = 0


def _apply_round_streak(won: int, lost: int) -> None:
    """Fold one poll-cycle's grading batch into the streak as a single round: a clean
    all-win batch extends the win run, a clean all-loss batch extends the loss run, a
    mixed batch (a loss snuck in) breaks it, a voids-only batch is a no-op."""
    global _result_streak
    if won > 0 and lost == 0:
        _result_streak = _result_streak + 1 if _result_streak > 0 else 1
    elif lost > 0 and won == 0:
        _result_streak = _result_streak - 1 if _result_streak < 0 else -1
    elif won > 0 and lost > 0:
        _result_streak = 0


def _streak_snap(prev: int, now: int) -> str:
    """Loud one-off banner when a notable (>=3 round) run just ended, else ''."""
    if prev >= 3 and now < prev:
        return f"💔 <b>HEATER OVER</b> — the {prev}-round win run ends"
    if prev <= -3 and now > prev:
        return f"🎉 <b>SKID SNAPPED</b> — {abs(prev)}-round slide is over"
    return ""


def _streak_headline(status: str, streak: int) -> tuple[str, str]:
    """Pure (headline, banter) for one resolution, given the cycle's round streak."""
    if status == "won":
        n = streak if streak > 0 else 1
        if n >= 10:
            return f"👑 <b>WIN — {n} STRAIGHT</b>", "this is illegal. we're reporting it to the Polymarket Commission."
        if n >= 7:
            return f"⚡ <b>WIN — {n} DEEP</b> 🚀", "at this point the bot is just disrespecting the markets."
        if n >= 5:
            return f"🌋 <b>WIN — {n} STRAIGHT</b> 🔥", "the model is cooked but somehow hitting. we don't ask questions."
        if n >= 3:
            return f"🔥🔥 <b>WIN — {n} in a row</b>", "hat trick+. the bot has opinions now. we're a bit worried."
        if n == 2:
            return "✅🔥 <b>WIN — back to back</b>", "statistically meaningless. emotionally enormous."
        return "✅ <b>WIN</b>", "on the board."
    if status == "lost":
        n = -streak if streak < 0 else 1
        if n >= 7:
            return f"🫠 <b>LOSS — {n} straight</b>", "the bot has taken a vow of silence. we respect it."
        if n >= 5:
            return f"💀💀 <b>LOSS — {n} straight</b>", "we've dispatched a sports psychologist. the bot declined the session."
        if n >= 3:
            return f"💀 <b>LOSS — {n} in a row</b>", "the bot is doing its best. its best is, admittedly, not great."
        if n == 2:
            return "❌😬 <b>LOSS — that's two</b>", "a wobble. perfectly normal. nothing to see here."
        return "❌ <b>LOSS</b>", "onto the next."
    return "↩️ <b>VOID</b>", "refunded — no harm, no foul."


# Live "ticker" thresholds — when a resolution clears one of these, the routine
# win/loss callout gets a loud banner so the genuinely big moments pop out of the feed.
# Deliberately SELECTIVE so banners stay rare; frequent momentum energy is carried by the
# streak headlines. Pure presentation, env-tunable.
#
# 2026-06-03 — retuned to the ACTUAL stake scale. With ~$2-5 bets, the old $25 big-win /
# $50 huge / $10 brutal thresholds were UNREACHABLE (max profit on a $5 bet is ~$5), so the
# banners were effectively dead code. Dollar tiers are now reachable on this bankroll; the
# longshot tier (≤20¢) is dormant under the 0.50 favorites floor and will light up only if
# that floor is ever lowered (the open forward-test) — kept intact for that case.
_TICKER_LONGSHOT_MAX: float = float(os.getenv("TICKER_LONGSHOT_MAX_PRICE", "0.20"))
_TICKER_BIG_WIN_USDC: float = float(os.getenv("TICKER_BIG_WIN_USDC", "3"))
_TICKER_UPSET_FAV_PRICE: float = float(os.getenv("TICKER_UPSET_FAV_PRICE", "0.80"))
_TICKER_BRUTAL_LOSS_USDC: float = float(os.getenv("TICKER_BRUTAL_LOSS_USDC", "3"))


def _big_moment(status: str, fill_price: float, pnl: float):
    """Return (banner, banter) for a threshold-clearing resolution, else None.
    Pure presentation over the already-computed entry price + P&L — touches no
    trading/sizing/accounting state. One banner per resolution; within a status the
    rarer/bigger story wins (longshot before big-$, upset before brutal-$). Returns
    None for 'invalid'/void status and for any missing (None) inputs."""
    if fill_price is None or pnl is None:
        return None
    if status == "won":
        if 0.0 < fill_price <= _TICKER_LONGSHOT_MAX:
            odds = round(1.0 / fill_price)
            if fill_price <= 0.10:
                return (f"🦄 <b>UNICORN — {odds}:1 LONGSHOT CASHES</b>",
                        "called it when nobody else would. 🤯")
            return (f"🎰 <b>LONGSHOT HITS — {odds}:1</b>", "big odds, ice veins. cash. 🧊")
        if pnl >= _TICKER_BIG_WIN_USDC:
            tier = "💎 <b>HUGE WIN</b>" if pnl >= 2 * _TICKER_BIG_WIN_USDC else "🚀 <b>BIG WIN</b>"
            return (f"{tier} — ${pnl:+.2f}", "that's the dinner bill. 🦞")
    elif status == "lost":
        if fill_price >= _TICKER_UPSET_FAV_PRICE:
            return (f"💀 <b>UPSET — backed the {fill_price * 100:.0f}% fav, got cooked</b>",
                    "that's the one that keeps you humble. 😮‍💨")
        if pnl <= -_TICKER_BRUTAL_LOSS_USDC:
            return (f"💸 <b>BRUTAL BEAT — ${pnl:+.2f}</b>", "oof. pour one out. 🫗")
    return None


async def _notify_trade_resolution(
    http_client: httpx.AsyncClient,
    trade: dict,
    resolution_status: str,
    pnl: float,
    streak: int = 0,
    snap: str = "",
) -> None:
    import html as _html

    market_q = trade.get("market_question") or trade.get("market_id", "")
    bet_side = trade.get("bet_side", "")
    fill_price = trade.get("bet_price_filled") or trade.get("bet_price_intended") or 0.0
    winning_outcome = trade.get("winning_outcome")

    headline, banter = _streak_headline(resolution_status, streak)
    moment = _big_moment(resolution_status, fill_price, pnl)
    # Cap the header at ONE prepended banner so the market title still shows in the phone's
    # notification preview. A rare big-moment (UNICORN / BIG WIN / UPSET) outranks a streak-snap.
    if moment:
        headline = f"{moment[0]}\n{headline}"
        banter = moment[1]
    elif snap:
        headline = f"{snap}\n{headline}"

    # Plain-money outcome line, matched to the audience (no "P&L"/"USDC" jargon). Wins show the
    # cash multiple — the screenshot-and-post moment.
    price_c = f"{fill_price*100:.0f}¢" if fill_price and fill_price > 0 else "?"
    if resolution_status == "won":
        mult = f"  (x{1.0/fill_price:.1f})" if fill_price and fill_price > 0 else ""
        money_line = f"💰 <b>Won ${pnl:.2f}</b>{mult}"
    elif resolution_status == "lost":
        money_line = f"💸 <b>Lost ${abs(pnl):.2f}</b>"
    else:
        money_line = "↩️ <b>Refunded</b> — push, no harm"

    outcome_line = (
        f"🏆 {_html.escape(str(winning_outcome))} took it\n" if winning_outcome else ""
    )

    text = (
        f"{headline}\n\n"
        f"📋 {_html.escape(market_q[:120])}\n"
        f"🎯 {_html.escape(str(bet_side))} at {price_c}\n"
        f"{outcome_line}"
        f"{money_line}\n"
        f"<i>{banter}</i>"
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

    alert_id    = alert["alert_id"]
    market_id   = alert["market_id"]
    market_q    = alert.get("market_question") or market_id
    bet_side    = alert["bet_side"]
    price_alert = float(alert["bet_price_at_alert"])
    score       = int(alert["score"])
    token_id    = alert.get("clob_token_id")
    _slug       = alert.get("market_slug")
    market_url  = f"https://polymarket.com/event/{_slug}" if _slug else None

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

    # Soccer-favorites filter: sports-category alerts at price > 0.50 have demonstrated
    # negative edge (n=12, ROI=-39.9%, avg_excess=-0.275 in CSV ground truth).
    market_category = alert.get("market_category") or ""
    if (FILTER_SOCCER_FAVORITES_ENABLED
            and market_category == "sports"
            and price_alert > FILTER_SOCCER_FAVORITES_MAX_PRICE):
        _alert_skip_cache[alert_id] = time.time() + _SKIP_DECISION_TTL_SECONDS
        log.info(
            "[Filter] Soccer favorite skipped: alert=%s cat=%s price=%.3f",
            alert_id[:12], market_category, price_alert,
        )
        await _notify_skip(http_client, alert, "soccer_favorite",
                           price_intended=price_alert, price_current=price_alert)
        try:
            await _api_post(http_client, "/api/skips/telemetry", {
                "alert_id":         alert_id,
                "market_id":        market_id,
                "market_question":  market_q,
                "bet_side":         bet_side,
                "score":            score,
                "market_type":      market_category,
                "price_intended":   price_alert,
                "price_current":    price_alert,
                "price_delta_abs":  0.0,
                "price_delta_frac": 0.0,
                "static_threshold": FILTER_SOCCER_FAVORITES_MAX_PRICE,
                "gate_outcome":     "rejected_soccer_favorite",
            })
        except Exception as _exc:
            log.debug("[Filter] Soccer skip telemetry error (non-fatal): %s", _exc)
        return

    # Skip-decision cache: if we already rejected this alert for a price-based reason
    # within the last 5 min, skip silently without hitting the price API again.
    if _alert_skip_cache.get(alert_id, 0) > time.time():
        return

    bet_size = await _calculate_bet_size(http_client, stats, score=score)

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

    if slippage is not None and slippage > TRADING_SLIPPAGE_THRESHOLD:
        log.warning(
            "[Trade] High slippage for alert %s: intended=%.3f current=%.3f delta=%.3f",
            alert_id[:12], price_alert, current_price, slippage,
        )
        _gate_outcome: Optional[str] = None
        try:
            _gate_outcome = _evaluate_dynamic_gate(price_alert, current_price, slippage)
            _t = asyncio.create_task(
                _post_skip_telemetry(http_client, alert, slippage, current_price, _gate_outcome, bet_size)
            )
            _background_tasks.add(_t)
            _t.add_done_callback(_background_tasks.discard)
        except Exception as _exc:
            log.debug("[Shadow] Gate/telemetry error (non-fatal): %s", _exc)

        _will_trade_expanded = (
            TRADING_DYNAMIC_SLIPPAGE_ENABLED and _gate_outcome == "would_have_traded"
        )

        if _will_trade_expanded:
            log.info(
                "[Trade] Dynamic gate PASS for alert %s: current=%.3f within expanded tolerance",
                alert_id[:12], current_price,
            )
        else:
            _alert_skip_cache[alert_id] = time.time() + _SKIP_DECISION_TTL_SECONDS
            await _notify_skip(
                http_client, alert, "slippage",
                price_intended=price_alert,
                price_current=current_price,
                slippage_delta=slippage,
            )
            return

    fill_price:  Optional[float] = None
    order_id:    Optional[str]   = None
    filled_size: float = 0.0   # default for error paths; overridden inside try on success
    status = "error"
    error_msg: Optional[str] = None

    try:
        # Resolve neg_risk from the DB-supplied market record first (populated by
        # the Railway side from markets.raw_json). Fall back to the CLOB API only
        # if the DB value is absent (e.g., market record predates the negRisk field).
        # If neither source works, skip the trade — defaulting to False previously
        # caused order_version_mismatch on negRisk=True markets (May 2026 incident).
        _neg_risk_from_db = alert.get("neg_risk")
        if _neg_risk_from_db is not None:
            neg_risk = _neg_risk_from_db
        else:
            try:
                neg_risk = await asyncio.to_thread(clob_client.get_neg_risk, token_id)
            except Exception as _nr_exc:
                log.error(
                    "[Trade] get_neg_risk failed for token %s and no DB fallback — "
                    "skipping alert %s: %s",
                    token_id, alert_id[:12], _nr_exc,
                )
                await _api_post(http_client, "/api/trades", {
                    "alert_id": alert_id, "market_id": market_id,
                    "market_question": market_q, "clob_token_id": token_id,
                    "bet_side": bet_side, "bet_price_intended": price_alert,
                    "size_usdc": TRADING_BET_SIZE_USDC,
                    "status": "error",
                    "error_message": f"get_neg_risk failed: {_nr_exc}",
                })
                return

        log.debug("[Trade] token=%s neg_risk=%s sig_type=%d funder=%s",
                  token_id[:16], neg_risk, TRADING_SIGNATURE_TYPE,
                  TRADING_FUNDER_ADDRESS[:10] + "..." if TRADING_FUNDER_ADDRESS else "self")

        # FAK: fills available liquidity, kills remainder — eliminates FOK "no match" on thin books.
        # order_type must be set on BOTH MarketOrderArgs (for price calculation) and the call
        # (for what's posted to the exchange).
        order = MarketOrderArgs(token_id=token_id, amount=bet_size, side=Side.BUY,
                                order_type=OrderType.FAK)
        options = PartialCreateOrderOptions(neg_risk=neg_risk)
        resp = await asyncio.to_thread(
            clob_client.create_and_post_market_order, order, options, OrderType.FAK
        )

        filled_size = bet_size  # default; overridden below for confirmed partial fills
        if isinstance(resp, dict):
            success = resp.get("success", False)
            error_msg = resp.get("errorMsg") or None
            order_id = resp.get("orderID") or resp.get("id")
            resp_status = (resp.get("status") or "").lower()

            if success or resp_status == "matched":
                fill_price = float(
                    resp.get("price") or resp.get("avgPrice") or current_price or price_alert
                )
                # Query order detail to get the actual fill (may be < bet_size for FAK).
                # IMPORTANT (audit 2026-06-11): get_order's size_matched is denominated in
                # SHARES (outcome tokens), NOT USDC. The cash actually spent = shares × fill
                # price. The old code recorded the raw share count as size_usdc, which inflated
                # P&L (a $0.08-entry longshot booked ~12x its cost — the phantom "+$127.78 win")
                # and broke the partial-fill check below (shares vs dollars). Convert to USDC.
                # Retry once on failure; if still unresolvable, mark fill-unconfirmed so
                # the trade is excluded from CB/bankroll math rather than defaulting to
                # the intended full amount.
                if order_id:
                    _get_order_result: Optional[dict] = None
                    _get_order_err: Optional[Exception] = None
                    for _attempt in range(2):
                        try:
                            _get_order_result = await asyncio.to_thread(clob_client.get_order, order_id)
                            _get_order_err = None
                            break
                        except Exception as _ge:
                            _get_order_err = _ge
                            if _attempt == 0:
                                await asyncio.sleep(2)

                    if _get_order_result is not None and isinstance(_get_order_result, dict):
                        raw_matched = (
                            _get_order_result.get("size_matched")
                            or _get_order_result.get("sizeMatched")
                            or _get_order_result.get("matched_amount")
                        )
                        if raw_matched is not None:
                            # shares × price = USDC cash spent (fill_price set above, >0 on a match).
                            _shares = float(raw_matched)
                            filled_size = _shares * fill_price if fill_price > 0 else bet_size
                        else:
                            # Exchange returned an order dict but no size field — unconfirmed.
                            log.warning(
                                "[Trade] FAK %s: get_order missing size_matched after retry — "
                                "marking fill-unconfirmed (order=%s). resp=%s",
                                alert_id[:12], order_id[:12], _get_order_result,
                            )
                            filled_size = 0.0
                            status = "fill-unconfirmed"
                            error_msg = "FAK fill amount unconfirmed: size_matched absent from get_order"
                    elif _get_order_err is not None:
                        log.warning(
                            "[Trade] FAK %s: get_order failed after retry (%s) — "
                            "marking fill-unconfirmed (order=%s)",
                            alert_id[:12], _get_order_err, order_id[:12],
                        )
                        filled_size = 0.0
                        status = "fill-unconfirmed"
                        error_msg = f"FAK fill amount unconfirmed: get_order failed: {_get_order_err}"

                if status != "fill-unconfirmed":
                    if filled_size >= bet_size * 0.99:
                        status = "filled"
                    elif filled_size > 0:
                        status = "partial"
                        log.info(
                            "[Trade] FAK partial fill %s: $%.2f / $%.2f USDC @ %.4f",
                            alert_id[:12], filled_size, bet_size, fill_price,
                        )
                    else:
                        status = "rejected"
                        error_msg = "FAK matched but size_matched=0"
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

    # Report to Railway — size_usdc is the actual filled amount, not the intended size.
    # _check_pending_resolutions reads size_usdc for P&L; recording it correctly keeps CB accurate.
    await _api_post(http_client, "/api/trades", {
        "alert_id":           alert_id,
        "market_id":          market_id,
        "market_question":    market_q,
        "clob_token_id":      token_id,
        "bet_side":           bet_side,
        "bet_price_intended": price_alert,
        "bet_price_filled":   fill_price,
        "slippage":           slippage,
        "size_usdc":          filled_size,
        "order_id":           order_id,
        "status":             status,
        "error_message":      error_msg,
    })

    log.info(
        "[Trade] %s | %s | side=%s | $%.2f | %s | fill=%s",
        alert_id[:12], market_q[:40], bet_side, filled_size, status,
        f"{fill_price:.4f}" if fill_price else "N/A",
    )

    if status in ("filled", "partial"):
        if filled_size > 0:
            global _session_avg_bet
            _session_avg_bet = 0.9 * _session_avg_bet + 0.1 * filled_size
        # Track held position for per-market/side dedup.
        if market_id and bet_side:
            _held_positions.add((market_id, bet_side))
            log.debug("[Dedup] Added (%s, %s) to _held_positions", market_id[:16], bet_side)
        await _notify_trade_filled(
            http_client, market_q, bet_side,
            fill_price or current_price or price_alert,
            filled_size, score, slippage, market_url,
            alert_created_at=int(alert.get("created_at") or 0),
            score_breakdown_json=alert.get("score_breakdown_json"),
        )
    elif status == "error" and _is_geoblock(error_msg):
        # Geoblock: trip a circuit-breaker so we stop hammering POST /order, and alert
        # ONCE (not a TRADE ERROR per signal). _check_risk_limits skips cycles during
        # the cooldown; new attempts auto-resume after it. The real fix is rotating the
        # egress IP (move fly region) — this just keeps the feed clean meanwhile.
        global _geoblock_pause_until
        _now = time.time()
        _newly = _geoblock_pause_until <= _now
        _geoblock_pause_until = _now + GEOBLOCK_PAUSE_SECONDS
        if _newly:
            log.error("[Geoblock] Order region-restricted — pausing new trades %ds", GEOBLOCK_PAUSE_SECONDS)
            await _notify_geoblock_pause(http_client, int(_geoblock_pause_until))
    else:
        await _notify_trade_error(
            http_client, market_q, bet_side, price_alert, score, status, error_msg,
        )


# ---------------------------------------------------------------------------
# Per-market/side dedup — startup seed
# ---------------------------------------------------------------------------

async def _seed_held_positions(http_client: httpx.AsyncClient) -> None:
    """
    Seed _held_positions from /api/positions/open with retry + backoff.
    Must complete before the poll loop processes any alert.
    On persistent failure, dedup stays inactive (_held_positions_seeded=False)
    and a recurring WARNING is emitted each cycle until a restart recovers it.
    """
    global _held_positions, _held_positions_seeded
    for attempt in range(5):
        try:
            positions = await _api_get(http_client, "/api/positions/open")
            if isinstance(positions, list):
                _held_positions = {
                    (p["market_id"], p["bet_side"])
                    for p in positions
                    if p.get("market_id") and p.get("bet_side")
                }
                _held_positions_seeded = True
                log.info(
                    "[Dedup] Seeded _held_positions: %d open positions",
                    len(_held_positions),
                )
                return
        except Exception as exc:
            log.warning("[Dedup] Seed attempt %d/5 failed: %s", attempt + 1, exc)
        await asyncio.sleep(2 ** attempt)   # 1, 2, 4, 8, 16 s
    log.warning(
        "[Dedup] _seed_held_positions failed after 5 attempts — "
        "per-market dedup INACTIVE until next restart"
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

    # Newly-resolved trades this cycle — effectively one grading batch from our POV.
    new = [t for t in data
           if t.get("alert_id")
           and t.get("alert_resolution_status", "pending") != "pending"
           and t["alert_id"] not in _notified_resolutions]
    if not new:
        return

    # Tie-aware streak: fold this whole batch into ONE round before posting anything,
    # so a grading catch-up of many trades can't inflate the headline to "32 STRAIGHT".
    cyc_won = sum(1 for t in new if t.get("alert_resolution_status") == "resolved_won")
    cyc_lost = sum(1 for t in new if t.get("alert_resolution_status") == "resolved_lost")
    prev_streak = _result_streak
    _apply_round_streak(cyc_won, cyc_lost)
    snap = _streak_snap(prev_streak, _result_streak)

    first = True
    for trade in new:
        alert_id = trade["alert_id"]
        alert_status = trade.get("alert_resolution_status", "pending")

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

        # Write resolution back to Railway so stats stay accurate.
        # Mark as 'prospective' so the CB backfill and sizing graduation
        # can distinguish these from historical backfill artifacts.
        resolved_ts = int(time.time())
        await _api_patch(http_client, f"/api/trades/{alert_id}/resolution", {
            "resolution_status": resolution_status,
            "pnl": pnl,
            "resolved_at": resolved_ts,
            "resolution_source": "prospective",
        })

        # Accumulate for magnitude-based circuit breaker; prune entries outside 2× window
        _cb_pnl_history.append((float(resolved_ts), pnl))
        cutoff = resolved_ts - TRADING_CB_WINDOW_HOURS * 3600 * 2
        while _cb_pnl_history and _cb_pnl_history[0][0] < cutoff:
            _cb_pnl_history.pop(0)

        _notified_resolutions.add(alert_id)

        # Remove from dedup set so a re-signal on the same market+side can trade again.
        _mid = trade.get("market_id", "")
        _bside = trade.get("bet_side", "")
        if _mid and _bside:
            _held_positions.discard((_mid, _bside))
            log.debug("[Dedup] Cleared (%s, %s) after resolution", _mid[:16], _bside)

        # Streak is the cycle's round value; the snap banner rides only the first post.
        await _notify_trade_resolution(http_client, trade, resolution_status, pnl,
                                       streak=_result_streak, snap=snap if first else "")
        first = False

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

    NOTE (deposit wallet / auto-redemption): the user enabled Polymarket's
    platform-level auto-redemption during onboarding. Polymarket's backend will
    automatically redeem winning positions to the deposit wallet, making the
    CTF call below a no-op in normal operation. This function is left running
    as a safety net in case auto-redemption misses a position, but it can be
    removed in a future cleanup once we confirm auto-redemption is reliable.
    """
    global _low_balance_warned, _cached_usdc_balance

    data = await _api_get(http_client, "/api/trades/pending")
    if not isinstance(data, list):
        return

    # Positions that are filled (or partially filled) and won, not yet redeemed this session.
    # "partial" included so FAK partial-fill wins reach the safety-net CTF redemption.
    redeemable = [
        t for t in data
        if t.get("status") in ("filled", "partial")
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
    global _wallet_address, _last_resolution_check, _last_redemption_check, _last_positions_summary, _sweep_state, _sweep_paused_at, _sweep_intended_amount, _sweep_last_date, _current_max_positions, _legacy_max_positions_ceiling, _current_tier, _cb_pnl_history, _cached_usdc_balance, _session_avg_bet, _held_positions, _held_positions_seeded

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
    log.info("Wallet (EOA, signs/gas): %s", _wallet_address)
    log.info("Funder (USDC balance):   %s",
             TRADING_FUNDER_ADDRESS if TRADING_FUNDER_ADDRESS else f"{_wallet_address} (EOA)")
    log.info("Poll:        every %ds | Min score: %d", POLL_INTERVAL, TRADING_MIN_SCORE)
    log.info("Vault:       %s  sweep=%s  threshold=$%.0f  floor=$%.0f",
             VAULT_WALLET_ADDRESS or "disabled",
             "ARMED" if VAULT_SWEEP_ENABLED else "dry-run",
             VAULT_SWEEP_THRESHOLD_USDC, VAULT_SWEEP_FLOOR_USDC)
    log.info("Scaling:     capital=$%.0f  headroom=$%.0f  exposure=%.0f%%  floor=%d  normal=%d  premium=%d  score_floor=%d",
             TRADING_WORKING_CAPITAL_USDC, TRADING_SWEEP_HEADROOM_USDC,
             TRADING_TARGET_EXPOSURE_PCT * 100,
             TRADING_MAX_POSITIONS_FLOOR, TRADING_NORMAL_POSITIONS_MAX,
             TRADING_PREMIUM_POSITIONS_MAX, TRADING_PREMIUM_SCORE_THRESHOLD)
    log.info("=" * 60)

    if os.getenv("TRADING_MAX_CONCURRENT_POSITIONS"):
        _legacy_max_positions_ceiling = TRADING_MAX_CONCURRENT_POSITIONS
        log.warning(
            "[Trader] DEPRECATED: TRADING_MAX_CONCURRENT_POSITIONS=%d is set. "
            "Using as ceiling override. New approach uses TRADING_TARGET_EXPOSURE_PCT.",
            TRADING_MAX_CONCURRENT_POSITIONS,
        )

    try:
        clob_client = await _init_clob_client()
        log.info("CLOB client initialised (host=%s)", TRADING_CLOB_HOST)
    except Exception as exc:
        log.critical("CLOB client init failed: %s", exc)
        sys.exit(1)

    # Recover sweep state if the bot restarted mid-sweep (wallet may be paused).
    if TRADING_FUNDER_ADDRESS and VAULT_WALLET_ADDRESS:
        try:
            paused_ts = await asyncio.to_thread(_get_wallet_paused_timestamp_sync)
            if paused_ts > 0:
                elapsed = time.time() - paused_ts
                log.warning("[Vault] Deposit wallet is paused (since %d, %.0fs ago)", paused_ts, elapsed)
                if elapsed >= _DEPOSIT_WALLET_TIMELOCK_SECONDS:
                    log.warning("[Vault] Timelock already elapsed — unpausing on startup")
                    try:
                        await asyncio.to_thread(_unpause_deposit_wallet_sync)
                        log.info("[Vault] Deposit wallet unpaused on startup")
                    except Exception as ue:
                        log.error("[Vault] Startup unpause failed: %s", ue)
                else:
                    _sweep_paused_at = float(paused_ts)
                    _sweep_state = "pause_pending"
                    log.warning("[Vault] Restoring sweep state=pause_pending (%.0fs remaining)", _DEPOSIT_WALLET_TIMELOCK_SECONDS - elapsed)
        except Exception as exc:
            log.warning("[Vault] Could not check pause state on startup: %s", exc)

    # 3-min lookback on startup: covers measured ~70s restart window (Railway deploy
    # trigger → container start) plus one POLL_INTERVAL, with safety margin.
    # 7200s prior caused replay of up to 2h of stale alerts on every restart.
    last_processed_ts = int(time.time()) - 180

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        # If a sweep was already recorded today (e.g. bot restarted after sweeping),
        # restore _sweep_last_date so we don't double-fire during the sweep hour.
        if VAULT_WALLET_ADDRESS:
            try:
                from datetime import datetime, timezone as _tz
                _today_utc = datetime.now(_tz.utc).strftime("%Y-%m-%d")
                vault_stats = await _api_get(http_client, "/api/stats/vault") or {}
                last_swept_at = vault_stats.get("last_swept_at")
                if last_swept_at:
                    from datetime import datetime as _dt, timezone as _tz2
                    swept_date = _dt.fromtimestamp(last_swept_at, tz=_tz2.utc).strftime("%Y-%m-%d")
                    if swept_date == _today_utc:
                        _sweep_last_date = _today_utc
                        log.info("[Vault] Sweep already recorded today (%s) — per-day guard set", _today_utc)
            except Exception as exc:
                log.warning("[Vault] Could not check last sweep date on startup: %s", exc)

        # Seed CB history from Railway so the circuit breaker isn't blind after a restart.
        # Then evaluate immediately — if the reconstructed window already breaches the
        # threshold, engage the pause now rather than on the first poll cycle.
        try:
            await _backfill_cb_pnl_history(http_client)
            _cached_usdc_balance = await _get_usdc_balance()
            block = await _check_risk_limits({}, http_client=http_client)
            if block:
                log.warning("[CB] Post-seed evaluation triggered on boot: %s", block)
        except Exception as exc:
            log.warning("[CB] Backfill/post-seed evaluation error (non-fatal): %s", exc)

        # Seed per-market/side dedup set BEFORE processing any alerts.
        # If this fails, dedup stays inactive and a warning fires each cycle.
        await _seed_held_positions(http_client)

        # Immediately grade any alerts the Railway resolution_checker has already
        # resolved (alert_outcomes), so the position cap unblocks on restart.
        # NOTE: on-chain backfill (_resolve_from_clob_positions) was removed — its
        # curPrice grading was unreliable for negative-risk / "No"-side markets.
        # Resolution is now sourced solely from alert_outcomes (Gamma, side-matched).
        try:
            await _check_pending_resolutions(http_client)
        except Exception as exc:
            log.warning("[Resolution] Startup resolution sync failed (non-fatal): %s", exc)

        while True:
            try:
                now = time.time()

                # Resolution poll (every 10 min) — sourced from alert_outcomes only.
                if now - _last_resolution_check >= _RESOLUTION_POLL_INTERVAL:
                    await _check_pending_resolutions(http_client)
                    _last_resolution_check = now

                # Vault sweep — runs every cycle. In idle state the function returns
                # immediately unless it's the configured daily sweep hour and the
                # per-day guard hasn't fired yet. In pause_pending / pause_ready it
                # advances the state machine on each cycle.
                if VAULT_WALLET_ADDRESS:
                    await _check_and_sweep(http_client)

                # Redemption check (also refreshes _cached_usdc_balance for the pause below)
                if now - _last_redemption_check >= REDEMPTION_CHECK_INTERVAL:
                    await _check_and_redeem(http_client)
                    _last_redemption_check = now

                # Periodic open-positions summary (every POSITIONS_SUMMARY_INTERVAL_SECONDS)
                if now - _last_positions_summary >= POSITIONS_SUMMARY_INTERVAL_SECONDS:
                    await _send_positions_summary(http_client)
                    _last_positions_summary = now

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
                stats = await _api_get(http_client, "/api/stats/trading")
                if stats is None:
                    log.warning("[Trader] Stats unavailable — skipping trade cycle (fail-closed)")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # Dynamic position cap — recompute every cycle.
                # Formula: open_positions + floor(free_balance / avg_bet).
                # open_positions is already known (stats just fetched).
                # _session_avg_bet is an EMA of actual fill sizes, so the cap
                # directly reflects how many more orders the wallet can fund.
                _open_now = stats.get("open_positions", 0)
                _bankroll = max(_cached_usdc_balance, 0.0)
                _est_bet  = max(TRADING_MIN_BET_USDC, _session_avg_bet)
                new_cap = _compute_max_positions(_bankroll, _est_bet, open_positions=_open_now)
                if new_cap != _current_max_positions:
                    log.info(
                        "[Trader] Max positions: %d → %d (open=%d free=$%.2f avg_bet=$%.2f exposure=%.0f%%)",
                        _current_max_positions, new_cap, _open_now, _bankroll, _est_bet,
                        TRADING_TARGET_EXPOSURE_PCT * 100,
                    )
                    if _current_max_positions > 0 and abs(new_cap - _current_max_positions) >= 5:
                        await _notify_cap_change(http_client, _current_max_positions, new_cap, _bankroll, _est_bet)
                    _current_max_positions = new_cap
                _tier_now = _get_tier(_open_now, _current_tier)
                _eff_normal = min(_current_max_positions, TRADING_NORMAL_POSITIONS_MAX)
                if _tier_now == "normal":
                    log.info("[Trader] Position tier: normal (%d/%d)", _open_now, _eff_normal)
                elif _tier_now == "premium":
                    log.info("[Trader] Position tier: premium (%d/%d, score floor %d)",
                             _open_now, TRADING_PREMIUM_POSITIONS_MAX, TRADING_PREMIUM_SCORE_THRESHOLD)
                else:
                    log.info("[Trader] Position tier: hard-cap reached (%d/%d)",
                             _open_now, TRADING_PREMIUM_POSITIONS_MAX)
                if _tier_now != _current_tier:
                    log.info("[Trader] Tier transition: %s → %s", _current_tier, _tier_now)
                    await _notify_tier_transition(http_client, _tier_now, _open_now)
                    _current_tier = _tier_now

                block_reason = await _check_risk_limits(stats, http_client=http_client)
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
                    stats = await _api_get(http_client, "/api/stats/trading")
                    if stats is None:
                        log.warning("[Trader] Stats unavailable mid-batch — halting batch (fail-closed)")
                        break
                    block_reason = await _check_risk_limits(stats, http_client=http_client)
                    if block_reason:
                        log.info("[Risk] Mid-loop block: %s — halting batch", block_reason)
                        await _notify_skip(http_client, alert, block_reason)
                        break

                    # Tier check: premium mode requires minimum score
                    _alert_open = stats.get("open_positions", 0)
                    tier_reason = _check_tier_for_alert(_alert_open, alert.get("score", 0))
                    if tier_reason:
                        log.info("[Trader] Tier skip: %s", tier_reason)
                        continue

                    # Per-market/side dedup: skip if already holding this market+side.
                    if not _held_positions_seeded:
                        log.warning(
                            "[Dedup] Dedup inactive (seed failed on startup) — "
                            "duplicate trades possible until next restart"
                        )
                    else:
                        _dmid  = alert.get("market_id", "")
                        _dside = alert.get("bet_side", "")
                        if _dmid and _dside and (_dmid, _dside) in _held_positions:
                            log.info(
                                "[Dedup] Skipping %s — already holding %s/%s",
                                (alert.get("alert_id") or "")[:12], _dmid[:16], _dside,
                            )
                            await _notify_skip(http_client, alert, "already holding position")
                            continue

                    # Max-age discard (source-2 fix): reject CB/daily-limit-trapped signals
                    # that survived until the gate cleared but are now stale.
                    # Telemetry is idempotent (INSERT OR IGNORE); spinning cycles are benign.
                    _alert_age_s = int(time.time()) - int(alert.get("created_at") or 0)
                    if _alert_age_s > MAX_SIGNAL_AGE_S:
                        _age_min = _alert_age_s // 60
                        _max_min = MAX_SIGNAL_AGE_S // 60
                        _stale_reason = f"signal too old — {_age_min} min since detection, max {_max_min} min"
                        log.info(
                            "[Stale] Discarding alert %s: age=%ds > MAX_SIGNAL_AGE_S=%ds",
                            (alert.get("alert_id") or "")[:12], _alert_age_s, MAX_SIGNAL_AGE_S,
                        )
                        _t = asyncio.create_task(_post_stale_telemetry(http_client, alert))
                        _background_tasks.add(_t)
                        _t.add_done_callback(_background_tasks.discard)
                        await _notify_skip(http_client, alert, _stale_reason)
                        continue

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
