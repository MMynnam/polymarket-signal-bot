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

# --- Brain conviction size-up (2026-06-23) ---------------------------------
# When the shadow LLM "brain" (Railway) has INDEPENDENTLY confirmed this exact
# market+side with high conviction, size the trade up toward BRAIN_SIZEUP_MAX_USDC
# — putting more weight on the bets the brain's own research agrees with, and
# posting its rationale on the betslip. This is a confluence signal (insider alert
# AND the brain's researched view land on the same side). SAFE BY DESIGN:
#   • OFF by default — arm with BRAIN_SIZEUP_ENABLED=true (fly secret).
#   • Only ever sizes the SINGLE initial trade (no follow-on top-ups → normal
#     one-execution-per-alert accounting / resolution / P&L; nothing orphaned).
#   • Never exceeds free cash above the sweep reserve (can't turn a fundable base
#     trade into a skip), and is bounded per day.
#   • Every rail still runs on the bigger size (slippage gate, reserve gate,
#     daily-loss stop, magnitude circuit breaker, vig gate).
# The brain is unvalidated (forward Brier vs market still accumulating), so this
# is deliberately capped and gated — it adds weight, not leverage.
BRAIN_SIZEUP_ENABLED: bool = os.getenv("BRAIN_SIZEUP_ENABLED", "false").lower() in ("true", "1", "yes")
BRAIN_SIZEUP_MAX_USDC: float = float(os.getenv("BRAIN_SIZEUP_MAX_USDC", "15.0"))
BRAIN_SIZEUP_MIN_CONFIDENCE: float = float(os.getenv("BRAIN_SIZEUP_MIN_CONFIDENCE", "0.75"))
BRAIN_SIZEUP_MIN_EDGE: float = float(os.getenv("BRAIN_SIZEUP_MIN_EDGE", "0.12"))
BRAIN_SIZEUP_MAX_PER_DAY: int = int(os.getenv("BRAIN_SIZEUP_MAX_PER_DAY", "5"))
BRAIN_CONFIRM_POLL_SECONDS: int = int(os.getenv("BRAIN_CONFIRM_POLL_SECONDS", "300"))
BRAIN_CONFIRM_LOOKBACK_HOURS: float = float(os.getenv("BRAIN_CONFIRM_LOOKBACK_HOURS", "18"))
# Real-time vet: ask the brain to analyze each alert AT TRADE TIME (synchronous ~20-40s call
# to /api/brain/vet) so it can actually weigh in before the position is taken — the whole point.
# When off, falls back to the polled confirmation cache (which rarely matches fresh alerts).
BRAIN_REALTIME_VET_ENABLED: bool = os.getenv("BRAIN_REALTIME_VET_ENABLED", "false").lower() in ("true", "1", "yes")
BRAIN_VET_TIMEOUT_S: float = float(os.getenv("BRAIN_VET_TIMEOUT_S", "90"))
# Brain VETO skip (operator chose "skip high-conviction vetoes" 2026-06-23): when the real-time
# vet strongly DISAGREES with a bet, skip it entirely — the brain gets veto power over the signal.
# Bars are LOWER than the size-up's (0.75): declining is the safe direction — you avoid a bet, you
# don't add money. NOTE: the fast no-web vet is humble, so strong vetoes (and thus skips) are
# uncommon on same-day sports; lower the bars or enable BRAIN_VET_WEB_SEARCH to make it veto more.
# The brain's veto is GRADED on resolution (via alert_outcomes), so we learn if its skips were right.
BRAIN_VETO_SKIP_ENABLED: bool = os.getenv("BRAIN_VETO_SKIP_ENABLED", "false").lower() in ("true", "1", "yes")
BRAIN_VETO_MIN_CONFIDENCE: float = float(os.getenv("BRAIN_VETO_MIN_CONFIDENCE", "0.50"))
BRAIN_VETO_MIN_EDGE: float = float(os.getenv("BRAIN_VETO_MIN_EDGE", "0.10"))
# Brain Picks: the brain's OWN research-driven trades (synthetic alerts with alert_id 'brain_…').
# These bypass the insider-specific gates (soccer filter, real-time vet — it IS the brain's pick),
# trade at a fixed tiny DISCOVERY stake, and are bounded per day. Gated separately so the brain's
# new thin-market strategy can be armed/killed independent of insider trading. All risk rails
# (reserve, daily-loss, circuit breaker, position cap) still apply.
# Live dashboard (2026-07-04, "re-imagine the Telegram UI"): ONE pinned audience message,
# edited in place every DASHBOARD_UPDATE_SECONDS — bank, vault, open book, today's record,
# brain activity. Gives V1 Poly a heartbeat even when the insider stream is quiet (diagnosed:
# between World Cup rounds 13/14 alerts were dedup repeats → 28h of channel silence).
DASHBOARD_ENABLED: bool = os.getenv("DASHBOARD_ENABLED", "true").lower() in ("true", "1", "yes")
DASHBOARD_UPDATE_SECONDS: int = int(os.getenv("DASHBOARD_UPDATE_SECONDS", "600"))

BRAIN_PICK_TRADING_ENABLED: bool = os.getenv("BRAIN_PICK_TRADING_ENABLED", "false").lower() in ("true", "1", "yes")
# Conviction-scaled pick stakes (2026-07-04): the picks went 7-0 (+34% ROI) at $1 flat — the
# only strategy in this bot's history with a winning record. Stake now scales with the brain's
# researched edge: base + SLOPE × (edge − 0.08), capped. At the 0.08 min edge → $2; at 0.28 → $5.
# Cap matches the $5 single-position rail. Still discovery-scale; a durable ✅ on the PICKS
# calibration line earns the next raise.
BRAIN_PICK_SIZE_USDC: float = float(os.getenv("BRAIN_PICK_SIZE_USDC", "2.0"))
BRAIN_PICK_MAX_SIZE_USDC: float = float(os.getenv("BRAIN_PICK_MAX_SIZE_USDC", "5.0"))
BRAIN_PICK_EDGE_SLOPE: float = float(os.getenv("BRAIN_PICK_EDGE_SLOPE", "15.0"))
BRAIN_PICK_MAX_PER_DAY: int = int(os.getenv("BRAIN_PICK_MAX_PER_DAY", "8"))
TRADING_DYNAMIC_MIN_RESOLVED: int = int(os.getenv("TRADING_DYNAMIC_MIN_RESOLVED", "20"))
POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL", "30"))
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
# Optional separate "ops" channel for operational noise (skips, errors, sweeps, tier/cap
# changes, low-balance, geoblock, redemptions). The audience channel (TELEGRAM_CHAT_ID) then
# only carries the SHOW: bets placed, resolutions, recaps. 2026-06-03.
#
# Feed v2 (2026-06-12): if TELEGRAM_OPS_CHAT_ID is unset, ops messages are DROPPED from
# Telegram (logged instead) — they used to fall back into the main channel silently, which
# leaked the insider score (skip cards, error cards, loss lines) into the audience feed.
# Set FEED_OPS_TO_MAIN=true to restore the old silent-in-main fallback. This mirrors the
# Railway make_research_sender() contract (TELEGRAM_OPS_CHAT_ID / FEED_RESEARCH_TO_MAIN).
TELEGRAM_OPS_CHAT_ID: str = os.getenv("TELEGRAM_OPS_CHAT_ID", "")
FEED_OPS_TO_MAIN: bool = os.getenv("FEED_OPS_TO_MAIN", "false").strip().lower() in ("true", "1", "yes")
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

# ---------------------------------------------------------------------------
# Continuous win-ratchet (2026-06-13; rewritten 2026-06-22 to ACTUALLY MOVE FUNDS).
# Each win earmarks SWEEP_WIN_PCT of its PROFIT toward the vault (_vault_tab); losses
# stay in the float. The on-chain settle reuses the proven pause→1h-timelock→withdraw→
# unwrap machine. The hard part is the 1h timelock: the bot re-bets free cash within
# minutes, so by withdraw time the cash would be gone and the sweep would self-cancel.
# FIX: when a sweep is pending, the bot RESERVES the in-flight amount (won't bet it),
# and it always keeps VAULT_RATCHET_FLOOR_USDC unbet as a buffer. So the cash survives
# the timelock and the withdraw actually lands. Per-sweep is capped at MAX_BATCH so the
# reserve never freezes trading for long. This relocates winnings to safety — it does
# NOT create edge — but it preserves what the bot wins instead of round-tripping it.
VAULT_RATCHET_ENABLED: bool = os.getenv("VAULT_RATCHET_ENABLED", "true").lower() == "true"
SWEEP_WIN_PCT: float = float(os.getenv("SWEEP_WIN_PCT", "0.50"))  # share of each win's PROFIT banked
# Small thresholds so sweeps ACTUALLY FIRE on this thin, fully-invested wallet (the old
# $8 batch / $40 floor never triggered — free cash never got near $48).
VAULT_RATCHET_MIN_SETTLE_USDC: float = float(os.getenv("VAULT_RATCHET_MIN_SETTLE_USDC", "3.0"))
# Operating floor: free USDC the bot always keeps unbet (so a sweep can start, and so the
# bot keeps a little working capital). Intentionally low — the point is to bank winnings
# even as the float erodes.
VAULT_RATCHET_FLOOR_USDC: float = float(os.getenv("VAULT_RATCHET_FLOOR_USDC", "6.0"))
# Cap per on-chain sweep. Bounds how much cash the reserve holds out of trading during the
# 1h timelock, so trading never freezes for a big batch; the tab drains over several sweeps.
VAULT_RATCHET_MAX_BATCH_USDC: float = float(os.getenv("VAULT_RATCHET_MAX_BATCH_USDC", "20.0"))

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

# Opposite-side vig gate (2026-06-13 edge audit). 33% of alerted markets get alerts on
# BOTH sides; copying both locks in the insiders' combined overround. For a binary,
# holding side A at price pa and then buying side B at pb with pa+pb>1 spends >$1 to
# guarantee a $1 return (exactly one side wins) = a mechanical locked loss of (pa+pb-1)
# per share — verified WR=50% by identity, ~45-54% of the tradeable stream, -3.5..-4.9pp
# drag. So: once we hold one side of a market, only take a DIFFERENT side if the two
# entry prices sum to <= this cap (i.e. the pair locks in >= a small profit / is a real
# cheap hedge). Set to >=1.0 to restore the old behavior (always take the second side).
TRADING_OPPOSITE_SIDE_MAX_SUM: float = float(os.getenv("TRADING_OPPOSITE_SIDE_MAX_SUM", "0.98"))

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
_vault_tab: float = 0.0             # win-ratchet: profit earmarked but not yet swept on-chain
                                    # (in-process target; the TRUE banked total is the vault's
                                    # on-chain balance, tracked in _cached_vault_balance)
_cached_vault_balance: float = -1.0  # last-read REAL on-chain vault USDC.e balance (-1 = unknown)
_cached_vault_balance_at: float = 0.0  # unix ts of the last vault-balance read
_ratchet_last_log: float = 0.0      # throttle for the dry-run "would secure" log
_current_max_positions: int = TRADING_MAX_CONCURRENT_POSITIONS  # updated each cycle by _compute_max_positions
_legacy_max_positions_ceiling: Optional[int] = None  # set at startup if old env var is detected
_current_tier: str = "normal"  # "normal" | "premium" | "hardcap"; drives transition notifications
_held_positions: set[tuple[str, str]] = set()  # (market_id, bet_side); per-market dedup
# market_id -> {bet_side: entry_price}; powers the opposite-side vig gate (don't take a
# second side of a market unless the two legs lock in a profit). Kept in lockstep with
# _held_positions at every seed/add/discard site.
_held_side_px: dict[str, dict[str, float]] = {}
_held_positions_seeded: bool = False  # False until startup seed succeeds

# Brain conviction size-up state. _brain_confirmations: (market_id, side) ->
# {confidence, edge, brain_prob, market_price, take} from /api/brain/confirmations,
# refreshed every BRAIN_CONFIRM_POLL_SECONDS. Daily counter bounds sized-up bets.
_brain_confirmations: dict[tuple[str, str], dict] = {}
_last_brain_confirm_poll: float = 0.0
_brain_sizeups_today: int = 0
_brain_sizeup_day: str = ""
_brain_picks_today: int = 0
_brain_pick_day: str = ""

# Live dashboard state: the pinned message we edit in place. In-process only — on restart
# we post a fresh dashboard and re-pin (the old one just stops updating; Telegram replaces
# the pin).
_dash_msg_id: Optional[int] = None
_last_dash_update: float = 0.0

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
        f"   Ratchet: bank {SWEEP_WIN_PCT*100:.0f}% of wins → vault (tab ${_vault_tab:.2f}, "
        f"settle ≥${VAULT_RATCHET_MIN_SETTLE_USDC:.0f}, floor ${VAULT_RATCHET_FLOOR_USDC:.0f}, "
        f"{'ARMED' if VAULT_SWEEP_ENABLED else 'dry-run'})\n"
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


# ---------------------------------------------------------------------------
# Win-ratchet helpers — pure decisions (tested) + the tab mutators.
# ---------------------------------------------------------------------------

def _ratchet_slice(profit: float, pct: float = SWEEP_WIN_PCT) -> float:
    """The slice of a single win's PROFIT to bank. 0 for losses / non-positive profit."""
    if not profit or profit <= 0:
        return 0.0
    return round(profit * pct, 4)


def _ratchet_settle_amount(tab: float, free_balance, floor: float = VAULT_RATCHET_FLOOR_USDC,
                           min_settle: float = VAULT_RATCHET_MIN_SETTLE_USDC,
                           max_batch: float = VAULT_RATCHET_MAX_BATCH_USDC) -> float:
    """How much to move on-chain this cycle: min(tab, free above floor, max_batch), but only
    once the tab clears min_settle AND that much cash is actually free above the operating
    floor. Capped at max_batch so the reserve never holds too much out of trading at once.
    Returns 0.0 when no settle should fire (dust, no headroom, or below the min batch)."""
    if tab < min_settle or free_balance is None:
        return 0.0
    headroom = free_balance - floor
    if headroom < min_settle:
        return 0.0
    return round(min(tab, headroom, max_batch), 4)


def _sweep_reserve_usdc() -> float:
    """Free USDC the bot must NOT bet: the operating floor always, plus the in-flight sweep
    amount while a sweep is pending (so the cash survives the 1h timelock to the withdraw)."""
    reserve = VAULT_RATCHET_FLOOR_USDC
    if _sweep_state in ("pause_pending", "pause_ready"):
        reserve += _sweep_intended_amount
    return reserve


def _accrue_vault_tab(profit: float) -> float:
    """Bank a win's slice into the tab. Returns the slice (for the feed). No-op on losses."""
    global _vault_tab
    slice_ = _ratchet_slice(profit)
    if slice_ > 0:
        _vault_tab = round(_vault_tab + slice_, 4)
    return slice_


def _debit_vault_tab(amount: float) -> None:
    """Reduce the tab after a successful on-chain settle (clamped at 0)."""
    global _vault_tab
    _vault_tab = round(max(0.0, _vault_tab - amount), 4)


async def _refresh_vault_balance(force: bool = False) -> float:
    """Read the REAL on-chain vault USDC.e balance into the cache (at most every 10 min,
    unless forced). This is the honest 'secured' number shown to the feed — never the tab.
    Returns the cached value (-1.0 if never successfully read)."""
    global _cached_vault_balance, _cached_vault_balance_at
    now = time.time()
    if not force and _cached_vault_balance >= 0 and (now - _cached_vault_balance_at) < 600:
        return _cached_vault_balance
    if not VAULT_WALLET_ADDRESS:
        return _cached_vault_balance
    try:
        bal = await asyncio.to_thread(_get_vault_usdce_balance_sync)
        _cached_vault_balance = float(bal)
        _cached_vault_balance_at = now
    except Exception as exc:
        log.debug("[Vault] balance refresh failed (non-fatal): %s", exc)
    return _cached_vault_balance


async def _check_and_sweep(http_client: httpx.AsyncClient) -> None:
    """
    Vault sweep state machine: pause() → 1h timelock → withdrawERC20() → unpause().

    Phase 1 (idle):  Win-ratchet trigger — sweep when the accrued tab clears the min batch
                     and there's free cash above the operating floor (legacy daily/threshold
                     trigger retired in favor of the continuous ratchet, 2026-06-13).
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
    global _cached_vault_balance, _cached_vault_balance_at

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
                # Same operating floor Phase 1 used to size the ratchet sweep — otherwise
                # the withdraw would self-cancel against the legacy (higher) floor.
                _p3_floor = VAULT_RATCHET_FLOOR_USDC
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
            # Ratchet: the banked slice is now locked on-chain — clear it from the tab
            # and update the cached real vault balance for the feed's "secured" number.
            if VAULT_SWEEP_TEST_AMOUNT_USDC <= 0:
                _debit_vault_tab(actual_amount)
            if vault_total and vault_total > 0:
                _cached_vault_balance = float(vault_total)
                _cached_vault_balance_at = time.time()
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
    # Phase 1 (idle) — win-ratchet trigger (continuous), or fixed test amount
    # ------------------------------------------------------------------
    global _ratchet_last_log
    from datetime import datetime, timezone
    is_test = VAULT_SWEEP_TEST_AMOUNT_USDC > 0

    if is_test:
        # Operator one-shot: sweep a fixed amount once per day to exercise the on-chain path.
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if _sweep_last_date == today:
            return
        intended_amount = VAULT_SWEEP_TEST_AMOUNT_USDC
        balance = None
    else:
        if not VAULT_RATCHET_ENABLED:
            return
        if _vault_tab < VAULT_RATCHET_MIN_SETTLE_USDC:
            return  # tab too small to bother with an on-chain batch
        try:
            balance = await _get_usdc_balance()
        except Exception as exc:
            log.warning("[Vault] Balance fetch failed: %s — skipping ratchet check", exc)
            return
        intended_amount = _ratchet_settle_amount(_vault_tab, balance)
        if intended_amount <= 0:
            return  # tab is ready but no free cash above the operating floor yet

    try:
        matic = await asyncio.to_thread(_get_matic_balance_sync)
        if matic < _SWEEP_MIN_MATIC:
            log.warning("[Vault] Insufficient MATIC (%.4f) — skipping sweep", matic)
            return  # don't consume the trigger — retry next poll if MATIC recovers
    except Exception as exc:
        log.warning("[Vault] MATIC check failed: %s — skipping sweep", exc)
        return

    if not VAULT_SWEEP_ENABLED:
        # Dry-run: the ratchet still accrues and the feed shows the lifeline growing, but no
        # money moves until the operator arms it. Throttle the log (sweep runs every cycle).
        now = time.time()
        if now - _ratchet_last_log > 600:
            _ratchet_last_log = now
            log.info("[Vault] DRY RUN (VAULT_SWEEP_ENABLED=false): tab $%.2f | would secure "
                     "$%.2f to the vault (free $%s, floor $%.2f)",
                     _vault_tab, intended_amount,
                     f"{balance:.2f}" if balance is not None else "?", VAULT_RATCHET_FLOOR_USDC)
        if is_test:
            _sweep_last_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return

    log.info("[Vault] Initiating ratchet sweep (tab $%.2f, securing $%.2f)...",
             _vault_tab, intended_amount)
    try:
        await asyncio.to_thread(_initiate_sweep_pause_sync)
        _sweep_paused_at       = time.time()
        _sweep_intended_amount = intended_amount
        _sweep_state           = "pause_pending"
        if is_test:
            _sweep_last_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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

async def _send_telegram(
    http_client: httpx.AsyncClient,
    text: str,
    *,
    ops: bool = False,
    silent: Optional[bool] = None,
    reply_to: Optional[int] = None,
    buttons: Optional[list] = None,
) -> Optional[int]:
    """Send a Telegram message. Returns the sent message_id (for reply-threading), or None.

    ops=False (default) → the AUDIENCE feed (TELEGRAM_CHAT_ID), with a push notification.
    ops=True            → operational noise. Goes to TELEGRAM_OPS_CHAT_ID if configured
                          (loud, for the operator). With no ops channel it is DROPPED from
                          Telegram and logged instead — ops content carries the insider
                          score and must not reach the audience feed. FEED_OPS_TO_MAIN=true
                          restores the pre-v2 silent-in-main fallback.
    silent              → explicit notification override for AUDIENCE messages (feed v2
                          notification discipline: betslips + routine settles are silent;
                          big moments, recaps and pauses buzz). Ignored for ops routing.
    reply_to            → message_id to thread under (settle replies to its betslip).
                          Sends standalone if the target is gone (allow_sending_without_reply).
    buttons             → [(label, url), ...] rendered as one row of inline URL buttons.
    """
    if not TELEGRAM_BOT_TOKEN:
        return None
    if ops and TELEGRAM_OPS_CHAT_ID:
        chat_id, quiet = TELEGRAM_OPS_CHAT_ID, False
    elif ops and not FEED_OPS_TO_MAIN:
        # No ops channel → ops output stays out of the audience feed entirely.
        log.info("[Telegram] ops message dropped (no TELEGRAM_OPS_CHAT_ID): %.150s",
                 text.replace("\n", " | "))
        return None
    else:
        chat_id = TELEGRAM_CHAT_ID
        quiet = ops if silent is None else bool(silent)  # ops fallback → silent in main
    if not chat_id:
        return None
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_notification": quiet,
        "link_preview_options": {"is_disabled": True},
    }
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    if buttons:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": str(lbl), "url": str(url)} for lbl, url in buttons]]
        }
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Bounded retry on TRANSIENT failures (429/5xx/network) so a one-blip outage
    # during a multi-settle grading batch doesn't permanently eat a card — settles
    # are one-shot (marked notified + PATCHed regardless of send outcome, by design).
    # Hard 4xx (e.g. 400 can't-parse-entities) never retries. Never raises.
    for attempt in range(3):
        try:
            resp = await http_client.post(url, json=payload, timeout=15)
            if resp.is_success:
                try:
                    return int(resp.json()["result"]["message_id"])
                except Exception:
                    return None
            if resp.status_code == 429 and attempt < 2:
                try:
                    delay = float(resp.json()["parameters"]["retry_after"])
                except Exception:
                    delay = 2.0
                await asyncio.sleep(min(delay, 5.0))
                continue
            if resp.status_code >= 500 and attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            log.warning("[Telegram] Send failed %d: %s", resp.status_code, resp.text[:100])
            return None
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            log.warning("[Telegram] Send error: %s", exc)
            return None
    return None


# ---------------------------------------------------------------------------
# Live dashboard — one pinned audience message, edited in place
# ---------------------------------------------------------------------------

async def _tg_call(http_client: httpx.AsyncClient, method: str, payload: dict) -> Optional[dict]:
    """Single best-effort Telegram Bot API call. Returns the result dict or None."""
    if not TELEGRAM_BOT_TOKEN:
        return None
    try:
        resp = await http_client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", json=payload, timeout=15)
        if resp.is_success:
            return resp.json().get("result")
        return None
    except Exception:
        return None


def _build_dashboard_text(*, bank: float, vault: float, open_n: int, cap: int, tier: str,
                          today_w: int, today_l: int, today_pnl: float,
                          picks_today: int, sizeups_today: int, ts_utc: str) -> str:
    """Pure dashboard card. One glance = the whole state of the operation. Edited in place,
    so it always carries a fresh timestamp (also defeats Telegram's 'not modified' rejection)."""
    total = today_w + today_l
    dots = ("🟩" * today_w + "🟥" * today_l) if 0 < total <= 12 else ""
    if today_pnl > 0:
        day_word = "up"
    elif today_pnl < 0:
        day_word = "down"
    else:
        day_word = "flat"
    record_bit = f"<b>{today_w}W-{today_l}L</b> · {day_word} <b>${abs(today_pnl):.2f}</b>" if total else "<i>no settles yet</i>"
    brain_bit = []
    if picks_today:
        brain_bit.append(f"{picks_today} pick{'s' if picks_today != 1 else ''}")
    if sizeups_today:
        brain_bit.append(f"{sizeups_today} size-up{'s' if sizeups_today != 1 else ''}")
    brain_line = f"\n🧠 brain today: {', '.join(brain_bit)}" if brain_bit else ""
    vault_bit = f" · 🏛 vault <b>${vault:.2f}</b>" if vault >= 0 else ""
    return (
        f"📟 <b>V1 POLY — LIVE BOOK</b>\n"
        f"💰 bank <b>${bank:.2f}</b>{vault_bit}\n"
        f"🎫 <b>{open_n}</b> open positions ({tier} {open_n}/{cap})\n"
        f"📅 today: {record_bit} {dots}"
        f"{brain_line}\n"
        f"<i>auto-updates · last {ts_utc} UTC</i>"
    )


async def _update_dashboard(http_client: httpx.AsyncClient, stats: dict) -> None:
    """Refresh the pinned live dashboard (rate-limited). First run posts + pins; afterwards
    edits in place. All failures are silent-best-effort — the dashboard must never interfere
    with trading."""
    global _dash_msg_id, _last_dash_update
    if not DASHBOARD_ENABLED or not TELEGRAM_CHAT_ID:
        return
    now = time.time()
    if now - _last_dash_update < DASHBOARD_UPDATE_SECONDS:
        return
    _last_dash_update = now
    try:
        # Today's record from resolutions since UTC midnight (prospective only).
        midnight = int(time.mktime(time.strptime(time.strftime("%Y-%m-%d", time.gmtime()), "%Y-%m-%d")))
        resolved = await _api_get(http_client, "/api/trades/resolved-recent",
                                  params={"since": midnight}) or []
        today_w = sum(1 for r in resolved if r.get("resolution_status") == "won")
        today_l = sum(1 for r in resolved if r.get("resolution_status") == "lost")
        today_pnl = sum(float(r.get("pnl") or 0.0) for r in resolved)
        open_n = int(stats.get("open_positions", 0))
        cap = min(_current_max_positions or TRADING_NORMAL_POSITIONS_MAX, TRADING_NORMAL_POSITIONS_MAX) \
            if _current_tier == "normal" else TRADING_PREMIUM_POSITIONS_MAX
        text = _build_dashboard_text(
            bank=max(_cached_usdc_balance, 0.0), vault=_cached_vault_balance,
            open_n=open_n, cap=cap, tier=_current_tier,
            today_w=today_w, today_l=today_l, today_pnl=today_pnl,
            picks_today=_brain_picks_today, sizeups_today=_brain_sizeups_today,
            ts_utc=time.strftime("%H:%M", time.gmtime()),
        )
        if _dash_msg_id is None:
            result = await _tg_call(http_client, "sendMessage", {
                "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML",
                "disable_notification": True, "link_preview_options": {"is_disabled": True}})
            if result and result.get("message_id"):
                _dash_msg_id = int(result["message_id"])
                await _tg_call(http_client, "pinChatMessage", {
                    "chat_id": TELEGRAM_CHAT_ID, "message_id": _dash_msg_id,
                    "disable_notification": True})
                log.info("[Dashboard] posted + pinned live dashboard (msg %s)", _dash_msg_id)
        else:
            edited = await _tg_call(http_client, "editMessageText", {
                "chat_id": TELEGRAM_CHAT_ID, "message_id": _dash_msg_id, "text": text,
                "parse_mode": "HTML", "link_preview_options": {"is_disabled": True}})
            if edited is None:
                # Message deleted or edit window closed — repost next tick.
                log.info("[Dashboard] edit failed — will repost a fresh dashboard")
                _dash_msg_id = None
    except Exception as exc:
        log.debug("[Dashboard] update error (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Feed v2 — betslip registry + house copy pools
# ---------------------------------------------------------------------------

# alert_id → Telegram message_id of its betslip, so the settle can thread as a
# reply and read as one self-contained "did it hit" card. In-memory and capped:
# a restart simply means the next settles post standalone (graceful).
_slip_msgs: dict = {}
_SLIP_MSGS_CAP = 600


def _remember_slip(alert_id: str, message_id: Optional[int]) -> None:
    if not alert_id or not message_id:
        return
    if len(_slip_msgs) >= _SLIP_MSGS_CAP:  # FIFO-ish evict (insertion-ordered dict)
        _slip_msgs.pop(next(iter(_slip_msgs)), None)
    _slip_msgs[alert_id] = message_id


def _pick_line(pool: list, seed: int) -> str:
    """Deterministic rotation through a copy pool — varied feed, reproducible tests."""
    return pool[seed % len(pool)] if pool else ""


# Odds vocabulary — price is the only honest signal (the insider score was proven
# non-predictive noise), so every card leads with the market's own odds, in plain words.
_ODDS_BANDS = [   # (max_price, key, display word)
    (0.15, "big_longshot", "big longshot"),
    (0.35, "underdog", "underdog"),
    (0.60, "coin_flip", "coin-flip"),
    (0.80, "favorite", "favorite"),
    (1.01, "heavy_chalk", "heavy chalk"),
]

# House copy pools — written by a 3-register comedy panel + head-writer pass
# (2026-06-12). Rotated deterministically per slip so the feed never repeats
# itself two bets in a row. Honest down-bad voice; no score, no cheerleading.
_SLIP_BANTER = {
    "big_longshot": [
        "the math says no. the heart says $2.",
        "it found a 12-cent dream and put real money on it.",
        "statistically doomed. spiritually undefeated.",
        "if this hits, we're framing the slip.",
        "long odds, short money, infinite belief.",
    ],
    "underdog": [
        "it took the dog. somewhere a favorite is plotting its betrayal.",
        "the dog has a puncher's chance and the bot has $3. let's dance.",
        "backing the dog. us losers stick together.",
        "everyone says no. the bot said $2 worth of yes.",
        "live look at us begging the underdog to do it one time.",
    ],
    "coin_flip": [
        "a coin flip. the bot brought a dollar and a feeling.",
        "fifty-fifty. historically the bot finds a third option.",
        "we could've flipped a real coin for free, but this has graphs.",
        "heads we celebrate, tails we have material.",
        "50/50. the bot specializes in finding the wrong 50.",
    ],
    "favorite": [
        "backing the favorite. the favorite knows what it did last time.",
        "back with the favorites. our most toxic relationship.",
        "a favorite, lightly seasoned with dread. in it goes.",
        "favorites owe this bot an apology tour, honestly.",
        "trusting a favorite again. fool me 47 times.",
    ],
    "heavy_chalk": [
        "ninety cents on a lock. locks around here come pre-picked.",
        "big favorite. small payout. the risk is purely emotional.",
        "should be free money. 'should' carries this entire operation.",
        "laying chalk to win a gumball. we are not serious people.",
        "so safe it's insulting. sweating it anyway, obviously.",
    ],
    "mystery": ["no read on this one — buckle up."],
}
_HIT_LINES = [
    "a win?? in this economy??",
    "a win. routine, professional, suspicious.",
    "it bet the right side AND the right side won. growth.",
    "won. don't ask about the lifetime number. just clap.",
    "the vault is still empty, but it's a happier empty.",
    "green. small, but legally green.",
    "correct outcome achieved. on purpose, we're told.",
]
_MISS_LINES = [
    "loss. anyway.",
    "wrong again. the bot remains undefeated at this.",
    "that money lives somewhere nicer now.",
    "it's not a loss. it's a subscription to humility.",
    "the bot would like to formally blame the favorite.",
    "a miss. the dollar died doing what it loved: leaving.",
    "another $2 donated to people smarter than us.",
]
_TIMEOUT_LINES = [
    "the bot benched itself. first good read of the day.",
    "losing too fast; the bot called its own timeout. growth, sort of.",
    "the bot rage quit, responsibly. back soon.",
    "trading halted by management. management is the bot. it knows.",
    "benched. even the bot knows when to stop, and it's a bot.",
]
# Win-ratchet: copy for EARMARKING a slice of a win toward the vault. Honest wording — the
# slice is set ASIDE (a target); it only moves on-chain when a batch sweep lands (the
# SECURED moment, _SWEEP_SECURED_LINES). Never claim it's already secured here.
_RATCHET_LINES = [
    "set aside for the vault.",
    "earmarked. waiting on the next sweep.",
    "tagged for the vault. the bot's trying to keep this one.",
    "another slice toward the next sweep.",
    "half of it has a one-way ticket to the vault.",
    "stacking the next sweep, win by win.",
]
# Win-ratchet: copy for the actual on-chain SECURED moment (the batch settle lands).
_SWEEP_SECURED_LINES = [
    "that's real, on-chain, and ours forever.",
    "moved to safety. the market can't claw it back now.",
    "the vault just got heavier. the bot earned a day.",
    "secured. whatever happens next, we keep this.",
]


def _odds_key(price) -> tuple[str, str]:
    """(pool_key, display_word) for an entry price; ('mystery', …) when unknowable."""
    if price is None or not (0 < price < 1):
        return "mystery", "mystery line"
    for cap, key, word in _ODDS_BANDS:
        if price <= cap:
            return key, word
    return "mystery", "mystery line"


def _odds_read(price: float) -> tuple[str, str]:
    """Back-compat shim: (odds_word, a banter line). Prefer _odds_key + _pick_line."""
    key, word = _odds_key(price)
    return word, _SLIP_BANTER[key][0]


def _build_slip_text(
    market_q: str,
    bet_side: str,
    fill_price: float,
    size: float,
    bet_no: Optional[int] = None,
    brain_verdict: Optional[dict] = None,
    brain_sized_up: bool = False,
    is_brain_pick: bool = False,
) -> str:
    """Pure betslip card (feed v2). One bet = one slip: number, odds word, the line,
    stake → payout, one rotating banter line. No score, no bank line, no link clutter
    (the market link rides as an inline button). Whenever the brain VETTED this bet (in
    real time at trade time), its verdict + reasoning ride on the slip — agree, coin-flip,
    or disagree — so the brain's read is visible on every position it weighed in on."""
    import html as _html

    profit_if_win = (size / fill_price) - size if fill_price and fill_price > 0 else 0.0
    safe_q   = _html.escape(str(market_q)[:90])
    safe_side = _html.escape(str(bet_side))
    key, word = _odds_key(fill_price)
    seed     = bet_no if bet_no is not None else int(size * 100) + int((fill_price or 0) * 100)
    banter   = _pick_line(_SLIP_BANTER.get(key) or _SLIP_BANTER["mystery"], seed)
    price_c  = f"{fill_price*100:.0f}¢" if fill_price and fill_price > 0 else "?¢"
    no_bit   = f" #{bet_no}" if bet_no else ""

    brain_line = ""
    if brain_verdict and brain_verdict.get("verdict"):
        v = brain_verdict["verdict"]
        take = _html.escape(str(brain_verdict.get("take") or "")[:220])
        odds_bit = ""
        try:
            odds_bit = f" — its read {float(brain_verdict['brain_prob'])*100:.0f}% vs market {float(brain_verdict['market_price'])*100:.0f}%"
        except Exception:
            pass
        if is_brain_pick or v == "PICK":
            head = f"🧠 <b>the brain's own edge</b>{odds_bit}"
        elif brain_sized_up:
            head = "🧠 <b>BRAIN CONVICTION — sized up</b>"
        elif v == "CONFIRM":
            head = f"🧠 <b>brain agrees</b>{odds_bit}"
        elif v == "VETO":
            head = f"🧠 <b>brain leans the other way</b>{odds_bit} · riding the signal anyway"
        else:  # NEUTRAL / anything else — no edge, parking at the market price
            head = f"🧠 <b>brain's with the market</b>{odds_bit} · no edge to add"
        brain_line = f"\n\n{head}" + (f"\n<i>{take}</i>" if take else "")

    header = (f"🧠 <b>BRAIN PICK{no_bit}</b> · {word}" if is_brain_pick
              else f"🎟 <b>BET{no_bit}</b> · {word}")
    return (
        f"{header}\n"
        f"<b>{safe_side}</b> — {safe_q}\n\n"
        f"{price_c} · ${size:.2f} riding to win <b>${profit_if_win:.2f}</b>\n"
        f"<i>{banter}</i>{brain_line}"
    )


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
    alert_id: str = "",
    bet_no: Optional[int] = None,
    brain_verdict: Optional[dict] = None,
    brain_sized_up: bool = False,
    is_brain_pick: bool = False,
) -> None:
    # AUDIENCE betslip — feed v2. SILENT push (notification discipline: anticipation is
    # browsable, results buzz). The slip's message_id is remembered so the settle threads
    # under it as a reply and each bet reads as one self-contained card.
    # (score / slippage / alert_created_at / score_breakdown_json kept in the signature
    # for callers + ops parity, intentionally unused here — the score is dead, long live
    # the price.) brain_verdict shows the brain's read; is_brain_pick flags its OWN trade.
    text = _build_slip_text(market_q, bet_side, fill_price, size, bet_no,
                            brain_verdict=brain_verdict, brain_sized_up=brain_sized_up,
                            is_brain_pick=is_brain_pick)
    buttons = [("⚡ watch it live", str(market_url))] if market_url else None
    msg_id = await _send_telegram(http_client, text, silent=True, buttons=buttons)
    _remember_slip(alert_id, msg_id)


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

    # AUDIENCE: the payoff moment — winnings just got locked on-chain where they can't be
    # lost back. Loud (this is the whole point of the ratchet), survival-themed.
    seed = int((vault_total + actual_amount) * 100)
    audience = (
        f"🏛️ <b>SECURED — ${usdce_received:.2f} BANKED TO THE VAULT</b>\n"
        f"vault now holds <b>${vault_total:.2f}</b>  ·  this can never be re-risked\n"
        f"<i>{_pick_line(_SWEEP_SECURED_LINES, seed)}</i>"
    )
    await _send_telegram(http_client, audience, silent=False)

    # OPS: the full forensic record.
    adjusted_line = (
        f"\n⚠️ <i>Adjusted down from ${intended_amount:.2f} (balance dropped during timelock)</i>"
        if adjusted else ""
    )
    ops_text = (
        "🏦 <b>VAULT SWEEP COMPLETED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💸 <b>Swept:</b> ${actual_amount:.2f} pUSD → ${usdce_received:.2f} USDC.e{adjusted_line}\n"
        f"💰 <b>Trading wallet remaining:</b> ${remaining:.2f}\n"
        f"🏛️ <b>Vault total received:</b> ${vault_total:.2f} USDC.e\n\n"
        f'🔍 <a href="https://polygonscan.com/tx/{unwrap_tx}">View unwrap transaction</a>\n'
        f'🔍 <a href="https://polygonscan.com/address/{VAULT_WALLET_ADDRESS}">View vault</a>\n\n'
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await _send_telegram(http_client, ops_text, ops=True)


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
    """Format the recent-loss list into Telegram HTML lines (market/side escaped —
    a literal '<' in a question like "Will Elon post <40 tweets…" 400s the whole
    parse_mode=HTML message and silently drops the circuit-breaker alert)."""
    import html as _html
    if not recent_losses:
        return "  (no resolved trades available)"
    lines = []
    for t in recent_losses:
        q     = _html.escape((t.get("market_question") or "Unknown")[:60])
        side  = _html.escape(str(t.get("bet_side") or "?"))
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


def _build_timeout_text(reason_line: str, resume_ts: int, seed: int) -> str:
    """Pure audience 'bot benched itself' card — the rails firing IS part of the show,
    told honestly and short. Full forensic detail goes to ops separately."""
    resume_str = time.strftime("%H:%M UTC", time.gmtime(resume_ts))
    return (
        f"🧯 <b>BOT IN TIMEOUT</b>\n"
        f"{reason_line}\n"
        f"⏳ benched {TRADING_PAUSE_DURATION_SECONDS // 60} min — back at {resume_str}\n"
        f"<i>{_pick_line(_TIMEOUT_LINES, seed)}</i>"
    )


async def _notify_loss_streak_pause(
    http_client: httpx.AsyncClient,
    consecutive: int,
    balance: float,
    resume_ts: int,
    recent_losses: list,
) -> None:
    # Audience: short, loud, honest. Ops: the full forensic version.
    text = _build_timeout_text(
        f"dropped <b>{consecutive} straight</b> — the rails stepped in",
        resume_ts, consecutive,
    )
    await _send_telegram(http_client, text)

    loss_lines = _format_loss_lines(recent_losses)
    bal_str = f"${balance:.2f} USDC" if balance >= 0 else "unknown"
    ops_text = (
        f"🚨 <b>CIRCUIT BREAKER — TRADING PAUSED</b>\n\n"
        f"<b>{consecutive} consecutive losses detected.</b>\n"
        f"⏸ Paused for {TRADING_PAUSE_DURATION_SECONDS // 60} min\n"
        f"💵 Bankroll: <b>{bal_str}</b>\n\n"
        f"<b>Recent losses:</b>\n{loss_lines}\n\n"
        "Investigate if unexpected. Bot resumes automatically."
    )
    await _send_telegram(http_client, ops_text, ops=True)


async def _notify_cb_drawdown_pause(
    http_client: httpx.AsyncClient,
    rolling_loss: float,
    threshold_usdc: float,
    bankroll: float,
    resume_ts: int,
    recent_losses: list,
) -> None:
    pct = (rolling_loss / max(bankroll, 1.0)) * 100
    # Audience: short, loud, honest. Ops: the full forensic version.
    text = _build_timeout_text(
        f"down <b>${rolling_loss:.2f}</b> in {TRADING_CB_WINDOW_HOURS:.0f}h "
        f"(that's {pct:.0f}% of the wallet, chief)",
        resume_ts, int(rolling_loss * 100),
    )
    await _send_telegram(http_client, text)

    loss_lines = _format_loss_lines(recent_losses)
    ops_text = (
        f"🚨 <b>CIRCUIT BREAKER — TRADING PAUSED</b>\n\n"
        f"Rolling {TRADING_CB_WINDOW_HOURS:.0f}h loss: <b>${rolling_loss:.2f}</b> "
        f"({pct:.1f}% of bankroll, threshold {TRADING_CB_DRAWDOWN_PCT * 100:.0f}%)\n"
        f"⏸ Paused for {TRADING_PAUSE_DURATION_SECONDS // 60} min\n"
        f"💵 Bankroll: <b>${bankroll:.2f} USDC</b>\n\n"
        f"<b>Recent losses:</b>\n{loss_lines}\n\n"
        "Investigate if unexpected. Bot resumes automatically."
    )
    await _send_telegram(http_client, ops_text, ops=True)


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
#
# 2026-06-12 (feed v2) — retuned again, upward. In v2 these banners GATE notifications
# (loud settle ⇔ banner/snap/streak), and at $3 a banner fired on most coin-flip wins and
# every near-max-stake loss — every other settle buzzed, so none of them meant anything.
# Envelope under the $5 cap + 0.50 floor: win pnl ≤ +$5.00, loss pnl ≥ −$5.00. BIG WIN now
# = the juiciest coin-flip wins (≤ ~53¢ at full stake), BRUTAL = a near-max-stake loss.
_TICKER_LONGSHOT_MAX: float = float(os.getenv("TICKER_LONGSHOT_MAX_PRICE", "0.20"))
_TICKER_BIG_WIN_USDC: float = float(os.getenv("TICKER_BIG_WIN_USDC", "4.5"))
_TICKER_UPSET_FAV_PRICE: float = float(os.getenv("TICKER_UPSET_FAV_PRICE", "0.80"))
_TICKER_BRUTAL_LOSS_USDC: float = float(os.getenv("TICKER_BRUTAL_LOSS_USDC", "4.75"))


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
            return (f"{tier} — +${pnl:.2f}", "that's the dinner bill. 🦞")
    elif status == "lost":
        if fill_price >= _TICKER_UPSET_FAV_PRICE:
            return (f"💀 <b>UPSET — backed the {fill_price * 100:.0f}% fav, got cooked</b>",
                    "that's the one that keeps you humble. 😮‍💨")
        if pnl <= -_TICKER_BRUTAL_LOSS_USDC:
            return (f"💸 <b>BRUTAL BEAT — −${abs(pnl):.2f}</b>", "oof. pour one out. 🫗")
    return None


def _build_settle_text(
    trade: dict,
    resolution_status: str,
    pnl: float,
    streak: int = 0,
    snap: str = "",
    *,
    threaded: bool = False,
    balance: Optional[float] = None,
    vault_slice: float = 0.0,
    vault_secured: float = -1.0,
) -> tuple[str, bool]:
    """Pure settle card (feed v2). Returns (html_text, loud).

    threaded=True → this message will post as a REPLY to its betslip, so the quoted
    slip already shows the market + side + price; the card then leads with the result
    and skips the title. threaded=False (slip unknown, e.g. after a restart) → the
    card stays self-contained and includes the line it settled.

    loud: only genuinely notable settles buzz phones — a big-moment banner, a ≥3-round
    streak, or a streak snap. Routine wins/losses post silent (notification discipline)."""
    import html as _html

    market_q = trade.get("market_question") or trade.get("market_id", "")
    bet_side = trade.get("bet_side", "")
    fill_price = trade.get("bet_price_filled") or trade.get("bet_price_intended") or 0.0
    winning_outcome = trade.get("winning_outcome")
    pnl = pnl or 0.0

    headline, banter = _streak_headline(resolution_status, streak)
    moment = _big_moment(resolution_status, fill_price, pnl)
    # Cap the header at ONE prepended banner so the result still leads the phone's
    # notification preview. A rare big-moment (UNICORN / BIG WIN / UPSET) outranks a snap.
    if moment:
        headline = f"{moment[0]}\n{headline}"
        banter = moment[1]
    elif snap:
        headline = f"{snap}\n{headline}"
    elif abs(streak) < 2:
        # Ordinary (non-streak) settle → rotate the house lines so the feed doesn't
        # repeat itself. Streak settles (|n|>=2) keep _streak_headline's curated
        # escalation banter — overwriting it here would make those lines dead copy.
        seed = abs(int(pnl * 100)) + int((fill_price or 0) * 100)
        if resolution_status == "won":
            banter = _pick_line(_HIT_LINES, seed)
        elif resolution_status == "lost":
            banter = _pick_line(_MISS_LINES, seed)

    # A VOID/refund settle doesn't buzz for a stale running streak — a voids-only
    # grading batch leaves _result_streak untouched, so the streak isn't this card's
    # news. (A big-moment or snap banner still buzzes whatever card carries it.)
    loud = bool(moment) or bool(snap) or \
        (abs(streak) >= 3 and resolution_status in ("won", "lost"))

    price_c = f"{fill_price*100:.0f}¢" if fill_price and fill_price > 0 else "?¢"
    safe_side = _html.escape(str(bet_side))
    if resolution_status == "won":
        mult = f" (x{1.0/fill_price:.1f})" if fill_price and fill_price > 0 else ""
        money_line = f"💰 <b>+${pnl:.2f}</b> — {safe_side} cashed at {price_c}{mult}"
    elif resolution_status == "lost":
        money_line = f"💸 <b>−${abs(pnl):.2f}</b> — {safe_side} at {price_c} didn't land"
    else:
        money_line = "↩️ <b>Refunded</b> — push, no harm"

    lines = [headline, ""]
    if not threaded:
        lines.append(f"📋 {_html.escape(str(market_q)[:120])}")
    lines.append(money_line)
    # On a loss, name the side that actually won (only when it adds information).
    if resolution_status == "lost" and winning_outcome:
        lines.append(f"🏆 {_html.escape(str(winning_outcome))} took it")
    # Win-ratchet: earmark a slice toward the vault. vault_secured is the REAL on-chain vault
    # balance (the honest "actually banked" number); the slice is set aside, not yet moved.
    if vault_slice and vault_slice > 0:
        seed = int((vault_secured + vault_slice) * 100)
        secured_bit = f"  ·  vault holds <b>${vault_secured:.2f}</b>" if vault_secured >= 0 else ""
        lines.append(f"🏦 <b>+${vault_slice:.2f}</b> set aside{secured_bit}  "
                     f"<i>{_pick_line(_RATCHET_LINES, seed)}</i>")
    elif balance is not None and balance >= 0:
        lines.append(f"🏦 bank ${balance:.2f}")
    lines.append(f"<i>{banter}</i>")
    return "\n".join(lines), loud


async def _notify_trade_resolution(
    http_client: httpx.AsyncClient,
    trade: dict,
    resolution_status: str,
    pnl: float,
    streak: int = 0,
    snap: str = "",
    balance: Optional[float] = None,
    vault_slice: float = 0.0,
    vault_secured: float = -1.0,
) -> None:
    # AUDIENCE settle — feed v2: threads as a reply under its betslip (one bet = one
    # story), buzzes only when notable, shows the bank where the money changed.
    # balance is fetched ONCE per grading batch by the caller (one RPC, not one per
    # settle inside the trading loop); None just omits the bank line. vault_slice>0 earmarks
    # a slice; vault_secured is the REAL on-chain vault balance (honest "actually banked").
    slip_id = _slip_msgs.get(str(trade.get("alert_id") or ""))
    text, loud = _build_settle_text(
        trade, resolution_status, pnl, streak, snap,
        threaded=bool(slip_id), balance=balance,
        vault_slice=vault_slice, vault_secured=vault_secured,
    )
    await _send_telegram(http_client, text, silent=not loud, reply_to=slip_id)


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

    global _session_avg_bet, _cached_usdc_balance, _brain_sizeups_today, _brain_picks_today, _brain_pick_day

    alert_id    = alert["alert_id"]
    market_id   = alert["market_id"]
    market_q    = alert.get("market_question") or market_id
    bet_side    = alert["bet_side"]
    price_alert = float(alert["bet_price_at_alert"])
    score       = int(alert["score"])
    token_id    = alert.get("clob_token_id")
    # Brain Pick: the brain's OWN research-driven trade (not an insider copy). It bypasses the
    # insider-specific gates (soccer filter, real-time vet) and trades at a fixed discovery stake.
    _is_brain_pick = str(alert_id).startswith("brain_")
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

    _brain_verdict = None
    _brain_conf = None
    if _is_brain_pick:
        # The brain's OWN pick: gate, per-day cap, fixed discovery stake. No vet/size-up/veto —
        # this IS the brain's researched conviction; vetting it would be circular.
        _bp_day = _utc_day()
        if _bp_day != _brain_pick_day:
            _brain_pick_day = _bp_day
            _brain_picks_today = 0
        if not BRAIN_PICK_TRADING_ENABLED:
            log.info("[Brain] pick %s — brain-pick trading disabled; recording skip", alert_id[:20])
            await _record_brain_pick_skip(http_client, alert, "brain-pick trading disabled")
            return
        if _brain_picks_today >= BRAIN_PICK_MAX_PER_DAY:
            log.info("[Brain] pick %s — daily pick cap reached (%d)", alert_id[:20], BRAIN_PICK_MAX_PER_DAY)
            await _record_brain_pick_skip(http_client, alert, "daily brain-pick cap reached")
            return
        # Conviction-scaled stake from the pick's researched edge (stored in the breakdown).
        try:
            _pick_edge = float(json.loads(alert.get("score_breakdown_json") or "{}").get("edge") or 0.0)
        except Exception:
            _pick_edge = 0.0
        bet_size = _brain_pick_stake(_pick_edge)
        log.info("[Brain] PICK trade: %.40s buy %s @ %.2f edge=%+.2f size $%.2f",
                 market_q, bet_side, price_alert, _pick_edge, bet_size)
    else:
        bet_size = await _calculate_bet_size(http_client, stats, score=score)

        # Brain conviction size-up: the brain vets this alert IN REAL TIME (synchronous ~20-40s
        # call) so it can actually weigh in before the position is taken; falls back to the polled
        # confirmation cache if real-time vetting is off/unavailable. On a high-conviction CONFIRM
        # the bet is bumped toward the cap, clamped to free cash above the sweep reserve so the
        # reserve gate below can't reject it. Every downstream rail still runs on the bigger size.
        _avail_for_sizeup = (_cached_usdc_balance - _sweep_reserve_usdc()) if _cached_usdc_balance >= 0 else -1.0
        _brain_verdict = await _brain_vet_realtime(http_client, alert)
        if _brain_verdict is None:
            _brain_verdict = _brain_confirmations.get((market_id, bet_side))  # cache fallback

        # Brain VETO: if it strongly disagrees, the brain overrules the signal — skip the bet entirely
        # (operator-chosen). The verdict is already logged Railway-side for grading; we just don't trade.
        if _brain_skip_veto(_brain_verdict):
            log.info("[Brain] VETO skip: %s %s/%s — brain %.0f%% vs mkt %.0f%% conf=%.2f edge=%+.2f",
                     alert_id[:12], market_id[:16], bet_side,
                     (_brain_verdict.get("brain_prob") or 0.0) * 100,
                     (_brain_verdict.get("market_price") or 0.0) * 100,
                     _brain_verdict.get("confidence") or 0.0, _brain_verdict.get("edge") or 0.0)
            await _notify_brain_veto_skip(http_client, alert, _brain_verdict)
            return

        bet_size, _brain_conf = _apply_brain_sizeup(bet_size, _brain_verdict, _avail_for_sizeup)
        if _brain_conf:
            log.info("[Brain] conviction size-up: %s %s/%s → $%.2f (conf %.2f edge %+.2f)",
                     alert_id[:12], market_id[:16], bet_side, bet_size,
                     _brain_conf["confidence"], _brain_conf["edge"])

    # Reserve gate: keep the operating floor (and any in-flight sweep) unbet so the vault
    # sweep's cash survives the 1h timelock instead of being re-bet and self-cancelling.
    # _cached_usdc_balance is kept fresh (decremented on each fill, refreshed each redeem
    # cycle). When the free float minus the reserve can't cover this bet, skip it.
    if _cached_usdc_balance >= 0:
        _avail = _cached_usdc_balance - _sweep_reserve_usdc()
        if _avail < bet_size:
            _sweeping = _sweep_state in ("pause_pending", "pause_ready")
            _reason = ("holding cash for the in-flight vault sweep" if _sweeping
                       else "bankroll below the operating floor")
            log.info("[Trade] Skipping %s — %s (free $%.2f, reserve $%.2f, avail $%.2f < bet $%.2f)",
                     alert_id[:12], _reason, _cached_usdc_balance, _sweep_reserve_usdc(),
                     _avail, bet_size)
            await _notify_skip(http_client, alert, _reason)
            return

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

    # Gate-orientation regression guard (2026-06-13). The pre-fix token[0] bug fed the
    # OPPOSITE token's price into the slippage gate, so price_current ≈ 1 - price_alert and
    # the gate spuriously rejected the best flow while everything looked healthy. The fix
    # (correct clob_token_id) cured it, but a regression would be silent — so warn loudly
    # if the inversion signature reappears: current ≈ 1-alert AND current is far from alert.
    if (current_price is not None and slippage is not None and slippage > 0.10
            and abs(current_price - (1.0 - price_alert)) < 0.03):
        log.warning(
            "[GateGuard] PRICE-INVERSION SIGNATURE for alert %s: current=%.3f ≈ 1-alert=%.3f "
            "(alert=%.3f). Possible token-side regression — gate price feed may be inverted.",
            alert_id[:12], current_price, 1.0 - price_alert, price_alert,
        )

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
            _session_avg_bet = 0.9 * _session_avg_bet + 0.1 * filled_size
            # Keep the cached free balance fresh between 10-min refreshes so the reserve
            # gate sees cash leave in real time (corrected on the next redeem-cycle read).
            if _cached_usdc_balance >= 0:
                _cached_usdc_balance = max(0.0, _cached_usdc_balance - filled_size)
        # Track held position for per-market/side dedup + opposite-side vig gate.
        if market_id and bet_side:
            _held_positions.add((market_id, bet_side))
            _entry_px = fill_price or current_price or price_alert
            if _entry_px:
                _held_side_px.setdefault(market_id, {})[bet_side] = float(_entry_px)
            log.debug("[Dedup] Added (%s, %s) to _held_positions", market_id[:16], bet_side)
        # Running slip number for the feed: lifetime filled bets (settled + open) + this
        # one. Presentation-only; omitted gracefully if the stats payload lacks the keys.
        try:
            bet_no = int(stats.get("resolved", 0)) + int(stats.get("open_positions", 0)) + 1
            if bet_no <= 0:
                bet_no = None
        except Exception:
            bet_no = None
        # Brain: count the sized-up bet (bounds the per-day cap) and carry the brain's read
        # onto the betslip. For a Brain Pick, count it + surface its own research edge.
        if _is_brain_pick:
            _brain_picks_today += 1
        elif _brain_conf:
            _brain_sizeups_today += 1
        _slip_verdict = _brain_pick_slip_info(alert) if _is_brain_pick else _brain_verdict
        await _notify_trade_filled(
            http_client, market_q, bet_side,
            fill_price or current_price or price_alert,
            filled_size, score, slippage, market_url,
            alert_created_at=int(alert.get("created_at") or 0),
            score_breakdown_json=alert.get("score_breakdown_json"),
            alert_id=alert_id,
            bet_no=bet_no,
            brain_verdict=_slip_verdict,
            brain_sized_up=bool(_brain_conf),
            is_brain_pick=_is_brain_pick,
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

def _opposite_side_vig(new_px, held_sides: dict, new_side: str,
                       cap: float = TRADING_OPPOSITE_SIDE_MAX_SUM):
    """Pure decision for the opposite-side vig gate. Given the price we'd pay for new_side
    and {side: entry_px} we already hold in this market, return (skip, worst_sum) where
    skip=True means taking new_side would lock in the insiders' overround (the tightest
    pair sums to > cap, i.e. > $1-ish for a guaranteed $1). Same-side or no-data → no skip
    (the same-side case is handled by the dedup set; this gate is only for NEW sides)."""
    if not held_sides or not new_px or new_side in held_sides:
        return False, 0.0
    others = [p for s, p in held_sides.items() if s != new_side and p]
    if not others:
        return False, 0.0
    worst = max(others)  # the held side that makes the tightest (most -EV) pair
    return (new_px + worst) > cap, new_px + worst


# ---------------------------------------------------------------------------
# Brain conviction size-up
# ---------------------------------------------------------------------------

def _utc_day() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _apply_brain_sizeup(base_size: float, info: Optional[dict],
                        avail: float) -> tuple[float, Optional[dict]]:
    """Pure decision. Given the brain's verdict for THIS bet (a real-time vet result or a
    cached confirm), size up toward BRAIN_SIZEUP_MAX_USDC when it's a high-conviction CONFIRM
    (and the per-day cap isn't spent) — never below base_size, never above the cash available
    above the sweep reserve (so the reserve gate that follows can't reject the trade), never
    above the cap. Otherwise base_size unchanged. Second element is the verdict dict (carrying
    the brain's take) when a size-up applies, else None. No side effects."""
    if not BRAIN_SIZEUP_ENABLED or not info:
        return base_size, None
    if info.get("verdict") != "CONFIRM":
        return base_size, None
    if info.get("confidence", 0.0) < BRAIN_SIZEUP_MIN_CONFIDENCE:
        return base_size, None
    if abs(info.get("edge", 0.0)) < BRAIN_SIZEUP_MIN_EDGE:
        return base_size, None
    if _brain_sizeups_today >= BRAIN_SIZEUP_MAX_PER_DAY:
        return base_size, None
    ceiling = BRAIN_SIZEUP_MAX_USDC
    if avail >= 0:
        ceiling = min(ceiling, avail)
    target = max(base_size, ceiling)
    if target <= base_size + 1e-9:   # no headroom to actually size up
        return base_size, None
    return round(target, 2), info


def _brain_skip_veto(info: Optional[dict]) -> bool:
    """Pure decision. True when the brain's verdict is a strong-enough VETO to SKIP the bet
    entirely: a VETO whose confidence clears BRAIN_VETO_MIN_CONFIDENCE and whose edge is at
    least BRAIN_VETO_MIN_EDGE against the bet (brain_prob that far BELOW the market price).
    No side effects."""
    if not BRAIN_VETO_SKIP_ENABLED or not info:
        return False
    if info.get("verdict") != "VETO":
        return False
    if info.get("confidence", 0.0) < BRAIN_VETO_MIN_CONFIDENCE:
        return False
    # edge = brain_prob − market_price; a real veto is strongly negative (the side is overpriced).
    if info.get("edge", 0.0) > -BRAIN_VETO_MIN_EDGE:
        return False
    return True


async def _notify_brain_veto_skip(http_client: httpx.AsyncClient, alert: dict, verdict: dict) -> None:
    """AUDIENCE card (V1 Poly): the brain overruled the signal and sat a bet out. Rare by design
    (only strong vetoes), so it reads as a moment, not noise."""
    import html as _html
    q = _html.escape(str(alert.get("market_question") or "")[:90])
    side = _html.escape(str(alert.get("bet_side") or ""))
    take = _html.escape(str(verdict.get("take") or "")[:220])
    try:
        reads = (f"the brain reads it <b>{float(verdict['brain_prob'])*100:.0f}%</b> vs the "
                 f"market's <b>{float(verdict['market_price'])*100:.0f}%</b>")
    except Exception:
        reads = "the brain strongly disagrees"
    text = (
        f"🧠 <b>BRAIN OVERRULE — sat this one out</b>\n\n"
        f"<b>{side}</b> — {q}\n\n"
        f"The signal flagged it, but {reads} — so the brain vetoed the bet.\n"
        + (f"<i>{take}</i>\n" if take else "")
        + "<i>the brain's call · no position taken.</i>"
    )
    await _send_telegram(http_client, text, silent=True)


def _brain_pick_stake(edge: float, base: float = None, slope: float = None,
                      cap: float = None) -> float:
    """Pure conviction-scaled stake for a Brain Pick: base + slope × (edge − 0.08), clamped to
    [base, cap]. A bigger researched edge earns a bigger (still capped) position."""
    if base is None:
        base = BRAIN_PICK_SIZE_USDC
    if slope is None:
        slope = BRAIN_PICK_EDGE_SLOPE
    if cap is None:
        cap = BRAIN_PICK_MAX_SIZE_USDC
    try:
        e = max(0.0, float(edge) - 0.08)
    except (TypeError, ValueError):
        e = 0.0
    return round(max(base, min(cap, base + slope * e)), 2)


def _brain_pick_slip_info(alert: dict) -> Optional[dict]:
    """Build the betslip 'verdict' for a Brain Pick from its stored research (score_breakdown_json):
    the brain's prob on the side it bought, the entry price, the edge, and the take."""
    try:
        b = json.loads(alert.get("score_breakdown_json") or "{}")
    except Exception:
        b = {}
    return {
        "verdict": "PICK",
        "take": b.get("take") or "",
        "brain_prob": float(b.get("brain_prob") or 0.0),
        "market_price": float(alert.get("bet_price_at_alert") or 0.0),
        "edge": float(b.get("edge") or 0.0),
    }


async def _record_brain_pick_skip(http_client: httpx.AsyncClient, alert: dict, reason: str) -> None:
    """Record a 'skipped' trade_execution for a Brain Pick we won't take (gate off / daily cap),
    so it drops out of the tradeable feed instead of re-appearing every cycle. No audience noise."""
    try:
        await _api_post(http_client, "/api/trades", {
            "alert_id": alert.get("alert_id", ""),
            "market_id": alert.get("market_id", ""),
            "market_question": alert.get("market_question") or "",
            "clob_token_id": alert.get("clob_token_id") or "UNKNOWN",
            "bet_side": alert.get("bet_side", ""),
            "bet_price_intended": float(alert.get("bet_price_at_alert") or 0.0),
            "size_usdc": 0.0, "status": "skipped", "error_message": reason,
        })
    except Exception as exc:
        log.debug("[Brain] pick skip-record failed: %s", exc)


async def _brain_vet_realtime(http_client: httpx.AsyncClient, alert: dict) -> Optional[dict]:
    """Ask the brain to vet this alert AT TRADE TIME (synchronous ~20-40s call). Returns the
    verdict dict {verdict, confidence, edge, brain_prob, market_price, take} or None (disabled,
    timeout, error, or the brain is over budget). The base trade proceeds regardless — this only
    governs the conviction size-up."""
    if not BRAIN_REALTIME_VET_ENABLED:
        return None
    body = {
        "alert_id": alert.get("alert_id", ""),
        "market_id": alert.get("market_id", ""),
        "market_question": alert.get("market_question") or "",
        "bet_side": alert.get("bet_side", ""),
        "bet_price_at_alert": alert.get("bet_price_at_alert"),
        "market_category": alert.get("market_category") or "",
        "hours_to_close_at_alert": alert.get("hours_to_close_at_alert"),
    }
    try:
        resp = await http_client.post(
            f"{RAILWAY_API_URL}/api/brain/vet", headers=_headers(), json=body,
            timeout=BRAIN_VET_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.info("[Brain] real-time vet unavailable [%s: %r] — base size", type(exc).__name__, exc)
        return None
    if not isinstance(data, dict) or not data.get("ok"):
        reason = data.get("reason", "no verdict") if isinstance(data, dict) else "bad response"
        log.info("[Brain] real-time vet returned no verdict (%s) — base size", reason)
        return None
    log.info("[Brain] real-time vet: %s conf=%.2f edge=%+.2f for %s",
             data.get("verdict"), float(data.get("confidence") or 0.0),
             float(data.get("edge") or 0.0), (alert.get("alert_id") or "")[:12])
    return {
        "verdict": data.get("verdict"),
        "confidence": float(data.get("confidence") or 0.0),
        "edge": float(data.get("edge") or 0.0),
        "brain_prob": float(data.get("brain_prob") or 0.0),
        "market_price": float(data.get("market_price") or 0.0),
        "take": (data.get("take") or "")[:300],
    }


async def _poll_brain_confirmations(http_client: httpx.AsyncClient) -> None:
    """Refresh the brain confirmation cache from the Railway API. Best-effort: on any
    error the previous cache is kept (a missing endpoint just means no size-ups). Rolls
    the per-day sized-up-bet counter at UTC midnight."""
    global _brain_confirmations, _brain_sizeups_today, _brain_sizeup_day
    today = _utc_day()
    if today != _brain_sizeup_day:
        _brain_sizeup_day = today
        _brain_sizeups_today = 0
    if not BRAIN_SIZEUP_ENABLED:
        return
    since = int(time.time() - BRAIN_CONFIRM_LOOKBACK_HOURS * 3600)
    rows = await _api_get(http_client, "/api/brain/confirmations",
                          params={"since": since, "min_confidence": BRAIN_SIZEUP_MIN_CONFIDENCE})
    if not isinstance(rows, list):
        return
    cache: dict[tuple[str, str], dict] = {}
    for r in rows:
        mid = r.get("market_id")
        side = r.get("target_label")
        if not mid or not side:
            continue
        cache[(mid, side)] = {
            "verdict": "CONFIRM",   # the endpoint only returns CONFIRM+act rows
            "confidence": float(r.get("confidence") or 0.0),
            "edge": float(r.get("edge") or 0.0),
            "brain_prob": float(r.get("brain_prob") or 0.0),
            "market_price": float(r.get("market_price") or 0.0),
            "take": (r.get("take") or "")[:300],
        }
    _brain_confirmations = cache
    log.info("[Brain] confirmation cache refreshed: %d high-conviction CONFIRM(s)", len(cache))


async def _seed_held_positions(http_client: httpx.AsyncClient) -> None:
    """
    Seed _held_positions from /api/positions/open with retry + backoff.
    Must complete before the poll loop processes any alert.
    On persistent failure, dedup stays inactive (_held_positions_seeded=False)
    and a recurring WARNING is emitted each cycle until a restart recovers it.
    """
    global _held_positions, _held_side_px, _held_positions_seeded
    for attempt in range(5):
        try:
            positions = await _api_get(http_client, "/api/positions/open")
            if isinstance(positions, list):
                _held_positions = {
                    (p["market_id"], p["bet_side"])
                    for p in positions
                    if p.get("market_id") and p.get("bet_side")
                }
                _held_side_px = {}
                for p in positions:
                    mid, side = p.get("market_id"), p.get("bet_side")
                    px = p.get("bet_price_filled") or p.get("bet_price_intended")
                    if mid and side and px:
                        _held_side_px.setdefault(mid, {})[side] = float(px)
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

    # One balance RPC per grading batch (not per settle — this loop runs inline in
    # the trading loop, and per-settle eth_calls would stack RPC latency between
    # resolution PATCHes). Wins don't move free USDC until redemption anyway.
    try:
        batch_balance = await _get_usdc_balance()
    except Exception:
        batch_balance = None
    # Real on-chain vault balance for the settle cards' honest "secured" number (cached,
    # refreshed at most every 10 min — never the in-process tab).
    batch_vault = await _refresh_vault_balance()

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

        # Win-ratchet: bank a slice of this win's profit toward the vault lifeline.
        vault_slice = _accrue_vault_tab(pnl) if VAULT_RATCHET_ENABLED else 0.0

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
            _sides = _held_side_px.get(_mid)
            if _sides is not None:
                _sides.pop(_bside, None)
                if not _sides:
                    _held_side_px.pop(_mid, None)
            log.debug("[Dedup] Cleared (%s, %s) after resolution", _mid[:16], _bside)

        # Streak is the cycle's round value; the snap banner rides only the first post.
        await _notify_trade_resolution(http_client, trade, resolution_status, pnl,
                                       streak=_result_streak, snap=snap if first else "",
                                       balance=batch_balance,
                                       vault_slice=vault_slice, vault_secured=batch_vault)
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
    global _wallet_address, _last_resolution_check, _last_redemption_check, _last_positions_summary, _sweep_state, _sweep_paused_at, _sweep_intended_amount, _sweep_last_date, _current_max_positions, _legacy_max_positions_ceiling, _current_tier, _cb_pnl_history, _cached_usdc_balance, _session_avg_bet, _held_positions, _held_positions_seeded, _last_brain_confirm_poll

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
    log.info("Vault:       %s  sweep=%s  ratchet=%s (%.0f%% of wins, settle>=$%.0f, floor=$%.0f)",
             VAULT_WALLET_ADDRESS or "disabled",
             "ARMED" if VAULT_SWEEP_ENABLED else "dry-run",
             "on" if VAULT_RATCHET_ENABLED else "off",
             SWEEP_WIN_PCT * 100, VAULT_RATCHET_MIN_SETTLE_USDC, VAULT_RATCHET_FLOOR_USDC)
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
            await _refresh_vault_balance(force=True)  # honest "secured" number ready for the feed
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

                # Brain confirmation poll — refresh the conviction cache that drives the
                # capped size-up. No-op when BRAIN_SIZEUP_ENABLED is off.
                if now - _last_brain_confirm_poll >= BRAIN_CONFIRM_POLL_SECONDS:
                    await _poll_brain_confirmations(http_client)
                    _last_brain_confirm_poll = now

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

                # Live dashboard: refresh the pinned audience card (rate-limited internally).
                await _update_dashboard(http_client, stats)

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
                        # Opposite-side vig gate: we already hold a DIFFERENT side of this
                        # market. Taking this side too locks the pair; only do it if the two
                        # entry prices sum to <= the cap (a real cheap hedge / arb). Otherwise
                        # it's the insiders' overround as a guaranteed loss — skip it.
                        _vig_skip, _vig_sum = _opposite_side_vig(
                            alert.get("bet_price_at_alert"),
                            _held_side_px.get(_dmid) or {}, _dside)
                        if _vig_skip:
                            log.info(
                                "[Dedup] Skipping %s — opposite-side vig: %s would lock "
                                "a pair summing to %.2f > %.2f",
                                (alert.get("alert_id") or "")[:12], _dside, _vig_sum,
                                TRADING_OPPOSITE_SIDE_MAX_SUM,
                            )
                            await _notify_skip(http_client, alert, "opposite-side vig")
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
