"""
config.py — All tunable parameters and environment variable loading.

Every magic number lives here. Nothing is hardcoded elsewhere.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# Feed v2 (2026-06-12): TELEGRAM_CHAT_ID is the AUDIENCE feed — the friends' channel.
# Research/ops output (the per-signal INSIDER SIGNAL walls, the Daily Intelligence
# Brief + CSV, and the terminal-style TRADE RESOLVED duplicates) routes to
# TELEGRAM_OPS_CHAT_ID when set. When unset, that output is DROPPED from Telegram
# (it remains in logs + DB + the API) so the audience channel stays clean — set
# FEED_RESEARCH_TO_MAIN=true to restore the old everything-in-one-channel behavior.
# Mirrors the fly trader's TELEGRAM_OPS_CHAT_ID / FEED_OPS_TO_MAIN contract.
TELEGRAM_OPS_CHAT_ID: str = os.getenv("TELEGRAM_OPS_CHAT_ID", "")
FEED_RESEARCH_TO_MAIN: bool = os.getenv("FEED_RESEARCH_TO_MAIN", "false").strip().lower() in ("true", "1", "yes")

# Polymarket enforces no more than 1 message per 1.5 seconds per chat to
# avoid Telegram rate-limit (429) errors.
TELEGRAM_RATE_LIMIT_SECONDS: float = float(os.getenv("TELEGRAM_RATE_LIMIT_SECONDS", "1.5"))

# ---------------------------------------------------------------------------
# Polymarket WebSocket feed
# ---------------------------------------------------------------------------

WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Polymarket requires a PING every 10 seconds or the server closes the socket.
WS_PING_INTERVAL_SECONDS: int = 10

# Hard limit from Polymarket: max 5 simultaneous WebSocket connections.
WS_MAX_CONNECTIONS: int = 5

# Exponential backoff: initial wait before reconnect attempt (seconds).
WS_RECONNECT_BASE_SECONDS: float = 2.0

# Cap on reconnect backoff to avoid waiting forever.
WS_RECONNECT_MAX_SECONDS: float = 120.0

# ---------------------------------------------------------------------------
# Trade filtering — minimum USD notional to consider a trade "significant"
# ---------------------------------------------------------------------------

# Trades below this threshold are ignored entirely.
TRADE_MIN_SIZE_USD: float = float(os.getenv("TRADE_MIN_SIZE_USD", "500"))

# ---------------------------------------------------------------------------
# REST fallback polling (Data API)
# ---------------------------------------------------------------------------

DATA_API_BASE: str = "https://data-api.polymarket.com"

# Seconds between REST trade polls when WebSocket is down.
TRADE_POLL_INTERVAL_SECONDS: int = int(os.getenv("TRADE_POLL_INTERVAL_SECONDS", "30"))

# Max trades per REST request (API cap).
TRADE_POLL_LIMIT: int = 50

# Cash-filter: only return trades with USD value >= this.
TRADE_FILTER_AMOUNT: float = TRADE_MIN_SIZE_USD

# ---------------------------------------------------------------------------
# Market discovery (Gamma API)
# ---------------------------------------------------------------------------

GAMMA_API_BASE: str = "https://gamma-api.polymarket.com"

# How often to poll Gamma for new/updated markets.
# 1800s (30 min) — markets change slowly; 5 min was downloading ~30 MB/cycle × 288 cycles/day = 8+ GB/day.
MARKET_DISCOVERY_INTERVAL_SECONDS: int = int(os.getenv("MARKET_DISCOVERY_INTERVAL_SECONDS", "1800"))  # 30 minutes

# How often the resolution-checker polls Gamma for outcome updates.
RESOLUTION_CHECK_INTERVAL_SECONDS: int = int(os.getenv("RESOLUTION_CHECK_INTERVAL_SECONDS", "3600"))  # 1 hour

# Maximum markets to fetch per Gamma poll (offset pagination).
GAMMA_MARKETS_LIMIT: int = int(os.getenv("GAMMA_MARKETS_LIMIT", "200"))

# ---------------------------------------------------------------------------
# Wallet profiling (Data API + Etherscan V2)
# ---------------------------------------------------------------------------

# SQLite cache TTL for wallet profiles — don't re-fetch within this window.
WALLET_CACHE_TTL_SECONDS: int = int(os.getenv("WALLET_CACHE_TTL_SECONDS", str(2 * 3600)))  # 2 hours

# Max trades to pull per wallet for history analysis.
WALLET_TRADE_HISTORY_LIMIT: int = int(os.getenv("WALLET_TRADE_HISTORY_LIMIT", "10000"))

# ---------------------------------------------------------------------------
# Etherscan V2 (Polygon chain)
# ---------------------------------------------------------------------------

ETHERSCAN_API_KEY: str = os.getenv("ETHERSCAN_API_KEY", "")

# Etherscan V2 unified endpoint — chainid=137 targets Polygon PoS.
ETHERSCAN_BASE_URL: str = "https://api.etherscan.io/v2/api"
ETHERSCAN_CHAIN_ID: int = 137  # Polygon PoS

# Rate limit: 3 calls per second, 100K calls per day (free tier).
ETHERSCAN_RATE_LIMIT_CALLS_PER_SEC: float = 3.0

# ---------------------------------------------------------------------------
# Alchemy (used ONLY for alchemy_getAssetTransfers — cluster detection)
# ---------------------------------------------------------------------------

ALCHEMY_RPC_URL: str = os.getenv("ALCHEMY_RPC_URL", "")  # Full URL including API key

# ---------------------------------------------------------------------------
# Scoring thresholds
# ---------------------------------------------------------------------------

# Two-tier alert thresholds.
# Scores >= ALERT_INSTANT_THRESHOLD fire an immediate Telegram message.
# Scores in [ALERT_DIGEST_THRESHOLD, ALERT_INSTANT_THRESHOLD) are buffered
# and sent as a periodic digest every DIGEST_INTERVAL_SECONDS.
ALERT_INSTANT_THRESHOLD: int = int(os.getenv("ALERT_INSTANT_THRESHOLD", "65"))
ALERT_DIGEST_THRESHOLD: int = int(os.getenv("ALERT_DIGEST_THRESHOLD", "60"))

# ---------------------------------------------------------------------------
# Pre-scorer trade filter
# ---------------------------------------------------------------------------

# Prices outside this band imply the market has effectively settled —
# no insider edge exists at 1¢ or 99¢.
FILTER_MIN_PRICE: float = float(os.getenv("FILTER_MIN_PRICE", "0.02"))
FILTER_MAX_PRICE: float = float(os.getenv("FILTER_MAX_PRICE", "0.98"))

# Bets below this USD notional are treated as noise regardless of score.
FILTER_MIN_BET_SIZE_USD: float = float(os.getenv("FILTER_MIN_BET_SIZE_USD", "50"))

# If fewer than this many minutes remain before market close, the alert
# cannot be acted on — filter it before scoring.
FILTER_MIN_ACTIONABLE_MINUTES: int = int(os.getenv("FILTER_MIN_ACTIONABLE_MINUTES", "10"))
# Market duration window: skip markets closing too soon (sub-24h bucket was unprofitable)
# or too far from close. Cut to 72h for data velocity: faster outcome feedback on the
# short-duration sports/esports universe where edge has appeared; drops unproven multi-week
# markets (Elon tweet counts, macro) that failed concentration testing.
# When end_date is unavailable, the trade passes through unchanged.
FILTER_MIN_HOURS_TO_CLOSE: float = float(os.getenv("FILTER_MIN_HOURS_TO_CLOSE", "6"))
# Entertainment-mode (2026-05-31): tightened 72h→48h for faster "did it hit?" cycles
# and better capital turnover on the thin balance — resolutions land sooner, the bot
# redeploys sooner, more outcome moments per day on the spectator feed.
FILTER_MAX_HOURS_TO_CLOSE: float = float(os.getenv("FILTER_MAX_HOURS_TO_CLOSE", "48"))

# Tradeable price band: only alert on bets within this range.
# Distinct from FILTER_MIN/MAX_PRICE (which reject glitch prices at 1-2¢).
# Entertainment-mode (2026-05-31): floor relaxed 0.50→0.30 to restore ~30% of alert
# volume the 0.50 favorites-only cut. KNOWN BLEED COST: the (0.30,0.50) band runs
# ~-8pp excess above implied (the loss-reduction rationale for 0.50 still holds AS
# MATH) — we're trading that bleed for activity/drama on a $20 spectator wallet,
# bounded by ≤$2/trade + the $10 daily-loss cap + the magnitude circuit breaker.
# (Deep <0.30 longshots still excluded — drop further or add a capped drama path if
# more lottery-ticket drama is wanted.)
FILTER_MIN_BET_PRICE: float = float(os.getenv("FILTER_MIN_BET_PRICE", "0.30"))
FILTER_MAX_BET_PRICE: float = float(os.getenv("FILTER_MAX_BET_PRICE", "0.90"))

# Market categories to exclude entirely. Parsed as comma-separated string from env.
# Entertainment-mode (2026-05-31): sports REINSTATED (default ""). Sports were
# excluded wholesale; clean-data ROI is only ~-3.5% on favorites (the old -39.9%
# read was inversion-era), and sports are entertainment-dense — daily matches,
# "did they win?!" moments. The small bleed is bounded by the sizing/daily-loss
# rails. (Soccer-favorites trader filter also disabled, see fly-trader config.)
FILTER_EXCLUDED_CATEGORIES: list[str] = [
    c.strip().lower()
    for c in os.getenv("FILTER_EXCLUDED_CATEGORIES", "").split(",")
    if c.strip()
]

# Minimum 1-week CLOB volume (USD) required to pass pre-scorer.
# Thin markets have unreliable prices and can't fill orders without slippage.
# Source: raw_json.volume1wkClob from Gamma API. Pass-through when field is absent.
FILTER_MIN_LIQUIDITY_USD: float = float(os.getenv("FILTER_MIN_LIQUIDITY_USD", "5000"))
DIGEST_INTERVAL_SECONDS: int = int(os.getenv("DIGEST_INTERVAL_SECONDS", "86400"))  # 24 hours
DIGEST_SEND_HOUR_UTC: int = int(os.getenv("DIGEST_SEND_HOUR_UTC", "0"))  # midnight UTC

# Attach a full-data CSV to each digest message. Set to false if Telegram
# file sends cause issues — the summary text is still sent either way.
DIGEST_CSV_ENABLED: bool = os.getenv("DIGEST_CSV_ENABLED", "true").lower() in ("true", "1", "yes")

# --- Component maximum points (must sum to 100 + bonuses) ---
# All weights are env-driven; change via Railway env vars, no code change needed.
# Rollback: revert env vars to old values, redeploy.
#
# Evidence-backed defaults (May 2026 backtest, n=5229 alerts, Regime C spine):
#   size_anomaly 33 — strongest cross-regime positive predictor (LR coef +0.32)
#   timing       20 — positive in Regime C but non-monotonic; partial reduction from 25
#   win_rate     15 — real signal once cap artifact is fixed; weight reduced from 20
#                      pending first 2-week re-run to confirm threshold effect
#   funding_vel  10 — borderline positive (MW p=0.05); held
#   concentration 10 — noise in Regime C (MW p=0.82); held
#   wallet_age   12 — significantly INVERTED in Regime C (MW p=0.03); cut sharply

# How close to market close is the bet? (0–20 pts default)
SCORE_MAX_TIMING: int = int(os.getenv("SCORE_MAX_TIMING", "20"))

# Funding-to-bet velocity: gap between last inbound transfer and the bet (0–10 pts default)
SCORE_MAX_FUNDING_VELOCITY: int = int(os.getenv("SCORE_MAX_FUNDING_VELOCITY", "10"))

# Historical win rate on resolved bets (0–15 pts default)
SCORE_MAX_WIN_RATE: int = int(os.getenv("SCORE_MAX_WIN_RATE", "15"))

# Bet size vs wallet median (0–33 pts default)
SCORE_MAX_SIZE_ANOMALY: int = int(os.getenv("SCORE_MAX_SIZE_ANOMALY", "33"))

# Wallet age — newer wallets score higher on this axis (0–12 pts default)
SCORE_MAX_WALLET_AGE: int = int(os.getenv("SCORE_MAX_WALLET_AGE", "12"))

# Capital concentration in a single market (0–10 pts default)
SCORE_MAX_CONCENTRATION: int = int(os.getenv("SCORE_MAX_CONCENTRATION", "10"))

# Betting on the underdog vs. betting the favorite — DISABLED (0 pts)
# 128-alert backtest: 14% win rate, -0.60 ROI (actively harmful signal).
SCORE_MAX_UNDERDOG: int = 0

# Cluster bonus: funded from same source as another flagged wallet (0 or +10)
# Zeroed out — fires on every alert, adds no information. Still tracked for analysis.
SCORE_CLUSTER_BONUS: int = 0

# ---------------------------------------------------------------------------
# Convergence detection — in-memory sliding window
# ---------------------------------------------------------------------------

# How many hours back to look for same-market, same-side trades.
CONVERGENCE_WINDOW_HOURS: int = int(os.getenv("CONVERGENCE_WINDOW_HOURS", "4"))

# Minimum distinct wallets required to flag an alert as a convergence event.
CONVERGENCE_MIN_WALLETS: int = int(os.getenv("CONVERGENCE_MIN_WALLETS", "2"))

# Score bonus added per additional wallet beyond the first (1 wallet = +0, 2 = +5, …).
CONVERGENCE_BONUS_PER_WALLET: int = int(os.getenv("CONVERGENCE_BONUS_PER_WALLET", "5"))

# Wider lookback window used only for contrarian detection (opposite side).
# Longer than CONVERGENCE_WINDOW_HOURS because 1-3 day markets trade slowly.
CONVERGENCE_CONTRARIAN_WINDOW_HOURS: int = int(os.getenv("CONVERGENCE_CONTRARIAN_WINDOW_HOURS", "8"))

# Maximum convergence bonus regardless of wallet count (caps at 5+ wallets = +20).
# Zeroed out — convergence was anti-predictive in production data. Still tracked for analysis.
CONVERGENCE_MAX_BONUS: int = int(os.getenv("CONVERGENCE_MAX_BONUS", "0"))

# --- Timing curve parameters ---
# Bets placed within this many hours of close score near maximum timing pts.
TIMING_MAX_SCORE_HOURS: float = float(os.getenv("TIMING_MAX_SCORE_HOURS", "2.0"))
# Bets placed beyond this many hours from close score near zero.
TIMING_ZERO_SCORE_HOURS: float = float(os.getenv("TIMING_ZERO_SCORE_HOURS", str(7 * 24)))  # 7 days

# --- Win rate thresholds ---
# High threshold raised 0.80→0.95 to restore gradation: the /v1/closed-positions API
# caps at 50 results regardless of limit, compressing 88% of wallets to a single
# saturated 10/10 score.  At 0.95, a wallet with 80% WR scores ~12/20 and a wallet
# with 95%+ WR scores the full 20/20.  Also env-driven for live tuning/rollback.
WINRATE_HIGH_THRESHOLD: float = float(os.getenv("WINRATE_HIGH_THRESHOLD", "0.95"))
WINRATE_LOW_THRESHOLD: float = float(os.getenv("WINRATE_LOW_THRESHOLD", "0.55"))
WINRATE_SIGNIFICANCE_BETS: int = int(os.getenv("WINRATE_SIGNIFICANCE_BETS", "20"))

# 2026-06-03 — win-rate component switched to a BINARY "has a winning track record" flag.
# The edge study (6,848 alerts vs on-chain outcomes) found the GRADED win-rate points are
# noise: among wallets that HAVE a track record the point gradient correlates -0.02 with
# winning. Only the EXISTENCE of a track record moved edge (-4.1% -> -0.4%). So award a flat
# flag when the wallet has >= WINRATE_FLAG_MIN_RESOLVED resolved bets AND win_rate above the
# low bar; 0 otherwise. Set WINRATE_BINARY_MODE=false to restore legacy graded scoring.
WINRATE_BINARY_MODE: bool = os.getenv("WINRATE_BINARY_MODE", "true").lower() == "true"
WINRATE_FLAG_PTS: int = int(os.getenv("WINRATE_FLAG_PTS", "8"))
WINRATE_FLAG_MIN_RESOLVED: int = int(os.getenv("WINRATE_FLAG_MIN_RESOLVED", "3"))

# Max resolved positions to fetch per wallet (pagination cap).
# The API returns at most 50 per page; we page until we hit this total or run out.
WINRATE_MAX_CLOSED_POSITIONS: int = int(os.getenv("WINRATE_MAX_CLOSED_POSITIONS", "500"))

# --- Funding velocity thresholds ---
# Gap between most recent inbound transfer and the bet.
# ≤ FAST_HOURS → near-max score (funded and deployed immediately = insider pattern)
FUNDING_VELOCITY_FAST_HOURS: float = float(os.getenv("FUNDING_VELOCITY_FAST_HOURS", "1.0"))
# > SLOW_HOURS → 0 pts (funds sat idle long enough to be unremarkable)
FUNDING_VELOCITY_SLOW_HOURS: float = float(os.getenv("FUNDING_VELOCITY_SLOW_HOURS", str(7 * 24)))  # 7 days

# --- Size anomaly thresholds ---
SIZE_ANOMALY_HIGH_MULTIPLE: float = 5.0   # 5× median → near-max score
SIZE_ANOMALY_LOW_MULTIPLE: float = 1.5    # < 1.5× median → 0 pts

# --- Wallet age thresholds (in days) ---
WALLET_AGE_NEW_DAYS: int = 30    # Under 30 days → high score on this axis
WALLET_AGE_OLD_DAYS: int = 365   # Over 1 year → near-zero on this axis

# --- Concentration thresholds ---
CONCENTRATION_HIGH_PCT: float = 0.70  # 70%+ of capital in one market → near-max
CONCENTRATION_LOW_PCT: float = 0.10   # Under 10% → 0 pts

# --- Underdog price thresholds ---
UNDERDOG_MAX_PRICE: float = 0.30   # Price <= 0.30 → near-max underdog score
UNDERDOG_MIN_PRICE: float = 0.60   # Price >= 0.60 → 0 pts (clear favorite)

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

SQLITE_DB_PATH: str = os.getenv("SQLITE_DB_PATH", "polymarket_bot.db")

# ---------------------------------------------------------------------------
# HTTP client settings
# ---------------------------------------------------------------------------

HTTP_TIMEOUT_SECONDS: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
HTTP_MAX_RETRIES: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))
HTTP_RETRY_BACKOFF_SECONDS: float = float(os.getenv("HTTP_RETRY_BACKOFF_SECONDS", "2.0"))

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

# ---------------------------------------------------------------------------
# Trading bot
# ---------------------------------------------------------------------------

# Master kill switch. Must be explicitly set to "true" to enable live trades.
TRADING_ENABLED: bool = os.getenv("TRADING_ENABLED", "false").lower() in ("true", "1", "yes")

# Private key of the wallet that will execute trades (hex, no 0x prefix required).
# NEVER logged anywhere — only the derived wallet address is logged.
TRADING_PRIVATE_KEY: str = os.getenv("TRADING_PRIVATE_KEY", "")

# Fixed USDC size per trade. Keep small during initial testing.
TRADING_BET_SIZE_USDC: float = float(os.getenv("TRADING_BET_SIZE_USDC", "2.0"))

# Risk limits — trading pauses when these are breached.
TRADING_MAX_DAILY_LOSS_USDC: float = float(os.getenv("TRADING_MAX_DAILY_LOSS_USDC", "10.0"))
TRADING_MAX_CONCURRENT_POSITIONS: int = int(os.getenv("TRADING_MAX_CONCURRENT_POSITIONS", "10"))
TRADING_MAX_SINGLE_POSITION_USDC: float = float(os.getenv("TRADING_MAX_SINGLE_POSITION_USDC", "5.0"))

# After N consecutive losses, pause trading for TRADING_PAUSE_DURATION_SECONDS.
TRADING_CONSECUTIVE_LOSS_PAUSE: int = int(os.getenv("TRADING_CONSECUTIVE_LOSS_PAUSE", "3"))
TRADING_PAUSE_DURATION_SECONDS: int = int(os.getenv("TRADING_PAUSE_DURATION_SECONDS", "7200"))

# Only execute alerts with score >= this value.
TRADING_MIN_SCORE: int = int(os.getenv("TRADING_MIN_SCORE", "65"))

# How often to poll alert_outcomes for new tradeable alerts.
TRADING_POLL_INTERVAL_SECONDS: int = int(os.getenv("TRADING_POLL_INTERVAL_SECONDS", "30"))

# Polymarket CLOB endpoint (constant — not user-configurable).
TRADING_CLOB_HOST: str = "https://clob.polymarket.com"
TRADING_CHAIN_ID: int = 137

# Dynamic position sizing — activates automatically once the bot has enough
# resolved trades AND a positive cumulative P&L. No manual toggle needed.
# Sizing: balance × TRADING_BET_PERCENTAGE, clamped to [MIN, MAX].
TRADING_BET_PERCENTAGE: float = float(os.getenv("TRADING_BET_PERCENTAGE", "0.02"))       # 2% of bankroll
TRADING_MIN_BET_USDC: float = float(os.getenv("TRADING_MIN_BET_USDC", "1.0"))            # floor
TRADING_MAX_BET_USDC: float = float(os.getenv("TRADING_MAX_BET_USDC", "10.0"))           # ceiling
TRADING_DYNAMIC_MIN_RESOLVED: int = int(os.getenv("TRADING_DYNAMIC_MIN_RESOLVED", "20")) # warmup guard

# Profit sweeping — active whenever VAULT_WALLET_ADDRESS is set (non-empty).
# The user must consciously choose a vault address; the bot never auto-generates one.
# Once set, sweeping runs automatically every VAULT_SWEEP_INTERVAL_SECONDS.
# ---------------------------------------------------------------------------
# Core scaling parameters — change ONLY these three to re-calibrate the system.
# Bet size, max concurrent positions, sweep threshold and floor all derive from them.
# ---------------------------------------------------------------------------

# How much USDC to keep in the trading wallet at all times (= sweep floor).
TRADING_WORKING_CAPITAL_USDC: float = float(os.getenv("TRADING_WORKING_CAPITAL_USDC", "110.0"))
# How much above working capital triggers a sweep (sweep threshold = working_capital + headroom).
TRADING_SWEEP_HEADROOM_USDC: float = float(os.getenv("TRADING_SWEEP_HEADROOM_USDC", "40.0"))
# Fraction of bankroll to keep deployed across open positions at once.
TRADING_TARGET_EXPOSURE_PCT: float = float(os.getenv("TRADING_TARGET_EXPOSURE_PCT", "0.50"))

# Position count safety clamps — hard floor/ceiling regardless of exposure calc.
TRADING_MAX_POSITIONS_FLOOR: int = int(os.getenv("TRADING_MAX_POSITIONS_FLOOR", "10"))
TRADING_MAX_POSITIONS_CEILING: int = int(os.getenv("TRADING_MAX_POSITIONS_CEILING", "50"))

VAULT_WALLET_ADDRESS: str = os.getenv("VAULT_WALLET_ADDRESS", "")
# Derived from core scaling params; can still be overridden directly for backward compat.
VAULT_SWEEP_THRESHOLD_USDC: float = float(os.getenv("VAULT_SWEEP_THRESHOLD_USDC", str(TRADING_WORKING_CAPITAL_USDC + TRADING_SWEEP_HEADROOM_USDC)))
VAULT_SWEEP_FLOOR_USDC: float = float(os.getenv("VAULT_SWEEP_FLOOR_USDC", str(TRADING_WORKING_CAPITAL_USDC)))
VAULT_SWEEP_INTERVAL_SECONDS: int = int(os.getenv("VAULT_SWEEP_INTERVAL_SECONDS", "3600"))

# ---------------------------------------------------------------------------
# The "brain" — LLM forecaster (shadow-mode, hard cost cap)
# ---------------------------------------------------------------------------
# An independent calibrated forecaster (Claude) that researches a market from
# the open web and produces its OWN probability — the bot's attempt at real
# alpha after the edge audit proved the insider signal is a 50/50 coin flip.
# It runs in SHADOW by default: it logs forecasts to brain_forecasts and posts
# high-conviction calls to the ops channel, but NEVER trades. No API key (or
# BRAIN_ENABLED=false) → the whole subsystem no-ops. Forward-validate calibration
# (Brier vs market) for weeks before any graduation to real bets.

# Master switch. Off until an Anthropic API key is provided AND we choose to run it.
BRAIN_ENABLED: bool = os.getenv("BRAIN_ENABLED", "false").lower() in ("true", "1", "yes")
# The bot's OWN Anthropic API key (separate from the user's Claude Max subscription).
BRAIN_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
# Shadow mode: log + post forecasts, NEVER place a real trade. Keep true until calibration is proven.
BRAIN_SHADOW: bool = os.getenv("BRAIN_SHADOW", "true").lower() in ("true", "1", "yes")

# Models: cheap triage (Haiku tier) gates the spend; stronger research/forecast (Sonnet tier).
BRAIN_TRIAGE_MODEL: str = os.getenv("BRAIN_TRIAGE_MODEL", "claude-haiku-4-5")
BRAIN_FORECAST_MODEL: str = os.getenv("BRAIN_FORECAST_MODEL", "claude-sonnet-4-6")
# Effort for the Sonnet calls (low is cheap and still strong). Haiku gets no effort param.
BRAIN_EFFORT: str = os.getenv("BRAIN_EFFORT", "low")

# Hard daily spend cap (USD). Token + web-search cost is tracked per call; once the
# cap is hit, the brain stops making calls until UTC-midnight rollover.
# 2026-07-04 cost audit: 10 days of live data showed ~$2.2/day (~$66/mo) — 90% of it scanner web
# research ($0.31/forecast), while 297 Haiku-cheap vets cost $2.12 TOTAL. Operator hit their API
# credit cap. New budget: $1.25/day ≈ $35/mo ceiling → ~3-4 researched markets/day (the picks'
# 7-0 record was built on ~6/day, so this keeps the winning strategy alive at half the burn).
BRAIN_DAILY_USD_CAP: float = float(os.getenv("BRAIN_DAILY_USD_CAP", "1.25"))
# Conservative pre-check estimate of one full researched forecast's cost (triage + web
# research + ensemble). Measured live 2026-07-04: ~$0.31 sequential, ~$0.16 at batch rates
# (the batch path budgets with this × 0.5). The old $0.05 guess let cycles start on fumes.
BRAIN_EST_FORECAST_USD: float = float(os.getenv("BRAIN_EST_FORECAST_USD", "0.30"))

# Ensemble: N independent forecast runs → mean → (reconcile if they disagree) → calibrate.
# 2 runs + reconcile-on-disagreement retains most of the ensemble value at 2/3 the cost
# (2026-07-04 throughput push — the saved tokens buy more researched markets instead).
BRAIN_ENSEMBLE_N: int = int(os.getenv("BRAIN_ENSEMBLE_N", "2"))
# Real-time vet (the trader calls /api/brain/vet at trade time): a SMALLER ensemble so the
# verdict comes back fast enough to actually size the position. Cost audit 2026-07-04: vets are
# commentary + rarely gate anything, so they run on Haiku (5x cheaper than Sonnet) with a single
# run — ~$0.001/vet, effectively free. Sonnet stays on the scanner where the money rides.
BRAIN_VET_ENSEMBLE_N: int = int(os.getenv("BRAIN_VET_ENSEMBLE_N", "1"))
BRAIN_VET_MODEL: str = os.getenv("BRAIN_VET_MODEL", "claude-haiku-4-5")
# Billing circuit-breaker: when the Anthropic account is OUT OF CREDITS (400 billing error),
# pause ALL brain API calls for this long instead of hammering failed requests every cycle.
BRAIN_BILLING_COOLDOWN_S: int = int(os.getenv("BRAIN_BILLING_COOLDOWN_S", "14400"))  # 4h
# Audience decision digest (to V1 Poly): how often the brain broadcasts a readable summary of
# what it blessed / passed on, keeping the channel alive between bets.
BRAIN_DIGEST_HOURS: float = float(os.getenv("BRAIN_DIGEST_HOURS", "4"))
# Web search in the REAL-TIME vet: measured at ~69s and ~$0.58/call (token-heavy search context)
# — too slow for the trade path (the connection times out before it returns) and far too costly.
# Default OFF: the real-time vet reasons fast from the model's own knowledge + base rates + the
# market price (~8s, ~$0.02). The hourly SCANNER still web-researches its independent ideas, where
# latency doesn't matter. Set true to web-research each vet (accepting the latency/cost/timeout risk).
BRAIN_VET_WEB_SEARCH: bool = os.getenv("BRAIN_VET_WEB_SEARCH", "false").lower() in ("true", "1", "yes")

# --- Brain Picks: the brain's OWN research-driven trades on thin/obscure markets ---
# The real unlock. The scanner web-researches thin/obscure markets (where LLM forecasting
# genuinely beats the price, per AIA Forecaster) and, when it finds a high-conviction edge,
# emits a token-SAFE synthetic alert the trader can take at DISCOVERY stakes. This is a NEW
# strategy, separate from the no-edge insider-sports path. Unvalidated → tiny stakes + high bar
# + gated; graded forward like any pick. Set BRAIN_PICK_ENABLED to have the scanner emit picks.
BRAIN_PICK_ENABLED: bool = os.getenv("BRAIN_PICK_ENABLED", "false").lower() in ("true", "1", "yes")
# Minimum CALIBRATED edge (brain's prob − price, on the side it would buy) to emit a pick.
# 2026-07-12 FEE AUDIT: Polymarket's zero-fee era ENDED this spring — dynamic taker fees
# (≈ rate × (1−p) of stake) now cover nearly every category, and thin-market spreads run
# 6-10¢. Total friction on a cheap-side thin-market entry: 8-12% of stake. The bars below
# fold in that friction ON TOP of the favorite-longshot tiering fit to our own cash record
# (longshot buys 0-for-3 at −100%; favorite buys +8% ROI). Cheap sides pay the most fee AND
# carry the documented bias against them — both push the same direction.
BRAIN_PICK_MIN_EDGE: float = float(os.getenv("BRAIN_PICK_MIN_EDGE", "0.10"))       # buys ≥0.55
BRAIN_PICK_EDGE_MID: float = float(os.getenv("BRAIN_PICK_EDGE_MID", "0.15"))       # buys 0.40-0.55
BRAIN_PICK_EDGE_LONGSHOT: float = float(os.getenv("BRAIN_PICK_EDGE_LONGSHOT", "0.22"))  # buys <0.40
# Too-good-to-be-true guard: claimed edges >0.35 were a COIN FLIP in our graded record (n=8 at
# |edge|>0.20 → 50%) — monster disagreements are as likely stale-data delusions as insight.
BRAIN_PICK_MAX_EDGE: float = float(os.getenv("BRAIN_PICK_MAX_EDGE", "0.35"))
# Minimum confidence (web-researched, so it can clear a real bar — unlike the humble no-web vet).
BRAIN_PICK_MIN_CONFIDENCE: float = float(os.getenv("BRAIN_PICK_MIN_CONFIDENCE", "0.60"))
# Score stamped on the synthetic alert so it clears the trader's min-score + premium-tier gates.
BRAIN_PICK_SCORE: int = int(os.getenv("BRAIN_PICK_SCORE", "90"))
# Red-team gate (2026-07-10): before ANY pick emits, one adversarial pass argues the market's
# side — stale facts (check dates), misread resolution rules, ignored base rates — and can kill
# the trade. Targets the observed failure mode (the stale-headline BTC bet). Additive filter:
# if the red team can't run (budget/error), the pick proceeds as before. ~$0.01-0.02 per candidate.
BRAIN_RED_TEAM_ENABLED: bool = os.getenv("BRAIN_RED_TEAM_ENABLED", "true").lower() in ("true", "1", "yes")
# If the ensemble's stdev exceeds this, spend one supervisor call to reconcile.
BRAIN_RECONCILE_STD: float = float(os.getenv("BRAIN_RECONCILE_STD", "0.15"))
# Platt/log-odds calibration coefficient. HISTORY: launched at the literature prior √3≈1.73
# ("extremize to counteract LLM hedging"). 2026-07-09 re-fit on OUR 56 graded forecasts found
# the optimum at ~0.7-0.8 — this brain is OVERconfident, not a hedger, and extremizing it
# manufactured phantom edges (the root cause of picks winning 77% of bets while losing money:
# fake edges bought at full spread). <1 shrinks toward 0.5; re-fit again as the sample grows.
BRAIN_PLATT_COEF: float = float(os.getenv("BRAIN_PLATT_COEF", "0.8"))

# Decision thresholds (shadow: controls what gets flagged; live: would gate a trade).
BRAIN_EDGE_THRESHOLD: float = float(os.getenv("BRAIN_EDGE_THRESHOLD", "0.10"))   # min |brain_prob − price|
BRAIN_MIN_CONFIDENCE: float = float(os.getenv("BRAIN_MIN_CONFIDENCE", "0.55"))
BRAIN_KELLY_CAP: float = float(os.getenv("BRAIN_KELLY_CAP", "0.25"))             # quarter-Kelly ceiling

# Loop cadence + per-cycle caps (also bounded by the daily spend cap).
# 2026-07-04: 1h/6-per-cycle burned the cap by mid-day; 2h/2-per-cycle spreads ~4 researched
# markets across the whole day so evening markets get looked at too.
BRAIN_SCAN_INTERVAL_SECONDS: int = int(os.getenv("BRAIN_SCAN_INTERVAL_SECONDS", "7200"))
BRAIN_MAX_PER_CYCLE: int = int(os.getenv("BRAIN_MAX_PER_CYCLE", "2"))
# Batch API scanner (2026-07-04): the scanner runs through the Batches API at a 50% token
# discount — it has zero latency sensitivity (2h cycles; batches typically clear in minutes to
# ~1h), so the same $1.25/day researches ~2x the markets (~7-8/day vs ~4). Falls back to the
# sequential path automatically if a batch fails.
BRAIN_BATCH_ENABLED: bool = os.getenv("BRAIN_BATCH_ENABLED", "true").lower() in ("true", "1", "yes")
BRAIN_BATCH_MARKETS: int = int(os.getenv("BRAIN_BATCH_MARKETS", "2"))       # EVENTS per batched cycle
BRAIN_BATCH_POLL_SECONDS: int = int(os.getenv("BRAIN_BATCH_POLL_SECONDS", "20"))
BRAIN_BATCH_TIMEOUT_S: int = int(os.getenv("BRAIN_BATCH_TIMEOUT_S", "2700"))  # 45 min < the 2h cycle
# Event-grouped research (2026-07-04, the throughput unlock): one match carries 4-6 tradeable
# derivative markets (O/U lines, spread, BTTS, exact scores) that all share the SAME research.
# The scanner now researches the EVENT once and forecasts up to BRAIN_EVENT_SIBLINGS sibling
# markets against the shared brief — ~3x pick flow per research dollar. The $8/event exposure
# cap (trader-side) keeps the resulting correlated picks bounded by construction.
BRAIN_EVENT_SIBLINGS: int = int(os.getenv("BRAIN_EVENT_SIBLINGS", "4"))
# Wide-triage / narrow-research funnel (2026-07-11, scale push): triage is Haiku pennies, so
# triage MANY events per cycle and spend the Sonnet research budget only on the most
# researchable of the survivors. Same $1.25/day, pointed at the best of a 3-5x wider funnel.
BRAIN_TRIAGE_WIDE: int = int(os.getenv("BRAIN_TRIAGE_WIDE", "10"))       # events triaged/cycle
BRAIN_SCAN_PAGES: int = int(os.getenv("BRAIN_SCAN_PAGES", "4"))          # Gamma pages pulled (deep = the true long tail)
# Web searches per research call. 2026-07-09 v2 re-aim: 2 shallow searches couldn't beat
# quant-priced markets (56-forecast Brier read: brain ≈ market at best). v2 trades breadth
# for depth — fewer researched events, 4 searches each.
BRAIN_RESEARCH_MAX_SEARCHES: int = int(os.getenv("BRAIN_RESEARCH_MAX_SEARCHES", "4"))
# Scanner targeting (v2): soccer-derivative books are priced by models even when thin —
# our graded data shows no edge there. TRUE mispricing lives where information is scattered
# (politics, entertainment, crypto, niche one-offs). Sports markets are deprioritized (not
# excluded): non-sports candidates get the research budget first; sports fill leftovers.
BRAIN_SCAN_DEPRIORITIZE_SPORTS: bool = os.getenv("BRAIN_SCAN_DEPRIORITIZE_SPORTS", "true").lower() in ("true", "1", "yes")
BRAIN_REFORECAST_HOURS: float = float(os.getenv("BRAIN_REFORECAST_HOURS", "72"))  # don't re-forecast a market within this window

# Scanner (long-tail mispricing hunt) market filters.
BRAIN_SCAN_MIN_PRICE: float = float(os.getenv("BRAIN_SCAN_MIN_PRICE", "0.12"))
BRAIN_SCAN_MAX_PRICE: float = float(os.getenv("BRAIN_SCAN_MAX_PRICE", "0.88"))
# Min 1.5d (2026-07-12): a 292M-trade calibration study shows prices are near-perfectly
# calibrated in the final hours (slope 0.99 <1h out) — the favorite edge lives at LONGER
# horizons (slope 1.32 beyond a month). No picks inside the efficient zone.
BRAIN_SCAN_MIN_DAYS: float = float(os.getenv("BRAIN_SCAN_MIN_DAYS", "1.5"))
BRAIN_SCAN_MAX_DAYS: float = float(os.getenv("BRAIN_SCAN_MAX_DAYS", "30"))
BRAIN_SCAN_MIN_VOL_USD: float = float(os.getenv("BRAIN_SCAN_MIN_VOL_USD", "2000"))   # enough liquidity to be real
BRAIN_SCAN_MAX_VOL_USD: float = float(os.getenv("BRAIN_SCAN_MAX_VOL_USD", "250000")) # but thin/obscure (LLM edge lives here, not on liquid markets)
BRAIN_SCAN_GAMMA_LIMIT: int = int(os.getenv("BRAIN_SCAN_GAMMA_LIMIT", "120"))

# Veto/confirm layer: re-forecast recent insider alerts to log agree/disagree.
BRAIN_VETO_MIN_SCORE: int = int(os.getenv("BRAIN_VETO_MIN_SCORE", "60"))
BRAIN_VETO_LOOKBACK_HOURS: float = float(os.getenv("BRAIN_VETO_LOOKBACK_HOURS", "12"))

# Daily calibration report (Brier brain vs market) to ops.
BRAIN_CAL_REPORT_HOUR_UTC: int = int(os.getenv("BRAIN_CAL_REPORT_HOUR_UTC", "13"))

# ---------------------------------------------------------------------------
# API server (Railway auto-injects PORT; 8080 is the fallback)
# ---------------------------------------------------------------------------

# Shared secret between Railway API and Fly.io trader. Fail-closed when empty.
API_SECRET_KEY: str = os.getenv("API_SECRET_KEY", "")

# Railway injects its own PORT env var — always use this, never hard-code 8080.
PORT: int = int(os.getenv("PORT", "8080"))


# ---------------------------------------------------------------------------
# Validation — fail loudly at startup if critical env vars are missing
# ---------------------------------------------------------------------------

def validate_config() -> None:
    """
    Called once at startup. Raises ValueError if any required env var is absent.
    Optional vars (Alchemy, Etherscan) emit warnings — the bot degrades
    gracefully when they are missing.
    """
    errors = []

    if not TELEGRAM_BOT_TOKEN:
        errors.append("TELEGRAM_BOT_TOKEN is not set")
    if not TELEGRAM_CHAT_ID:
        errors.append("TELEGRAM_CHAT_ID is not set")

    if errors:
        raise ValueError("Configuration errors:\n" + "\n".join(f"  • {e}" for e in errors))

    log = logging.getLogger("config")

    if not ETHERSCAN_API_KEY:
        log.warning(
            "ETHERSCAN_API_KEY not set — wallet age scoring will be skipped"
        )
    if not ALCHEMY_RPC_URL:
        log.warning(
            "ALCHEMY_RPC_URL not set — proxy wallet cluster detection disabled"
        )
    if DRY_RUN:
        log.warning("DRY_RUN=true — Telegram alerts will NOT be sent")

    log.info("Configuration validated OK")
    log.info(
        "Instant threshold: %d | Digest threshold: %d | Daily brief at %02d:00 UTC | "
        "Trade min size: $%.0f | Cache TTL: %dh",
        ALERT_INSTANT_THRESHOLD,
        ALERT_DIGEST_THRESHOLD,
        DIGEST_SEND_HOUR_UTC,
        TRADE_MIN_SIZE_USD,
        WALLET_CACHE_TTL_SECONDS // 3600,
    )

# Abstain-on-disagreement (2026-07-12): if the forecast ensemble still disagrees by more than
# this stdev AFTER reconciliation, confidence is capped below the pick bar — "we don't know"
# is a position. Published result: agreement-filtering improves realized returns.
BRAIN_ABSTAIN_STD: float = float(os.getenv("BRAIN_ABSTAIN_STD", "0.18"))

# REGIME BOUNDARY for the Stage-1 gates (2026-07-13). Pre-registered, tied to a code change,
# NOT data-mined: 2026-07-12T15:00Z is just after the fee-aware tiered edge bars shipped
# (commit ad8aaed, 14:53Z — the Platt refit landed 07-09, the maker era 07-13). Gate
# evidence from before this boundary measures a brain that no longer exists (fee-blind flat
# bars) and would dilute the CI floor forever. Midnight would have been cleaner but wrong:
# picks emitted that morning ran the OLD bars (one such pick verifiably filled at 04:55Z).
# Picks/forecasts BEFORE this ts still exist and still grade — they just don't count toward
# graduation.
BRAIN_REGIME_START_TS: int = int(os.getenv("BRAIN_REGIME_START_TS", "1783868400"))

# GRADUATION SPRINT (2026-07-13): while the new-regime graded-pick count is below the n gate,
# the scanner runs at double cadence and researches one extra event per cycle. Self-limiting:
# the moment n_graded reaches the target (or stage1 flips), everything reverts to defaults.
# (The 07-13 fast-close-front-loading experiment was REVERTED 07-16 — fast markets are
# efficiently priced and produced zero picks; see the sort comment in brain.py.)
BRAIN_SPRINT_ENABLED: bool = os.getenv("BRAIN_SPRINT_ENABLED", "true").lower() == "true"
BRAIN_SPRINT_TARGET: int = int(os.getenv("BRAIN_SPRINT_TARGET", "40"))
