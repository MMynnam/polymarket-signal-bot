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
MARKET_DISCOVERY_INTERVAL_SECONDS: int = int(os.getenv("MARKET_DISCOVERY_INTERVAL_SECONDS", "300"))  # 5 minutes

# How often the resolution-checker polls Gamma for outcome updates.
RESOLUTION_CHECK_INTERVAL_SECONDS: int = int(os.getenv("RESOLUTION_CHECK_INTERVAL_SECONDS", "3600"))  # 1 hour

# Maximum markets to fetch per Gamma poll (offset pagination).
GAMMA_MARKETS_LIMIT: int = int(os.getenv("GAMMA_MARKETS_LIMIT", "200"))

# ---------------------------------------------------------------------------
# Wallet profiling (Data API + Etherscan V2)
# ---------------------------------------------------------------------------

# SQLite cache TTL for wallet profiles — don't re-fetch within this window.
WALLET_CACHE_TTL_SECONDS: int = int(os.getenv("WALLET_CACHE_TTL_SECONDS", str(6 * 3600)))  # 6 hours

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
ALERT_INSTANT_THRESHOLD: int = int(os.getenv("ALERT_INSTANT_THRESHOLD", "75"))
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
DIGEST_INTERVAL_SECONDS: int = int(os.getenv("DIGEST_INTERVAL_SECONDS", "7200"))  # 2 hours

# Attach a full-data CSV to each digest message. Set to false if Telegram
# file sends cause issues — the summary text is still sent either way.
DIGEST_CSV_ENABLED: bool = os.getenv("DIGEST_CSV_ENABLED", "true").lower() in ("true", "1", "yes")

# --- Component maximum points (must sum to 100 + 10 bonus) ---

# How close to market close is the bet? (0–25 pts)
SCORE_MAX_TIMING: int = 25

# Funding-to-bet velocity: gap between last inbound transfer and the bet (0–20 pts)
SCORE_MAX_FUNDING_VELOCITY: int = 10

# Historical win rate on resolved bets (0–10 pts)
SCORE_MAX_WIN_RATE: int = 10

# Bet size vs wallet median (0–20 pts)
SCORE_MAX_SIZE_ANOMALY: int = 20

# Wallet age — newer wallets score higher on this axis (0–25 pts)
SCORE_MAX_WALLET_AGE: int = 25

# Capital concentration in a single market (0–10 pts)
SCORE_MAX_CONCENTRATION: int = 10

# Betting on the underdog vs. betting the favorite — DISABLED (0 pts)
# 128-alert backtest: 14% win rate, -0.60 ROI (actively harmful signal).
SCORE_MAX_UNDERDOG: int = 0

# Cluster bonus: funded from same source as another flagged wallet (0 or +10)
SCORE_CLUSTER_BONUS: int = 10

# --- Timing curve parameters ---
# Bets placed within this many hours of close score near maximum timing pts.
TIMING_MAX_SCORE_HOURS: float = float(os.getenv("TIMING_MAX_SCORE_HOURS", "2.0"))
# Bets placed beyond this many hours from close score near zero.
TIMING_ZERO_SCORE_HOURS: float = float(os.getenv("TIMING_ZERO_SCORE_HOURS", str(7 * 24)))  # 7 days

# --- Win rate thresholds ---
WINRATE_HIGH_THRESHOLD: float = 0.80  # 80%+ → near-max score
WINRATE_LOW_THRESHOLD: float = 0.50   # 50% or below → 0 pts
WINRATE_SIGNIFICANCE_BETS: int = 20   # >= 20 resolved bets → full statistical weight

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
        "Instant threshold: %d | Digest threshold: %d | Digest interval: %ds | "
        "Trade min size: $%.0f | Cache TTL: %dh",
        ALERT_INSTANT_THRESHOLD,
        ALERT_DIGEST_THRESHOLD,
        DIGEST_INTERVAL_SECONDS,
        TRADE_MIN_SIZE_USD,
        WALLET_CACHE_TTL_SECONDS // 3600,
    )
