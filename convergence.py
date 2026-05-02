"""
convergence.py — In-memory sliding-window convergence detector.

Tracks alert-worthy trades (score >= ALERT_DIGEST_THRESHOLD) grouped by
(market_id, bet_side). When multiple distinct wallets bet the same side on
the same market within CONVERGENCE_WINDOW_HOURS, a score bonus is applied
to each participating trade.

Memory contract: The window is in-memory only and resets on bot restart.
Convergence is a real-time signal — historical convergence has no value after
the market resolves. Do not attempt to persist or reload the window.

Bonus tiers (distinct wallets → bonus pts):
  1 wallet  →  +0  (no convergence; just the current trade)
  2 wallets →  +5
  3 wallets → +10
  4 wallets → +15
  5+ wallets → +20  (capped at CONVERGENCE_MAX_BONUS)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import config

log = logging.getLogger("convergence")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ConvergenceResult:
    distinct_wallets: int = 0
    total_volume: float = 0.0
    convergence_bonus: int = 0
    is_convergence_alert: bool = False
    wallet_addresses: list = field(default_factory=list)  # str list


# Key: (market_id, bet_side_normalised)
# Value: list of entry dicts {wallet_address, score, timestamp, trade_id, bet_size_usd}
_window: dict[tuple, list] = {}
_lock = asyncio.Lock()

# Safety valve: if total entries across all keys exceeds this, prune oldest.
_MAX_TOTAL_ENTRIES: int = 10_000
_PRUNE_TARGET: int = 8_000


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def check_convergence(
    market_id: str,
    bet_side: str,
    wallet_address: str,
    score: int,
    trade_id: str,
    bet_size_usd: float,
    timestamp: float,
) -> ConvergenceResult:
    """
    Register a trade in the convergence window and return a ConvergenceResult.

    Only adds the trade to the window when score >= ALERT_DIGEST_THRESHOLD —
    low-quality trades from random wallets do not count toward convergence.

    The same wallet betting the same side multiple times counts as 1
    (deduplicated by wallet_address within the window).

    This function is safe to call from concurrent asyncio tasks.
    """
    key = (market_id, bet_side.strip().lower())
    cutoff = timestamp - config.CONVERGENCE_WINDOW_HOURS * 3600

    async with _lock:
        # Prune expired entries for this key.
        if key in _window:
            _window[key] = [e for e in _window[key] if e["timestamp"] >= cutoff]
        else:
            _window[key] = []

        # Only register trades that individually meet the digest threshold.
        if score >= config.ALERT_DIGEST_THRESHOLD:
            _window[key].append({
                "wallet_address": wallet_address.lower(),
                "score":          score,
                "timestamp":      timestamp,
                "trade_id":       trade_id,
                "bet_size_usd":   bet_size_usd,
            })

        # Global size safety valve.
        total_entries = sum(len(v) for v in _window.values())
        if total_entries > _MAX_TOTAL_ENTRIES:
            log.warning(
                "[Convergence] Window size %d exceeds %d — pruning oldest entries globally",
                total_entries, _MAX_TOTAL_ENTRIES,
            )
            _prune_global()

        # Deduplicate by wallet address: keep latest entry per wallet.
        seen: dict[str, dict] = {}
        for e in _window[key]:
            addr = e["wallet_address"]
            if addr not in seen or e["timestamp"] > seen[addr]["timestamp"]:
                seen[addr] = e

        distinct_wallets = len(seen)
        total_volume = sum(e["bet_size_usd"] for e in seen.values())
        wallet_list = list(seen.keys())

    # Compute bonus outside the lock.
    bonus = min(
        config.CONVERGENCE_MAX_BONUS,
        max(0, (distinct_wallets - 1) * config.CONVERGENCE_BONUS_PER_WALLET),
    )

    return ConvergenceResult(
        distinct_wallets=distinct_wallets,
        total_volume=total_volume,
        convergence_bonus=bonus,
        is_convergence_alert=(distinct_wallets >= config.CONVERGENCE_MIN_WALLETS),
        wallet_addresses=wallet_list,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _prune_global() -> None:
    """
    Remove the oldest entries globally until total count is below _PRUNE_TARGET.
    Called while holding _lock.
    """
    all_entries: list[tuple] = []
    for key, entries in _window.items():
        for e in entries:
            all_entries.append((key, e))

    all_entries.sort(key=lambda x: x[1]["timestamp"])
    to_drop = len(all_entries) - _PRUNE_TARGET
    if to_drop <= 0:
        return

    # Rebuild window from surviving entries only.
    for key in _window:
        _window[key] = []
    for key, entry in all_entries[to_drop:]:
        _window[key].append(entry)

    log.info("[Convergence] Global prune complete: dropped %d entries", to_drop)
