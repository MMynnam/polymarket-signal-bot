"""
stats.py — CLI script to print outcome statistics for the signal bot.

Usage:
  python stats.py                  # All-time stats
  python stats.py --since-days 30  # Last 30 days only

Requires the bot database to exist (polymarket_bot.db by default).
All SQL lives in database.get_outcome_stats(); this script only formats
and does arithmetic on the returned dict.
"""

import argparse
import json
import sys
import time
from typing import Optional

import config  # noqa: F401 — loads .env and logging config as a side-effect
import database

# Minimum resolved alerts before we trust the stats enough to print breakdowns.
_MIN_RESOLVED_FOR_STATS: int = 10

# Score buckets and their display labels, matching database.get_outcome_stats().
_SCORE_BUCKET_LABELS = ["75-79", "80-89", "90-99", "100+"]

# Component definitions: (display_label, primary_json_key, fallback_json_key_or_None, max_pts)
# primary_json_key is the current ScoreBreakdown field name.
# fallback_json_key is checked when primary returns None — used for backward
# compatibility with rows written before a component was renamed.
_COMPONENTS = [
    ("timing",        "timing",            None,       25),
    ("funding_vel",   "funding_velocity",  None,       10),
    ("win_rate",      "win_rate",          None,       10),
    ("size_anomaly",  "size_anomaly",      None,       20),
    ("wallet_age",    "wallet_age",        None,       25),
    ("concentration", "concentration",     None,       10),
    ("cluster",       "cluster_bonus",     None,       10),
    ("convergence",   "convergence_bonus", None,       20),
]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "—"
    return f"{100 * numerator // denominator}%"


def _roi_str(roi: Optional[float]) -> str:
    if roi is None:
        return "—"
    sign = "+" if roi >= 0 else ""
    return f"{sign}{roi:.2f}"


def _col(text: str, width: int) -> str:
    """Left-justify text in a fixed-width column."""
    return str(text).ljust(width)


# ---------------------------------------------------------------------------
# Component correlation analysis (pure Python, no SQL)
# ---------------------------------------------------------------------------

def _component_stats(resolved_rows: list[dict]) -> list[dict]:
    """
    For each scoring component, split resolved alerts into two buckets:
      high — component score > 50% of its maximum
      low  — component score <= 50% of its maximum

    Returns a list of dicts ready for printing.
    """
    results = []

    for label, key, fallback_key, max_pts in _COMPONENTS:
        threshold = max_pts * 0.5

        high_wins = high_total = 0
        low_wins  = low_total  = 0
        high_roi: list[float] = []
        low_roi:  list[float] = []

        for row in resolved_rows:
            try:
                bd = json.loads(row["score_breakdown_json"])
            except (ValueError, TypeError, KeyError):
                continue

            component_score = bd.get(key)
            # Backward compat: try fallback key for rows written before a rename
            if component_score is None and fallback_key:
                component_score = bd.get(fallback_key)
            if component_score is None:
                component_score = 0

            roi = row.get("roi")
            status = row.get("resolution_status", "")
            is_won = status == "resolved_won"

            if component_score > threshold:
                high_total += 1
                if is_won:
                    high_wins += 1
                if roi is not None:
                    high_roi.append(roi)
            else:
                low_total += 1
                if is_won:
                    low_wins += 1
                if roi is not None:
                    low_roi.append(roi)

        results.append({
            "label":      label,
            "high_count": high_total,
            "high_wins":  high_wins,
            "high_roi":   sum(high_roi) / len(high_roi) if high_roi else None,
            "low_count":  low_total,
            "low_wins":   low_wins,
            "low_roi":    sum(low_roi) / len(low_roi) if low_roi else None,
        })

    return results


# ---------------------------------------------------------------------------
# Main output
# ---------------------------------------------------------------------------

def print_stats(since_days: Optional[int], db_path: Optional[str] = None) -> None:
    database.init_db(db_path=db_path)

    since_ts: Optional[int] = None
    if since_days is not None:
        since_ts = int(time.time()) - since_days * 86400

    stats = database.get_outcome_stats(since_timestamp=since_ts)

    total    = stats["total"]
    resolved = stats["resolved"]
    pending  = stats["pending"]
    invalid  = stats["invalid"]
    wins     = stats["wins"]
    losses   = stats["losses"]
    avg_roi  = stats["avg_roi"]
    total_roi = stats["total_roi"]
    buckets  = stats["score_buckets"]
    resolved_rows = stats["resolved_rows"]
    latency_rows  = stats["latency_rows"]

    window_str = f"last {since_days} days" if since_days else "all time"

    print()
    print("=== Polymarket Signal Bot — Outcome Stats ===")
    print(f"Window: {window_str}")
    print()
    print(f"Total alerts fired:        {total}")
    print(f"Resolved:                  {resolved}  ({_pct(resolved, total)})")
    print(f"Pending:                   {pending}")
    print(f"Invalid/voided:            {invalid}")

    if resolved < _MIN_RESOLVED_FOR_STATS:
        print()
        print(
            f"Not enough resolved alerts yet for meaningful stats "
            f"(need at least {_MIN_RESOLVED_FOR_STATS}, have {resolved})."
        )
        print()
        return

    # --- Hit rate & ROI ---
    print()
    print("--- Hit rate & ROI (resolved alerts only) ---")
    print(f"Wins:                      {wins}  ({_pct(wins, resolved)})")
    print(f"Losses:                    {losses}  ({_pct(losses, resolved)})")
    print(f"Average ROI per alert:     {_roi_str(avg_roi)}")
    print(f"Total simulated ROI:       {_roi_str(total_roi)}")

    # --- Score buckets ---
    _MIN_RESOLVED_FOR_BUCKET = 5

    print()
    print("--- ROI by score bucket ---")
    for b in buckets:
        if b["count"] == 0:
            continue
        b_resolved = b["resolved"]
        b_total = b["count"]
        b_wins = b["wins"]
        b_roi = _roi_str(b["avg_roi"])
        if b_resolved < _MIN_RESOLVED_FOR_BUCKET:
            print(
                f"Score {b['label']:7s}  {b_total:4d} alerts  "
                f"(insufficient data — {b_resolved} resolved)"
            )
        else:
            b_win_pct = _pct(b_wins, b_resolved)
            print(
                f"Score {b['label']:7s}  {b_resolved}/{b_total} resolved,  "
                f"win rate {b_win_pct:4s},  avg ROI {b_roi}"
            )

    # --- Resolution speed ---
    _LATENCY_BUCKETS = [
        ("< 24h",    0,    24),
        ("1-3 days", 24,   72),
        ("3-7 days", 72,   168),
        ("7+ days",  168,  float("inf")),
    ]
    _MIN_LATENCY_FOR_SECTION = 10

    if len(latency_rows) >= _MIN_LATENCY_FOR_SECTION:
        print()
        print("--- ROI by resolution speed ---")
        for label, lo, hi in _LATENCY_BUCKETS:
            bucket_rows = [
                r for r in latency_rows
                if lo <= r["resolution_latency_hours"] < hi
            ]
            if not bucket_rows:
                continue
            b_wins = sum(1 for r in bucket_rows if r["resolution_status"] == "resolved_won")
            b_roi_vals = [r["roi"] for r in bucket_rows if r["roi"] is not None]
            b_avg_roi = sum(b_roi_vals) / len(b_roi_vals) if b_roi_vals else None
            print(
                f"{label:<10}  {len(bucket_rows):4d} alerts,  "
                f"win rate {_pct(b_wins, len(bucket_rows)):4s},  "
                f"avg ROI {_roi_str(b_avg_roi)}"
            )

    # --- Component correlation ---
    if not resolved_rows:
        return

    component_data = _component_stats(resolved_rows)

    print()
    print("--- ROI by score component (correlation analysis) ---")
    print(
        f"{'Component':<16} | {'High-bucket alerts':>18} | "
        f"{'Win rate':>8} | {'Avg ROI':>7}"
    )
    print("-" * 60)
    for c in component_data:
        if c["high_count"] == 0:
            continue
        print(
            f"{_col(c['label'], 16)} | "
            f"{c['high_count']:>18}  | "
            f"{_pct(c['high_wins'], c['high_count']):>8} | "
            f"{_roi_str(c['high_roi']):>7}"
        )

    print()

    # --- Convergence summary ---
    convergence_resolved: list[dict] = []
    no_convergence_resolved: list[dict] = []
    for row in resolved_rows:
        try:
            bd = json.loads(row["score_breakdown_json"])
            if bd.get("convergence_bonus", 0) > 0:
                convergence_resolved.append(row)
            else:
                no_convergence_resolved.append(row)
        except (ValueError, TypeError, KeyError):
            no_convergence_resolved.append(row)

    if convergence_resolved:
        print("--- Convergence detection summary ---")
        c_wins = sum(1 for r in convergence_resolved if r["resolution_status"] == "resolved_won")
        c_roi_vals = [r["roi"] for r in convergence_resolved if r.get("roi") is not None]
        c_avg_roi = sum(c_roi_vals) / len(c_roi_vals) if c_roi_vals else None
        nc_wins = sum(1 for r in no_convergence_resolved if r["resolution_status"] == "resolved_won")
        nc_roi_vals = [r["roi"] for r in no_convergence_resolved if r.get("roi") is not None]
        nc_avg_roi = sum(nc_roi_vals) / len(nc_roi_vals) if nc_roi_vals else None
        print(
            f"Convergence alerts:     {len(convergence_resolved):4d} resolved  "
            f"win rate {_pct(c_wins, len(convergence_resolved)):4s}  avg ROI {_roi_str(c_avg_roi)}"
        )
        if no_convergence_resolved:
            print(
                f"Non-convergence alerts: {len(no_convergence_resolved):4d} resolved  "
                f"win rate {_pct(nc_wins, len(no_convergence_resolved)):4s}  avg ROI {_roi_str(nc_avg_roi)}"
            )
        print()

    # --- ROI by market category ---
    _CATEGORIES_ORDER = ["crypto", "sports", "politics", "entertainment", "other"]
    category_rows = [r for r in resolved_rows if r.get("market_category") is not None]
    if category_rows:
        print("--- ROI by market category ---")
        for cat in _CATEGORIES_ORDER:
            cat_rows = [r for r in category_rows if r.get("market_category") == cat]
            if len(cat_rows) < 5:
                continue
            cat_wins = sum(1 for r in cat_rows if r["resolution_status"] == "resolved_won")
            cat_roi_vals = [r["roi"] for r in cat_rows if r.get("roi") is not None]
            cat_avg_roi = sum(cat_roi_vals) / len(cat_roi_vals) if cat_roi_vals else None
            print(
                f"{cat:<14}  {len(cat_rows):4d} resolved,  "
                f"win rate {_pct(cat_wins, len(cat_rows)):4s},  avg ROI {_roi_str(cat_avg_roi)}"
            )
        print()

    # --- ROI by price band ---
    _BAND_ORDER = [
        ("longshot",  "longshot  (2-30%)"),
        ("uncertain", "uncertain (30-50%)"),
        ("lean",      "lean      (50-70%)"),
        ("favorite",  "favorite  (70-98%)"),
    ]
    band_rows = [r for r in resolved_rows if r.get("bet_price_band") is not None]
    if band_rows:
        print("--- ROI by price band ---")
        for band_key, band_label in _BAND_ORDER:
            b_rows = [r for r in band_rows if r.get("bet_price_band") == band_key]
            if len(b_rows) < 5:
                continue
            b_wins = sum(1 for r in b_rows if r["resolution_status"] == "resolved_won")
            b_roi_vals = [r["roi"] for r in b_rows if r.get("roi") is not None]
            b_avg_roi = sum(b_roi_vals) / len(b_roi_vals) if b_roi_vals else None
            print(
                f"{band_label:<22}  {len(b_rows):4d} resolved,  "
                f"win rate {_pct(b_wins, len(b_rows)):4s},  avg ROI {_roi_str(b_avg_roi)}"
            )
        print()

    # --- ROI by time of day (UTC, 6-hour blocks) ---
    _TOD_BUCKETS = [
        ("00-05", 0,  6),
        ("06-11", 6,  12),
        ("12-17", 12, 18),
        ("18-23", 18, 24),
    ]
    tod_rows = [r for r in resolved_rows if r.get("trade_hour_utc") is not None]
    if tod_rows:
        print("--- ROI by time of day (UTC) ---")
        for label, lo, hi in _TOD_BUCKETS:
            t_rows = [r for r in tod_rows if lo <= r["trade_hour_utc"] < hi]
            if len(t_rows) < 5:
                continue
            t_wins = sum(1 for r in t_rows if r["resolution_status"] == "resolved_won")
            t_roi_vals = [r["roi"] for r in t_rows if r.get("roi") is not None]
            t_avg_roi = sum(t_roi_vals) / len(t_roi_vals) if t_roi_vals else None
            print(
                f"{label}  {len(t_rows):4d} resolved,  "
                f"win rate {_pct(t_wins, len(t_rows)):4s},  avg ROI {_roi_str(t_avg_roi)}"
            )
        print()

    # --- Contrarian signals ---
    contrarian_rows = [r for r in resolved_rows if r.get("is_contrarian") == 1]
    non_contrarian_rows = [r for r in resolved_rows if r.get("is_contrarian") == 0]
    if contrarian_rows:
        print("--- Contrarian signals ---")
        ct_wins = sum(1 for r in contrarian_rows if r["resolution_status"] == "resolved_won")
        ct_roi_vals = [r["roi"] for r in contrarian_rows if r.get("roi") is not None]
        ct_avg_roi = sum(ct_roi_vals) / len(ct_roi_vals) if ct_roi_vals else None
        print(
            f"Contrarian alerts:     {len(contrarian_rows):4d} resolved  "
            f"win rate {_pct(ct_wins, len(contrarian_rows)):4s}  avg ROI {_roi_str(ct_avg_roi)}"
        )
        if non_contrarian_rows:
            nc_wins = sum(1 for r in non_contrarian_rows if r["resolution_status"] == "resolved_won")
            nc_roi_vals = [r["roi"] for r in non_contrarian_rows if r.get("roi") is not None]
            nc_avg_roi = sum(nc_roi_vals) / len(nc_roi_vals) if nc_roi_vals else None
            print(
                f"Non-contrarian alerts: {len(non_contrarian_rows):4d} resolved  "
                f"win rate {_pct(nc_wins, len(non_contrarian_rows)):4s}  avg ROI {_roi_str(nc_avg_roi)}"
            )
        print()

    print_trade_stats(since_days=since_days, db_path=db_path)


# ---------------------------------------------------------------------------
# Trading bot performance
# ---------------------------------------------------------------------------

_TRADING_STARTING_BANKROLL: float = 100.0  # USDC


def print_trade_stats(since_days: Optional[int] = None, db_path: Optional[str] = None) -> None:
    since_ts: Optional[int] = None
    if since_days is not None:
        since_ts = int(time.time()) - since_days * 86400

    stats = database.get_trade_stats(since_timestamp=since_ts)

    if stats.get("total", 0) == 0:
        print()
        print("=== Trading Bot Performance ===")
        print("No trades executed yet.")
        print()
        return

    total         = stats["total"]
    resolved      = stats["resolved"]
    won           = stats["won"]
    lost          = stats["lost"]
    total_pnl     = stats["total_pnl"]
    avg_pnl       = stats["avg_pnl"]
    avg_slippage  = stats["avg_slippage"]
    max_slippage  = stats["max_slippage"]
    avg_size_usdc = stats.get("avg_size_usdc")

    def _pnl(v: Optional[float]) -> str:
        if v is None:
            return "—"
        sign = "+" if v >= 0 else ""
        return f"${sign}{v:.2f}"

    def _slip(v: Optional[float]) -> str:
        return "—" if v is None else f"{v:.4f}"

    win_rate_str = _pct(won, resolved) if resolved else "—"
    bankroll = _TRADING_STARTING_BANKROLL + (total_pnl or 0.0)

    window_str = f"last {since_days} days" if since_days else "all time"

    # Sizing mode — mirrors the three-state auto-graduation logic in trader.py
    trade_resolved = stats.get("resolved", 0)
    trade_pnl = stats.get("total_pnl") or 0.0
    if trade_resolved < config.TRADING_DYNAMIC_MIN_RESOLVED:
        sizing_mode = (
            f"Warmup — fixed ${config.TRADING_BET_SIZE_USDC:.2f} "
            f"({trade_resolved}/{config.TRADING_DYNAMIC_MIN_RESOLVED} resolved)"
        )
    elif trade_pnl <= 0:
        sizing_mode = (
            f"Fixed ${config.TRADING_BET_SIZE_USDC:.2f} "
            f"(awaiting profitability, P&L ${trade_pnl:+.2f})"
        )
    else:
        sizing_mode = (
            f"Dynamic ({config.TRADING_BET_PERCENTAGE * 100:.0f}% of bankroll, "
            f"${config.TRADING_MIN_BET_USDC:.2f}–${config.TRADING_MAX_BET_USDC:.2f})"
        )

    # Vault sweep stats
    sweep_stats = database.get_vault_sweep_stats()
    sweep_count = sweep_stats["sweep_count"]
    total_swept = sweep_stats["total_swept"]
    if sweep_count > 0:
        sweep_str = f"{sweep_count} sweep{'s' if sweep_count != 1 else ''}, ${total_swept:.2f} total"
    else:
        sweep_str = "none"

    print()
    print("=== Trading Bot Performance ===")
    print(f"Window: {window_str}")
    print()
    print(f"Total trades executed:     {total}")
    print(f"Resolved:                  {resolved}")
    print(f"Won:                       {won}  ({win_rate_str})")
    print(f"Lost:                      {lost}  ({_pct(lost, resolved)})")
    print()
    print(f"Actual P&L:                {_pnl(total_pnl)}")
    print(f"Average P&L per trade:     {_pnl(avg_pnl)}")
    print(f"Win rate:                  {win_rate_str}")
    print()
    print(f"Average slippage:          {_slip(avg_slippage)}")
    print(f"Max slippage:              {_slip(max_slippage)}")
    print()
    print(f"Sizing mode:               {sizing_mode}")
    print(f"Average bet size:          {'$'+f'{avg_size_usdc:.2f}' if avg_size_usdc else '—'}")
    print(f"Bankroll (est.):           ${bankroll:,.2f}  (started at ${_TRADING_STARTING_BANKROLL:.0f})")
    print(f"Vault sweeps:              {sweep_str}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket Signal Bot — Outcome Stats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python stats.py                  # All-time stats
  python stats.py --since-days 30  # Last 30 days
        """,
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=None,
        metavar="N",
        help="Restrict stats to the last N days (default: all time)",
    )
    parser.add_argument(
        "--db",
        default=None,
        metavar="PATH",
        help="Path to SQLite database (default: config.SQLITE_DB_PATH)",
    )
    args = parser.parse_args()
    print_stats(since_days=args.since_days, db_path=args.db)


if __name__ == "__main__":
    main()
