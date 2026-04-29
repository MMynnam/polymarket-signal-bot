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
_SCORE_BUCKET_LABELS = ["60-69", "70-79", "80-89", "90+"]

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
    print()
    print("--- ROI by score bucket ---")
    for b in buckets:
        if b["count"] == 0:
            continue
        b_resolved = sum(
            1 for r in resolved_rows
            if r["resolution_status"] != "pending"
        )
        # Compute bucket-specific win rate from pre-aggregated counts.
        b_wins = b["wins"]
        b_total = b["count"]
        b_win_pct = _pct(b_wins, b_total)
        b_roi = _roi_str(b["avg_roi"])
        print(
            f"Score {b['label']:7s}  {b_total:4d} alerts,  "
            f"win rate {b_win_pct:4s},  avg ROI {b_roi}"
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
