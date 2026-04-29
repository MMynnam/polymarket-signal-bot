"""
backfill_outcomes.py — One-time backfill of alert_outcomes from alert_history.

Problem: alert_outcomes was added mid-way through the bot's life. The earlier
170 alert_history rows have no corresponding alert_outcomes rows, so the
resolution checker can't grade them.

This script inserts a pending outcome row for each alert_history entry that
doesn't already have one in alert_outcomes.

Usage:
  python backfill_outcomes.py                          # uses config.SQLITE_DB_PATH
  python backfill_outcomes.py --db polymarket_bot.db   # explicit path
  python backfill_outcomes.py --dry-run                # preview without writing

Run once, then never again. Idempotent: safe to run multiple times (uses
INSERT OR IGNORE so existing rows are never overwritten).

Extraction strategy:
  The alert_text HTML has evolved across three formats. This script tries
  each format in order and falls back to NULL for fields it can't extract.
  A row with NULL bet fields is still useful — the resolution checker can
  grade it as resolved_invalid (voided), preserving the market resolution
  result even without per-alert ROI.

  Format A (rows 1–~155, old "Score Breakdown" bullets):
    <b>Outcome bet:</b> YES @ $0.28 (28% implied underdog)
    <b>Position size:</b> $1,000
    <b>Market:</b> Market question here

  Format B (rows ~155–165, intermediate, same patterns as A):
    identical extraction to Format A

  Format C (rows ~166–170, "Bet:" line with cents):
    <b>Bet:</b> YES @ 0c  |  <b>$537</b>
    (price in cents; 0c means <0.5% so we recover from payout line)
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from typing import Optional

import config  # noqa: F401 — loads .env as side-effect; provides SQLITE_DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("backfill")


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

# Compiled regex patterns for each alert format.

# Format A/B: "Outcome bet:" line
_RE_OUTCOME_BET = re.compile(
    r'<b>Outcome bet:</b>\s*([^\s@<]+(?:\s[^\s@<]+)*?)\s*@\s*\$([\d.]+)',
    re.IGNORECASE,
)
_RE_POSITION_SIZE = re.compile(
    r'<b>Position size:</b>\s*\$([\d,]+(?:\.\d+)?)',
    re.IGNORECASE,
)

# Format C: "Bet:" line (cents format, may use | or • as separator)
_RE_BET_CENTS = re.compile(
    r'<b>Bet:</b>\s*([^\s@<]+(?:\s[^\s@<]+)*?)\s*@\s*(\d+)[c¢]',
    re.IGNORECASE,
)
_RE_BET_SIZE_BOLD = re.compile(
    r'<b>Bet:</b>.*?<b>\$([\d,]+(?:\.\d+)?)</b>',
    re.IGNORECASE | re.DOTALL,
)

# Payout line: "+$531,774 profit (991.7x return) if YES wins"
# Used to recover price when cents format rounds to 0.
_RE_PAYOUT_RETURN = re.compile(
    r'\(([\d.]+)x return\)',
    re.IGNORECASE,
)

# Market title patterns
_RE_MARKET_TAG = re.compile(
    r'<b>Market:</b>\s*(.+?)(?:\n|<)',
    re.IGNORECASE,
)


def _extract_bet_fields(
    alert_text: str,
) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Returns (bet_side, bet_price_at_alert, bet_size_usd).
    Any field that can't be reliably extracted returns None.
    """
    # --- Format A/B: <b>Outcome bet:</b> ---
    m = _RE_OUTCOME_BET.search(alert_text)
    if m:
        side  = m.group(1).strip()
        price = float(m.group(2))
        sm = _RE_POSITION_SIZE.search(alert_text)
        size  = float(sm.group(1).replace(",", "")) if sm else None
        return side, price, size

    # --- Format C: <b>Bet:</b> YES @ 77c --- (cents, possibly unicode ¢)
    m = _RE_BET_CENTS.search(alert_text)
    if m:
        side      = m.group(1).strip()
        cents     = int(m.group(2))
        price     = cents / 100.0

        # If cents == 0 (price < 0.5%), recover from the payout multiplier line.
        if cents == 0:
            pm = _RE_PAYOUT_RETURN.search(alert_text)
            if pm:
                return_x = float(pm.group(1))
                price = 1.0 / (return_x + 1.0) if return_x > 0 else None

        sm = _RE_BET_SIZE_BOLD.search(alert_text)
        size = float(sm.group(1).replace(",", "")) if sm else None
        return side, price, size

    return None, None, None


def _extract_market_question(
    alert_text: str,
    market_id: str,
    db: sqlite3.Connection,
) -> Optional[str]:
    """
    Try to get the market title. Priority:
    1. Local markets table (most reliable).
    2. <b>Market:</b> tag in alert_text.
    3. Return None (caller should use market_id as fallback).
    """
    row = db.execute(
        "SELECT title FROM markets WHERE condition_id = ?", (market_id,)
    ).fetchone()
    if row and row[0]:
        return row[0]

    m = _RE_MARKET_TAG.search(alert_text)
    if m:
        return m.group(1).strip()

    return None


# ---------------------------------------------------------------------------
# Main backfill logic
# ---------------------------------------------------------------------------

def backfill(db_path: str, dry_run: bool = False) -> None:
    log.info("Opening database: %s", db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Fetch all alert_history rows.
    ah_rows = conn.execute(
        "SELECT * FROM alert_history ORDER BY created_at ASC"
    ).fetchall()
    log.info("alert_history: %d total rows", len(ah_rows))

    # Build set of already-existing alert_outcomes alert_ids.
    existing_ids: set[str] = {
        r[0] for r in conn.execute("SELECT alert_id FROM alert_outcomes").fetchall()
    }
    log.info("alert_outcomes: %d existing rows", len(existing_ids))

    backfilled = 0
    skipped    = 0
    failed     = 0

    for row in ah_rows:
        d = dict(row)
        trade_id = d["trade_id"]

        if trade_id in existing_ids:
            skipped += 1
            continue

        alert_text = d.get("alert_text", "") or ""
        market_id  = d.get("market_id", "") or ""

        # --- Extract bet fields ---
        bet_side, bet_price, bet_size = _extract_bet_fields(alert_text)

        if bet_side is None or bet_price is None or bet_size is None:
            log.warning(
                "Extraction incomplete for trade %s (side=%s price=%s size=%s) "
                "— inserting with NULL fields",
                trade_id, bet_side, bet_price, bet_size,
            )
            failed += 1  # Count as failed-extraction but still insert

        # --- Market question ---
        market_question = _extract_market_question(alert_text, market_id, conn)
        if not market_question:
            market_question = market_id  # last resort

        # --- score_breakdown_json ---
        # Use the score_json from alert_history as-is (original breakdown at alert time).
        score_breakdown_json = d.get("score_json") or "{}"

        # --- created_at (INTEGER for alert_outcomes) ---
        raw_ts = d.get("created_at")
        created_at = int(raw_ts) if raw_ts is not None else int(time.time())

        if dry_run:
            log.info(
                "[DRY-RUN] Would insert: trade=%s score=%d market=%r "
                "side=%s price=%s size=%s",
                trade_id, d.get("score", 0),
                (market_question or "")[:60],
                bet_side, bet_price, bet_size,
            )
            backfilled += 1
            continue

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO alert_outcomes
                    (alert_id, created_at, market_id, market_question,
                     wallet_address, score, score_breakdown_json,
                     bet_side, bet_price_at_alert, bet_size_usd,
                     resolution_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    trade_id,
                    created_at,
                    market_id,
                    market_question,
                    (d.get("wallet_address") or "").lower(),
                    d.get("score", 0),
                    score_breakdown_json,
                    bet_side,       # may be None — stored as NULL
                    bet_price,      # may be None — stored as NULL
                    bet_size,       # may be None — stored as NULL
                ),
            )
            conn.commit()
            backfilled += 1
        except Exception as exc:
            log.error("Failed to insert row for trade %s: %s", trade_id, exc)
            failed += 1

    conn.close()

    log.info("")
    log.info("=== Backfill complete ===")
    log.info("Backfilled : %d", backfilled)
    log.info("Skipped    : %d (already in alert_outcomes)", skipped)
    log.info("Failed     : %d (extraction failures — inserted with NULL fields)", failed)
    log.info("")

    print(f"Backfilled {backfilled} rows. Skipped {skipped} (already exist). "
          f"Failed {failed} (extraction errors).")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time backfill of alert_outcomes from alert_history",
    )
    parser.add_argument(
        "--db",
        default=config.SQLITE_DB_PATH,
        metavar="PATH",
        help="Path to the SQLite database (default: from SQLITE_DB_PATH env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be inserted without writing anything",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        log.error("Database file not found: %s", args.db)
        sys.exit(1)

    backfill(db_path=args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
