"""
analysis/build_dataset.py
Extracts alert_outcomes from SQLite and writes a flat CSV to stdout.
Requires only stdlib — designed to run on Railway (no pandas/numpy).

Usage:
    python3 analysis/build_dataset.py [/path/to/polymarket_bot.db]
    python3 analysis/build_dataset.py > analysis/dataset.csv

Regime boundaries (UTC unix timestamps, anchored to DB earliest=1777691269=May-2 03:07 UTC):
    A: created_at <  1778106829  (pre May-6 22:33 UTC — bonuses active, win_rate broken)
    B: created_at <  1778728669  (May-6 22:33 → May-14 03:17 UTC — bonuses zeroed, win_rate broken)
    C: created_at >= 1778728669  (post May-14 03:17 UTC — current regime, win_rate working)
"""

import csv
import json
import sqlite3
import sys

DB = sys.argv[1] if len(sys.argv) > 1 else "/data/polymarket_bot.db"
REGIME_A_END = 1778106829   # May 6 22:33 UTC
REGIME_C_START = 1778728669  # May 14 03:17 UTC

BINARY_SIDES = {"Yes", "No"}
DIRECTIONAL_SIDES = {"Up", "Down"}
SPREAD_SIDES = {"Over", "Under"}

def regime(ts):
    if ts < REGIME_A_END:
        return "A"
    if ts < REGIME_C_START:
        return "B"
    return "C"

def market_type(side):
    if side in BINARY_SIDES:
        return "yes_no"
    if side in DIRECTIONAL_SIDES:
        return "up_down"
    if side in SPREAD_SIDES:
        return "over_under"
    return "esports_sports"

def outcome(status):
    if status == "resolved_won":
        return 1
    if status == "resolved_lost":
        return 0
    return ""  # pending

FIELDS = [
    "alert_id", "created_at", "regime",
    # components
    "timing", "wallet_age", "win_rate", "size_anomaly",
    "concentration", "funding_velocity",
    "cluster_bonus", "convergence_bonus",
    # derived
    "bet_side", "market_type",
    "resolution_status", "outcome",
    "roi", "score", "bet_price_at_alert",
    # notes (for qualitative audit)
    "timing_note", "wallet_age_note", "win_rate_note",
    "size_anomaly_note", "concentration_note",
    "funding_velocity_note", "cluster_note", "convergence_note",
    # market context
    "market_question",
]

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, lineterminator="\n")
writer.writeheader()

rows = db.execute("""
    SELECT alert_id, created_at, score, bet_side,
           bet_price_at_alert, resolution_status, roi,
           score_breakdown_json, market_question
    FROM alert_outcomes
    ORDER BY created_at
""").fetchall()

for row in rows:
    ts = row["created_at"]
    try:
        bd = json.loads(row["score_breakdown_json"] or "{}")
    except Exception:
        bd = {}

    rec = {
        "alert_id":          row["alert_id"],
        "created_at":        ts,
        "regime":            regime(ts),
        # components
        "timing":            bd.get("timing", ""),
        "wallet_age":        bd.get("wallet_age", ""),
        "win_rate":          bd.get("win_rate", ""),
        "size_anomaly":      bd.get("size_anomaly", ""),
        "concentration":     bd.get("concentration", ""),
        "funding_velocity":  bd.get("funding_velocity", ""),
        "cluster_bonus":     bd.get("cluster_bonus", ""),
        "convergence_bonus": bd.get("convergence_bonus", ""),
        # derived
        "bet_side":          row["bet_side"] or "",
        "market_type":       market_type(row["bet_side"] or ""),
        "resolution_status": row["resolution_status"] or "",
        "outcome":           outcome(row["resolution_status"] or ""),
        "roi":               row["roi"] if row["roi"] is not None else "",
        "score":             row["score"] or "",
        "bet_price_at_alert": row["bet_price_at_alert"] or "",
        # notes
        "timing_note":           bd.get("timing_note", ""),
        "wallet_age_note":       bd.get("wallet_age_note", ""),
        "win_rate_note":         bd.get("win_rate_note", ""),
        "size_anomaly_note":     bd.get("size_anomaly_note", ""),
        "concentration_note":    bd.get("concentration_note", ""),
        "funding_velocity_note": bd.get("funding_velocity_note", ""),
        "cluster_note":          bd.get("cluster_note", ""),
        "convergence_note":      bd.get("convergence_note", ""),
        "market_question":       (row["market_question"] or "")[:120],
    }
    writer.writerow(rec)

sys.stderr.write(f"Exported {len(rows)} rows.\n")
