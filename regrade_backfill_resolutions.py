"""
regrade_backfill_resolutions.py — One-off data-healing tool (DRY-RUN by default).

Heals trade_executions rows whose resolution_status/pnl were written incorrectly
by the now-removed on-chain CLOB backfill (held losers booked as wins, etc.).

TRUTH SOURCE: alert_outcomes (graded by resolution_checker from Gamma
outcomePrices, name-matched to bet_side). This is the only reliable grader:
on-chain curPrice does NOT map to the Gamma outcome for negative-risk / "No"-side
markets (cross-checked: 23/80 disagreements, alert_outcomes always right), so the
earlier on-chain approach was discarded.

INPUT: a JSON array of resolved trade_executions joined to alert_outcomes, dumped
from the live DB. Produce it with (run inside the Railway container):

    SELECT te.alert_id, te.bet_side, te.bet_price_filled, te.bet_price_intended,
           te.size_usdc, te.resolution_status AS cur, te.pnl AS cur_pnl,
           te.resolved_at,
           ao.resolution_status AS ao_status, ao.winning_outcome AS ao_winner
    FROM trade_executions te
    LEFT JOIN alert_outcomes ao ON te.alert_id = ao.alert_id
    WHERE te.resolution_status IN ('won','lost');

LOGIC (per row):
  * truth = won/lost/invalid from ao_status IF alert_outcomes is graded.
  * If alert_outcomes is still 'pending' (resolution_checker hasn't graded it) →
    SKIP. We never guess; those heal once Path A grades them.
  * Correct only when truth != current resolution_status.
  * Per-ROW P&L: won -> size*(1/fill-1), lost -> -size, invalid -> 0 (matches
    _check_pending_resolutions; NOT aggregate realizedPnl, which multi-counts).

DRY-RUN unless --apply --yes. Writes via /api/trades/bulk-correction (env creds),
tagged source='reconcile-ao' (excluded from CB seeding).

    python regrade_backfill_resolutions.py --data live_full.json
    set RAILWAY_URL=...; set RAILWAY_API_KEY=...
    python regrade_backfill_resolutions.py --data live_full.json --apply --yes
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request

AO_MAP = {"resolved_won": "won", "resolved_lost": "lost", "resolved_invalid": "invalid"}
CORRECTION_SOURCE = os.getenv("REGRADE_SOURCE", "reconcile-ao")


def build_corrections(rows: list[dict]):
    corrections = []
    stats = {"checked": len(rows), "ao_pending": 0, "agree": 0,
             "won_to_lost": 0, "lost_to_won": 0, "to_invalid": 0, "pnl_delta": 0.0}
    for r in rows:
        truth = AO_MAP.get(r.get("ao_status"))
        if truth is None:
            stats["ao_pending"] += 1
            continue
        cur = r.get("cur")
        if truth == cur:
            stats["agree"] += 1
            continue
        size = r.get("size_usdc") or 2.0
        fill = r.get("bet_price_filled") or r.get("bet_price_intended") or 0.5
        if truth == "won":
            new_pnl = size * (1.0 / fill - 1.0)
        elif truth == "invalid":
            new_pnl = 0.0
        else:
            new_pnl = -size
        stats["pnl_delta"] += new_pnl - (r.get("cur_pnl") or 0.0)
        if cur == "won" and truth == "lost":
            stats["won_to_lost"] += 1
        elif cur == "lost" and truth == "won":
            stats["lost_to_won"] += 1
        else:
            stats["to_invalid"] += 1
        corrections.append({
            "alert_id": r["alert_id"],
            "resolution_status": truth,
            "pnl": round(new_pnl, 6),
            "resolved_at": r.get("resolved_at") or int(time.time()),
            "_cur": cur, "_cur_pnl": r.get("cur_pnl") or 0.0,
            "_side": r.get("bet_side"), "_winner": r.get("ao_winner"),
        })
    return corrections, stats


def apply_corrections(corrections, batch=100):
    base, key = os.getenv("RAILWAY_URL"), os.getenv("RAILWAY_API_KEY")
    if not base or not key:
        sys.exit("--apply needs env RAILWAY_URL and RAILWAY_API_KEY (never hardcode the key).")
    payloads = [{k: c[k] for k in ("alert_id", "resolution_status", "pnl", "resolved_at")} for c in corrections]
    total = 0
    for i in range(0, len(payloads), batch):
        body = json.dumps({"corrections": payloads[i:i + batch], "source": CORRECTION_SOURCE}).encode()
        req = urllib.request.Request(f"{base}/api/trades/bulk-correction", data=body, method="POST",
                                     headers={"X-API-Key": key, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            res = json.loads(resp.read())
        total += res.get("updated", 0)
        print(f"  batch {i // batch + 1}: updated={res.get('updated')} errors={len(res.get('errors', []))}")
    print(f"Applied {total} correction(s), source='{CORRECTION_SOURCE}'.")


def main():
    ap = argparse.ArgumentParser(description="Re-grade backfill resolutions from alert_outcomes (dry-run by default).")
    ap.add_argument("--data", required=True, help="JSON dump of trade_executions joined to alert_outcomes.")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--limit-print", type=int, default=40)
    args = ap.parse_args()

    rows = json.load(open(args.data))
    corrections, st = build_corrections(rows)

    print("=== RE-GRADE (truth = alert_outcomes, side-matched via Gamma) ===")
    print(f"  resolved rows checked : {st['checked']}")
    print(f"  agree (no-op)         : {st['agree']}")
    print(f"  alert_outcomes pending: {st['ao_pending']}  (skipped — heal once Path A grades them)")
    print(f"  corrections           : {len(corrections)}  "
          f"(won->lost {st['won_to_lost']}, lost->won {st['lost_to_won']}, ->invalid {st['to_invalid']})")
    print(f"  net P&L delta         : ${st['pnl_delta']:+.2f}")
    if corrections:
        print(f"\n  showing {min(len(corrections), args.limit_print)}:")
        for c in corrections[:args.limit_print]:
            print(f"   {c['alert_id'][:12]} {c['_cur']:>4}->{c['resolution_status']:<5} "
                  f"pnl {c['_cur_pnl']:+7.2f}->{c['pnl']:+7.2f}  side={c['_side']} winner={c['_winner']}")

    if not corrections:
        print("\nNothing to correct."); return
    if not args.apply:
        print("\nDRY-RUN - nothing written. Re-run with --apply --yes to push corrections."); return
    if not args.yes:
        sys.exit("\nRefusing to write without --yes alongside --apply.")
    print(f"\nApplying {len(corrections)} correction(s)...")
    apply_corrections(corrections)


if __name__ == "__main__":
    main()
