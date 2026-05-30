"""
heal_resolution_stragglers.py — LOCAL one-shot reconciliation check (DRY-RUN by default).

Runs FROM YOUR MACHINE (needs the `railway` CLI logged in + linked to the
polymarket-signal-bot service). It shells into the Railway container over
`railway ssh`, so it canNOT run as a remote cloud routine — the cloud sandbox has
no Railway auth. That's why this is a local script you run by hand.

What it does (all read-only unless --apply):
  1. Reports the pending alert_outcomes backlog (should stay well below the
     ~2,725 that triggered the FIFO starvation).
  2. Re-grades resolved trade_executions against alert_outcomes (Gamma, side-
     matched — the ONLY reliable grader; on-chain curPrice mis-grades neg-risk /
     "No" markets). Corrects a row only when alert_outcomes is graded AND
     disagrees; skips ao-pending rows (they heal once Path A grades them).
  3. With --apply, writes corrections in-container (tagged
     resolution_source='reconcile-ao', kept out of CB seeding), then reports the
     post-heal resolved P&L + win rate.

Usage:
    python heal_resolution_stragglers.py            # dry-run: show backlog + proposed corrections
    python heal_resolution_stragglers.py --apply --yes   # apply the corrections

You're done (stop checking) when: pending backlog is small/steady AND a dry-run
shows 0 corrections (only genuinely-open markets remain pending).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time

RAILWAY = os.getenv("RAILWAY_BIN") or shutil.which("railway") or "railway"
DB = "/data/polymarket_bot.db"

_DUMP_SRC = f"""
import sqlite3, json, sys
c = sqlite3.connect("file:{DB}?mode=ro", uri=True)
c.row_factory = sqlite3.Row
pend = c.execute("SELECT COUNT(*) FROM alert_outcomes WHERE resolution_status='pending' OR resolution_status IS NULL").fetchone()[0]
rows = [dict(r) for r in c.execute(
    "SELECT te.alert_id, te.bet_side, te.bet_price_filled, te.bet_price_intended, te.size_usdc, "
    "te.resolution_status AS cur, te.pnl AS cur_pnl, te.resolved_at, "
    "ao.resolution_status AS ao_status, ao.winning_outcome AS ao_winner "
    "FROM trade_executions te LEFT JOIN alert_outcomes ao ON te.alert_id=ao.alert_id "
    "WHERE te.resolution_status IN ('won','lost')")]
totals = [list(t) for t in c.execute(
    "SELECT resolution_status, COUNT(*), ROUND(COALESCE(SUM(pnl),0),2) FROM trade_executions GROUP BY resolution_status")]
sys.stdout.write(json.dumps({{"pending_alert_outcomes": pend, "rows": rows, "totals": totals}}))
"""

_APPLY_TMPL = """
import sqlite3, json, base64
data = json.loads(base64.b64decode("{corr_b64}"))
c = sqlite3.connect("{db}", timeout=120)
n = 0
for d in data:
    n += c.execute("UPDATE trade_executions SET resolution_status=?, pnl=?, resolved_at=?, resolution_source=? WHERE alert_id=?",
                   (d["status"], d["pnl"], d["resolved_at"], "reconcile-ao", d["alert_id"])).rowcount
c.commit(); c.close()
print(json.dumps({{"updated": n, "of": len(data)}}))
"""

AO_MAP = {"resolved_won": "won", "resolved_lost": "lost", "resolved_invalid": "invalid"}


def ssh_python(src: str) -> str:
    """Run a Python snippet inside the Railway container and return its stdout."""
    b64 = base64.b64encode(src.encode()).decode()
    cmd = [RAILWAY, "ssh", f"echo {b64} | base64 -d | python"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    out = proc.stdout.strip()
    if not out:
        sys.exit(f"railway ssh returned no output (is the CLI logged in + linked?).\nstderr: {proc.stderr[-500:]}")
    return out


def compute(rows: list[dict]):
    corrections, agree, pending = [], 0, 0
    for r in rows:
        truth = AO_MAP.get(r.get("ao_status"))
        if truth is None:
            pending += 1
            continue
        if truth == r.get("cur"):
            agree += 1
            continue
        size = r.get("size_usdc") or 2.0
        fill = r.get("bet_price_filled") or r.get("bet_price_intended") or 0.5
        pnl = (size * (1.0 / fill - 1.0)) if truth == "won" else (0.0 if truth == "invalid" else -size)
        corrections.append({"alert_id": r["alert_id"], "status": truth, "pnl": round(pnl, 6),
                            "resolved_at": r.get("resolved_at") or int(time.time()),
                            "_cur": r.get("cur"), "_side": r.get("bet_side"), "_winner": r.get("ao_winner")})
    return corrections, agree, pending


def show_totals(totals):
    d = {t[0]: (t[1], t[2]) for t in totals}
    won, lost = d.get("won", (0, 0)), d.get("lost", (0, 0))
    resolved = won[0] + lost[0]
    net = round(won[1] + lost[1], 2)
    wr = (100.0 * won[0] / resolved) if resolved else 0.0
    print(f"  resolved net P&L: ${net:+.2f}  |  win rate: {won[0]}/{resolved} = {wr:.1f}%  |  pending trades: {d.get('pending',(0,))[0]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--yes", action="store_true")
    args = ap.parse_args()

    print(f"Using railway: {RAILWAY}")
    dump = json.loads(ssh_python(_DUMP_SRC))
    print(f"\nPending alert_outcomes backlog: {dump['pending_alert_outcomes']}  (was ~2725 at the starvation peak)")
    print("Current resolved totals:")
    show_totals(dump["totals"])

    corrections, agree, pending = compute(dump["rows"])
    print(f"\nRe-grade vs alert_outcomes: {agree} agree, {pending} ao-pending (skipped), {len(corrections)} to correct")
    for c in corrections[:50]:
        print(f"   {c['alert_id'][:12]} {c['_cur']}->{c['status']}  pnl ->{c['pnl']:+.2f}  side={c['_side']} winner={c['_winner']}")

    if not corrections:
        clean = dump["pending_alert_outcomes"] < 200
        print("\nNo corrections needed." + ("  Backlog drained — you can stop checking." if clean else
              "  (Backlog still elevated; check again later.)"))
        return
    if not args.apply:
        print("\nDRY-RUN - nothing written. Re-run with --apply --yes to heal these.")
        return
    if not args.yes:
        sys.exit("\nRefusing to write without --yes alongside --apply.")

    corr_b64 = base64.b64encode(json.dumps(
        [{k: c[k] for k in ("alert_id", "status", "pnl", "resolved_at")} for c in corrections]).encode()).decode()
    res = json.loads(ssh_python(_APPLY_TMPL.format(corr_b64=corr_b64, db=DB)))
    print(f"\nApplied: {res['updated']} of {res['of']}")
    print("Post-heal resolved totals:")
    show_totals(json.loads(ssh_python(_DUMP_SRC))["totals"])


if __name__ == "__main__":
    main()
