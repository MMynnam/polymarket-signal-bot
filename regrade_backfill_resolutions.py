"""
regrade_backfill_resolutions.py — One-off data-healing tool (DRY-RUN by default).

WHY THIS EXISTS
---------------
`fly-trader/trader_remote.py::_resolve_from_clob_positions` (the "CLOB backfill")
historically inferred resolution from /v1/positions alone:

    redeemable=True  -> "won"
    absent           -> "lost"

Both are INVERTED (see PR "Fix inverted win/loss in CLOB backfill"):
  * redeemable=True is NOT a win — losing tokens are equally redeemable (burn for $0).
    A resolved position is won/lost by its settle price (curPrice ~1 vs ~0).
  * Auto-redeemed WINNERS leave /v1/positions (tokens burned) and live in
    /v1/closed-positions — so "absent" was mostly *won*, not lost.

As a result `trade_executions` rows written by that path (resolution_source='backfill')
hold inverted resolution_status/pnl, corrupting /api/stats/trading. The code fix stops
new corruption; this script heals the existing rows.

WHAT IT DOES
------------
1. Reads resolved trade_executions from a SQLite copy of the bot DB (--db, read-only).
2. Pulls ground truth from Polymarket for the funder wallet:
     /v1/closed-positions  -> settled & redeemed (realizedPnl = actual cash)
     /v1/positions         -> currently held (open + resolved-but-unredeemed)
3. Classifies each market by SETTLE PRICE only (curPrice >= 0.5 -> won, else lost).
   This is "hard evidence". Markets with no hard evidence (absent from both, or held
   non-redeemable mid-price) are LEFT ALONE — we never re-flip on absence, which is
   exactly the bug we are fixing and also dodges closed-positions API coverage gaps.
4. Proposes a correction only where the stored resolution_status CONTRADICTS hard
   evidence. Correct rows (including correct 'prospective' rows) are untouched.
5. DRY-RUN: prints a full diff + net P&L delta and writes nothing.
   With --apply --yes: POSTs corrections to the live /api/trades/bulk-correction
   (tagged resolution_source via `source`, default 'reconcile-curprice', which keeps
   them out of CB seeding), then verifies via /api/diag/pnl-window.

USAGE
-----
    # Review (writes nothing):
    python regrade_backfill_resolutions.py --db prod_local.db

    # Apply to live Railway DB (requires env RAILWAY_URL + RAILWAY_API_KEY):
    set RAILWAY_URL=https://...railway.app
    set RAILWAY_API_KEY=...           # do NOT hardcode; this is the API secret
    python regrade_backfill_resolutions.py --db prod_local.db --apply --yes

NOTES
-----
* Pull a FRESH DB copy before running so the candidate set matches live. Corrections
  are keyed by alert_id and are idempotent (re-applying the same verdict is a no-op).
* Reads creds from env only — unlike the older reconcile_db.py, no secrets in source.
* market_id in trade_executions is the Polymarket conditionId (lower-cased for matching).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DATA_API = os.getenv("POLYMARKET_DATA_API", "https://data-api.polymarket.com")
WALLET = os.getenv("TRADING_FUNDER_ADDRESS", "0x00BD1F45caAFd08a1FFfEABa7e17c712a8791e9E")

# A resolved outcome settles to ~1 (won) or ~0 (lost). Decisive midpoint.
WIN_PRICE = 0.5
# Correction tag — anything other than 'prospective'/NULL is excluded from CB seeding.
CORRECTION_SOURCE = os.getenv("REGRADE_SOURCE", "reconcile-curprice")


# ---------------------------------------------------------------------------
# Polymarket data API (ground truth)
# ---------------------------------------------------------------------------

def _polymarket_get(path: str, params: dict) -> list:
    url = f"{DATA_API}{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if isinstance(data, dict):
        data = data.get("data") or data.get("positions") or []
    return data if isinstance(data, list) else []


def _fetch_paginated(path: str, page_size: int = 50, hard_cap: int = 5000) -> dict[str, dict]:
    """Return {conditionId(lower): position} across all pages."""
    out: dict[str, dict] = {}
    offset = 0
    while offset < hard_cap:
        page = _polymarket_get(path, {"user": WALLET, "limit": page_size, "offset": offset})
        if not page:
            break
        for pos in page:
            cid = (pos.get("conditionId") or "").lower().strip()
            if cid:
                out[cid] = pos
        if len(page) < page_size:
            break
        offset += page_size
    return out


def classify(condition_id: str, positions: dict, closed: dict) -> str | None:
    """
    Return the verdict using SETTLE PRICE as the only decider.
      'won' | 'lost' | None   (None = no hard evidence, do not touch)

    NOTE on P&L: callers compute P&L PER ROW as size*(1/fill-1) for a win and
    -size for a loss. We deliberately do NOT use closed-positions `realizedPnl`
    as the per-row value: realizedPnl is the AGGREGATE for a conditionId, so when
    several trade_executions share a market it would multi-count. Per-row notional
    is correct per row and sums back to realizedPnl (verified by the cross-check
    printed in main()).
    """
    cid = (condition_id or "").lower().strip()

    cp = closed.get(cid)
    if cp is not None:
        cur = float(cp.get("curPrice") or 0.0)
        return "won" if cur >= WIN_PRICE else "lost"

    pos = positions.get(cid)
    if pos is not None and pos.get("redeemable"):
        cur = float(pos.get("curPrice") or 0.0)
        return "won" if cur >= WIN_PRICE else "lost"

    # Held non-redeemable (open / mid-price) OR absent from both -> no hard evidence.
    return None


# ---------------------------------------------------------------------------
# Local DB read (read-only)
# ---------------------------------------------------------------------------

def read_resolved_trades(db_path: str) -> list[dict]:
    if not os.path.exists(db_path):
        sys.exit(f"DB not found: {db_path} (pull a fresh copy of the bot DB and pass --db)")
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    cols = {r[1] for r in con.execute("PRAGMA table_info(trade_executions)")}
    has_source = "resolution_source" in cols
    src_expr = "resolution_source" if has_source else "NULL AS resolution_source"
    rows = con.execute(f"""
        SELECT alert_id, market_id, market_question, bet_side,
               bet_price_filled, bet_price_intended, size_usdc,
               status, resolution_status, pnl, resolved_at, {src_expr}
        FROM trade_executions
        WHERE resolution_status IN ('won','lost')
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Correction builder
# ---------------------------------------------------------------------------

def build_corrections(trades: list[dict], positions: dict, closed: dict) -> tuple[list[dict], dict]:
    corrections: list[dict] = []
    stats = {"checked": 0, "no_evidence": 0, "agree": 0,
             "won_to_lost": 0, "lost_to_won": 0, "pnl_delta": 0.0,
             "by_source": {}}

    for t in trades:
        stats["checked"] += 1
        src = t.get("resolution_source") or "unknown"
        stats["by_source"].setdefault(src, {"agree": 0, "won_to_lost": 0, "lost_to_won": 0, "no_evidence": 0})

        verdict = classify(t["market_id"], positions, closed)
        if verdict is None:
            stats["no_evidence"] += 1
            stats["by_source"][src]["no_evidence"] += 1
            continue

        current = t["resolution_status"]
        if verdict == current:
            stats["agree"] += 1
            stats["by_source"][src]["agree"] += 1
            continue

        # Per-ROW P&L (not aggregate realizedPnl — see classify() docstring).
        size = t.get("size_usdc") or 2.0
        fill = t.get("bet_price_filled") or t.get("bet_price_intended") or 0.5
        new_pnl = size * (1.0 / fill - 1.0) if verdict == "won" else -size

        old_pnl = t.get("pnl") or 0.0
        stats["pnl_delta"] += (new_pnl - old_pnl)
        key = "won_to_lost" if current == "won" else "lost_to_won"
        stats[key] += 1
        stats["by_source"][src][key] += 1

        corrections.append({
            "alert_id": t["alert_id"],
            "resolution_status": verdict,
            "pnl": round(new_pnl, 6),
            "resolved_at": t.get("resolved_at") or int(time.time()),
            "_mid": (t.get("market_id") or "").lower().strip(),
            "_old_status": current,
            "_old_pnl": old_pnl,
            "_source": src,
            "_q": (t.get("market_question") or "")[:55],
        })

    return corrections, stats


# ---------------------------------------------------------------------------
# Live apply (only with --apply --yes)
# ---------------------------------------------------------------------------

def apply_corrections(corrections: list[dict], batch: int = 100) -> None:
    base = os.getenv("RAILWAY_URL")
    key = os.getenv("RAILWAY_API_KEY")
    if not base or not key:
        sys.exit("--apply needs env RAILWAY_URL and RAILWAY_API_KEY (never hardcode the key).")

    payloads = [{k: c[k] for k in ("alert_id", "resolution_status", "pnl", "resolved_at")} for c in corrections]
    total = 0
    for i in range(0, len(payloads), batch):
        chunk = payloads[i:i + batch]
        body = json.dumps({"corrections": chunk, "source": CORRECTION_SOURCE}).encode()
        req = urllib.request.Request(
            f"{base}/api/trades/bulk-correction", data=body, method="POST",
            headers={"X-API-Key": key, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            res = json.loads(resp.read())
        total += res.get("updated", 0)
        print(f"  batch {i // batch + 1}: updated={res.get('updated')} errors={len(res.get('errors', []))}")
    print(f"Applied {total} correction(s) with source='{CORRECTION_SOURCE}'.")

    # Verify
    try:
        req = urllib.request.Request(f"{base}/api/stats/trading", headers={"X-API-Key": key})
        with urllib.request.urlopen(req, timeout=30) as resp:
            print("Post-fix /api/stats/trading:", json.dumps(json.loads(resp.read()), indent=1)[:600])
    except Exception as exc:
        print(f"(verification fetch failed: {exc})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Re-grade inverted backfill resolutions by settle price (dry-run by default).")
    ap.add_argument("--db", default="prod_local.db", help="SQLite copy of the bot DB to read (read-only).")
    ap.add_argument("--apply", action="store_true", help="POST corrections to the live bulk-correction API.")
    ap.add_argument("--yes", action="store_true", help="Required alongside --apply to actually write.")
    ap.add_argument("--limit-print", type=int, default=40, help="Max correction rows to print.")
    args = ap.parse_args()

    print(f"Wallet : {WALLET}")
    print(f"DB     : {args.db} (read-only)")
    print("Fetching ground truth from Polymarket...")
    positions = _fetch_paginated("/v1/positions")
    closed = _fetch_paginated("/v1/closed-positions")
    print(f"  /v1/positions: {len(positions)} | /v1/closed-positions: {len(closed)}")

    trades = read_resolved_trades(args.db)
    print(f"Resolved trade_executions read: {len(trades)}")

    corrections, stats = build_corrections(trades, positions, closed)

    print("\n=== RE-GRADE SUMMARY (hard settle-price evidence only) ===")
    print(f"  checked        : {stats['checked']}")
    print(f"  agree (no-op)  : {stats['agree']}")
    print(f"  no evidence    : {stats['no_evidence']}  (left untouched)")
    print(f"  won  -> lost   : {stats['won_to_lost']}")
    print(f"  lost -> won    : {stats['lost_to_won']}")
    print(f"  net P&L delta  : ${stats['pnl_delta']:+.2f}")
    print("  by resolution_source:")
    for src, d in sorted(stats["by_source"].items()):
        print(f"    {src:12s}  agree={d['agree']:4d}  won->lost={d['won_to_lost']:3d}  "
              f"lost->won={d['lost_to_won']:3d}  no_evidence={d['no_evidence']:4d}")

    if corrections:
        print(f"\n=== PROPOSED CORRECTIONS ({len(corrections)}, showing {min(len(corrections), args.limit_print)}) ===")
        for c in corrections[:args.limit_print]:
            print(f"  {c['alert_id'][:12]}  {c['_old_status']:>4}->{c['resolution_status']:<4} "
                  f"pnl {c['_old_pnl']:+7.2f}->{c['pnl']:+7.2f}  [{c['_source']}]  {c['_q']}")

        # Cross-check: summed per-row notional for corrected WINS should match the
        # on-chain aggregate realizedPnl of those markets — confirms no multi-counting
        # when several trade rows share one conditionId.
        won_corr = [c for c in corrections if c["resolution_status"] == "won"]
        if won_corr:
            mids = {c["_mid"] for c in won_corr}
            notional_sum = sum(c["pnl"] for c in won_corr)
            realized_sum = sum(float((closed.get(m) or {}).get("realizedPnl") or 0.0) for m in mids)
            print(f"\nCross-check (corrected wins): per-row notional ${notional_sum:+.2f} "
                  f"vs on-chain realizedPnl ${realized_sum:+.2f} over {len(mids)} distinct market(s)")

    if not corrections:
        print("\nNo corrections needed — stored resolutions agree with on-chain settle prices.")
        return

    if not args.apply:
        print("\nDRY-RUN — nothing written. Re-run with --apply --yes to push via /api/trades/bulk-correction.")
        return

    if not args.yes:
        sys.exit("\nRefusing to write without --yes alongside --apply.")

    print(f"\nApplying {len(corrections)} correction(s) to live Railway DB...")
    apply_corrections(corrections)


if __name__ == "__main__":
    main()
