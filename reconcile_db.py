"""
reconcile_db.py — Cross-reference Railway DB trade_executions against on-chain truth.

Ground truth sources (in priority order):
  1. Polymarket /v1/positions  (current open/redeemable)
  2. Polymarket /v1/closed-positions  (historically redeemed)
  3. CSV transaction history  (authoritative but file-based)

Logic per DB "won" trade:
  - If conditionId in closed-positions  → confirmed won ✓
  - If conditionId in positions (redeemable=True) → confirmed won (unredeemed) ✓
  - Neither → mislabeled, fix to lost ✗

Logic per DB "lost" trade (sanity-check only):
  - If conditionId in closed-positions → mislabeled, fix to won (rare edge case)

Outputs a JSON patch list and calls /api/trades/bulk-correction on Railway.
"""

import json
import os
import time
import urllib.request
import urllib.parse

from onchain_match import is_side_ambiguous

RAILWAY_URL = os.getenv("RAILWAY_API_URL", "https://polymarket-signal-bot-production-d248.up.railway.app")
RAILWAY_KEY = os.environ["API_SECRET_KEY"]   # set in .env / environment; never hardcode
DATA_API    = "https://data-api.polymarket.com"
WALLET      = "0x00BD1F45caAFd08a1FFfEABa7e17c712a8791e9E"

CSV_PATH    = r"C:\Users\manny\Downloads\ChatExport_2026-05-20\Polymarket-History-2026-05-20.csv"


def _api_get(url, headers=None):
    h = {"X-API-Key": RAILWAY_KEY}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _api_post(url, payload, headers=None):
    data = json.dumps(payload).encode()
    h = {"X-API-Key": RAILWAY_KEY, "Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _polymarket_get(path, params=None):
    url = f"{DATA_API}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def load_csv_redeems():
    """Return set of marketName values that have at least one Redeem in the CSV."""
    import csv, codecs
    redeemed = set()
    with open(CSV_PATH, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('action') == 'Redeem':
                redeemed.add(row['marketName'].strip())
    return redeemed


def main():
    print("=== DB Reconciliation ===")
    print()

    # 1. Get all filled trades from Railway
    print("Fetching all filled trades from Railway...")
    trades = _api_get(f"{RAILWAY_URL}/api/trades/all-filled")
    print(f"  Got {len(trades)} trade_executions")

    # 2. Get current /v1/positions (redeemable=True or False)
    print("Fetching /v1/positions from Polymarket...")
    positions_raw = _polymarket_get("/v1/positions", {"user": WALLET, "limit": 500})
    if isinstance(positions_raw, dict):
        positions_raw = positions_raw.get("data") or positions_raw.get("positions") or []
    on_chain_positions = {}
    for pos in positions_raw:
        cid = (pos.get("conditionId") or "").lower().strip()
        if cid:
            on_chain_positions[cid] = pos
    print(f"  Got {len(on_chain_positions)} positions (conditionId keys)")

    # 3. Get /v1/closed-positions (confirmed redeemed = confirmed won)
    print("Fetching /v1/closed-positions from Polymarket (paginated)...")
    closed_positions = {}
    offset = 0
    while True:
        page = _polymarket_get("/v1/closed-positions", {"user": WALLET, "limit": 50, "offset": offset})
        if isinstance(page, dict):
            page = page.get("data") or page.get("positions") or []
        if not page:
            break
        for pos in page:
            cid = (pos.get("conditionId") or "").lower().strip()
            if cid:
                closed_positions[cid] = pos
        if len(page) < 50:
            break
        offset += 50
    print(f"  Got {len(closed_positions)} closed positions (redeemed = confirmed won)")

    # 4. Load CSV redeems as a secondary source (market name based)
    print("Loading CSV redeemed markets...")
    csv_redeemed_names = load_csv_redeems()
    print(f"  {len(csv_redeemed_names)} distinct markets redeemed in CSV")

    # 5. Analyse discrepancies
    print()
    print("=== Analysis ===")

    won_db = [t for t in trades if t['resolution_status'] == 'won']
    lost_db = [t for t in trades if t['resolution_status'] == 'lost']
    pending_db = [t for t in trades if t['resolution_status'] == 'pending']

    print(f"DB state: {len(won_db)} won, {len(lost_db)} lost, {len(pending_db)} pending")
    print()

    # Check market_id format
    sample_ids = [t['market_id'] for t in trades[:5]]
    print(f"Sample DB market_ids: {sample_ids}")
    print(f"Sample closed-pos conditionIds: {list(closed_positions.keys())[:3]}")
    print(f"Sample positions conditionIds:  {list(on_chain_positions.keys())[:3]}")
    print()

    # 6. Identify wrong "won" entries
    # Only use on-chain sources (CSV name match is too imprecise — redeems lack tokenName).
    wrong_won = []
    correct_won_closed = []   # confirmed via /v1/closed-positions
    correct_won_redeemable = []  # confirmed via /v1/positions redeemable=True
    correct_won_open = []     # in /v1/positions but redeemable=False (still open? mislabeled won)
    unverifiable = []         # market_id not in any on-chain source

    for t in won_db:
        market_id = (t['market_id'] or "").lower().strip()
        if market_id in closed_positions:
            correct_won_closed.append(t)
        elif market_id in on_chain_positions:
            pos = on_chain_positions[market_id]
            if pos.get("redeemable"):
                correct_won_redeemable.append(t)
            else:
                # In /v1/positions but NOT redeemable = still open or resolved lost
                # If curPrice == 0 and redeemable=False, this is a lost position
                cur_price = pos.get("curPrice", -1)
                if cur_price == 0:
                    wrong_won.append(('still_open_zero_price', t))
                else:
                    correct_won_open.append(t)  # genuinely still open, watch
        else:
            # Not in any current on-chain source.
            # This could mean: resolved as lost (no longer held), or a very old redeem
            # not returned by the closed-positions API.
            # CSV name-match is a last-resort tiebreaker — but ONLY for single-side markets.
            # For side-ambiguous titles (binary up/down, spreads, totals, handicaps) one name
            # maps to OPPOSITE sides, so a redeem on the name can't tell which side won. Those
            # must be judged by token id, not name; here we leave them unverifiable rather than
            # mislabel. (See onchain_match.py.)
            mq = (t.get('market_question') or "").strip()
            if is_side_ambiguous(mq):
                unverifiable.append(t)  # needs token-id join; name evidence is unreliable here
            elif mq in csv_redeemed_names:
                correct_won_closed.append(t)  # CSV name-match confirms redeemed (clean single-side market)
            else:
                wrong_won.append(('no_on_chain_evidence', t))

    # 7. Identify wrong "lost" entries (sanity check)
    wrong_lost = []
    for t in lost_db:
        market_id = (t['market_id'] or "").lower().strip()
        if market_id in closed_positions:
            wrong_lost.append(t)

    print("Won analysis:")
    print(f"  Confirmed via /v1/closed-positions:  {len(correct_won_closed)}")
    print(f"  Confirmed via /v1/positions (redeem): {len(correct_won_redeemable)}")
    print(f"  In /v1/positions but still open:      {len(correct_won_open)}")
    print(f"  Incorrectly labeled (no evidence):    {len([w for tag, w in wrong_won if tag == 'no_on_chain_evidence'])}")
    print(f"  In positions with curPrice=0:         {len([w for tag, w in wrong_won if tag == 'still_open_zero_price'])}")
    print(f"  Unverifiable (side-ambiguous, needs token-id join): {len(unverifiable)}")
    wrong_won_trades = [t for tag, t in wrong_won]
    print()
    print("Lost analysis:")
    print(f"  Confirmed lost (no chain evidence): {len(lost_db) - len(wrong_lost)}")
    print(f"  Incorrectly labeled lost→won:       {len(wrong_lost)}")

    if wrong_won_trades:
        print()
        print("=== Wrong 'won' entries to correct ===")
        for t in wrong_won_trades[:25]:
            print(f"  alert={t['alert_id'][:12]} side={t['bet_side']:4s} q={t['market_question'][:55]}")

    if wrong_lost:
        print()
        print("=== Wrong 'lost' entries to correct ===")
        for t in wrong_lost[:10]:
            print(f"  alert={t['alert_id'][:12]} q={t['market_question'][:50]}")

    # 8. Build corrections
    corrections = []
    now_ts = int(time.time())
    total_pnl_adjustment = 0.0

    for t in wrong_won_trades:
        # Change won → lost, recalculate pnl
        size = t.get('size_usdc') or 2.0
        new_pnl = -size
        old_pnl = t.get('pnl') or 0.0
        total_pnl_adjustment += (new_pnl - old_pnl)
        corrections.append({
            "alert_id": t['alert_id'],
            "resolution_status": "lost",
            "pnl": new_pnl,
            "resolved_at": t.get('resolved_at') or now_ts,
        })

    for t in wrong_lost:
        # Change lost → won, recalculate pnl
        size = t.get('size_usdc') or 2.0
        fill_price = t.get('bet_price_filled') or t.get('bet_price_intended') or 0.5
        new_pnl = size * (1.0 / fill_price - 1.0)
        old_pnl = t.get('pnl') or 0.0
        total_pnl_adjustment += (new_pnl - old_pnl)
        corrections.append({
            "alert_id": t['alert_id'],
            "resolution_status": "won",
            "pnl": new_pnl,
            "resolved_at": t.get('resolved_at') or now_ts,
        })

    print()
    print(f"Total corrections: {len(corrections)}")
    if total_pnl_adjustment != 0:
        print(f"P&L adjustment: ${total_pnl_adjustment:+.2f}")
        print(f"Estimated post-fix cumulative_pnl: ${159.93 + total_pnl_adjustment:.2f}")
    print()

    if not corrections:
        print("No corrections needed (or market_id format mismatch prevented matching).")
        print("Run again after verifying market_id format.")
        return

    # 9. Apply corrections
    print("Applying corrections via /api/trades/bulk-correction ...")
    result = _api_post(
        f"{RAILWAY_URL}/api/trades/bulk-correction",
        {"corrections": corrections, "source": "reconcile"},
    )
    print(f"Result: {result}")
    print()

    # 10. Verify post-fix numbers
    print("Post-fix verification:")
    key = RAILWAY_KEY
    verify_url = f"{RAILWAY_URL}/api/diag/pnl-window?from_ts=1778544000&until_ts=1779235200"
    req = urllib.request.Request(verify_url, headers={"X-API-Key": key})
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read())
    s = result.get('summary', {})
    print(f"  total: {s.get('total')}, resolved: {s.get('resolved')}")
    print(f"  won: {s.get('won')}, lost: {s.get('lost')}, pending: {s.get('still_pending')}")
    wr = s.get('won', 0) / max(s.get('resolved', 1), 1) * 100
    print(f"  WR: {wr:.1f}%")
    print(f"  cumulative_pnl: ${s.get('db_pnl'):.2f}")
    print(f"  CSV ground truth: ~52% WR, -$35.04 net")


if __name__ == "__main__":
    main()
