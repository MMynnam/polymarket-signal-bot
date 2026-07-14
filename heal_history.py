"""
heal_history.py — one-off: rewrite trade_executions resolution labels + pnl from ON-CHAIN truth.

Why: the 2026-06-11 audit found 25% of DB resolution labels inverted vs chain truth and pnl
computed off share-counts (size_usdc units bug). This corrects every filled row whose token is
chain-resolved, using:
  status: token in closed-positions -> won | dead redeemable -> lost | 50/50 void -> invalid
  pnl:    pro-rata share of the token's TRUE cash pnl (returned - cost), split across the
          row's fills by each fill's actual cash (sums exactly to the wallet's realized P&L)
  resolved_at: last redeem ts for winners; position endDate for losers (fallback: keep/now)

Tag: resolution_source='reconcile-chain' (excluded from CB seeding via prospective_only).
Run: python heal_history.py          (dry-run, prints summary)
     python heal_history.py APPLY    (posts to /api/trades/bulk-correction in batches)
"""
import json, os, sys, time, urllib.request
from collections import defaultdict

RAILWAY_URL = os.getenv("RAILWAY_API_URL", "https://polymarket-signal-bot-production-d248.up.railway.app")
RAILWAY_KEY = os.environ["API_SECRET_KEY"]   # set in .env / environment; never hardcode

L = json.load(open("ledger.json"))
D = json.load(open("fresh_dump.json"))

trades = [a for a in L["activity"] if a["type"] == "TRADE"]
redeems = [a for a in L["activity"] if a["type"] == "REDEEM"]
closed_by_asset = {c["asset"]: c for c in L["closed"]}
dead_by_asset = {q["asset"]: q for q in L["positions"] if q.get("redeemable")}
live_assets = {q["asset"] for q in L["positions"] if not q.get("redeemable")}

cost_by_asset = defaultdict(float)
for t in trades:
    cost_by_asset[t["asset"]] += float(t["usdcSize"])
red_by_cid = defaultdict(float); red_ts = {}
for r in redeems:
    red_by_cid[r["conditionId"]] += float(r["usdcSize"])
    red_ts[r["conditionId"]] = max(red_ts.get(r["conditionId"], 0), r["timestamp"])

# void = token "won" (cash arrived) but payout < $0.95/share (50/50 refund)  [3 known]
def token_truth(asset):
    if asset in closed_by_asset:
        cid = closed_by_asset[asset]["conditionId"]
        ret = red_by_cid.get(cid, 0.0)
        cost = cost_by_asset.get(asset, 0.0)
        per_cost = ret / cost if cost else 0
        # winners pay 1/px per $; voids pay ~0.5/share. Use CLOB winner flag via cache:
        return ("won", ret, red_ts.get(cid, 0))
    if asset in dead_by_asset:
        q = dead_by_asset[asset]
        ts = q.get("endDate")
        if isinstance(ts, str):
            try: ts = int(time.mktime(time.strptime(ts[:10], "%Y-%m-%d"))) + 43200
            except Exception: ts = 0
        return ("lost", 0.0, ts or 0)
    return ("pending", 0.0, 0)

clob = json.load(open("clob_markets.json"))
void_assets = set()
for cid, m in clob.items():
    if m.get("error"): continue
    toks = m.get("tokens") or []
    if toks and all(tk.get("winner") is False for tk in toks) and m.get("closed"):
        for tk in toks:
            if str(tk["token_id"]) in closed_by_asset:
                void_assets.add(str(tk["token_id"]))

# per-fill actual cash from the earlier matched dataset
bets = {  # (alert join via tid+ts isn't needed: honest_bets rows are per-te-row in order)
}
hb = json.load(open("honest_bets.json"))
# honest_bets rows align 1:1 with filled te rows that matched activity (all 636)
te_filled = [t for t in D["te"] if t["status"] == "filled"]
assert len(hb) == len(te_filled), (len(hb), len(te_filled))

# group fills by token to pro-rate the token's true pnl
fills_by_token = defaultdict(list)
for t, h in zip(te_filled, hb):
    assert str(t.get("clob_token_id") or "").strip() == h["tid"]
    fills_by_token[h["tid"]].append((t, h))

corrections = []
flip = pnl_fix = back_resolve = void_n = 0
sum_old = sum_new = 0.0
for tid, pairs in fills_by_token.items():
    status, ret, ts = token_truth(tid)
    if status == "pending":
        continue
    cost = cost_by_asset.get(tid, 0.0)
    token_pnl = ret - cost
    tot_cash = sum(h["cash"] for _, h in pairs) or 1.0
    for t, h in pairs:
        share = h["cash"] / tot_cash
        new_pnl = round(token_pnl * share * (cost / tot_cash if False else 1.0), 4)  # pro-rata by fill cash
        new_status = "invalid" if tid in void_assets else status
        old_status = t["resolution_status"]
        old_pnl = t["pnl"] if t["pnl"] is not None else None
        resolved_at = ts or t.get("resolved_at") or int(time.time())
        changed = (old_status != new_status) or old_pnl is None or abs((old_pnl or 0) - new_pnl) > 0.005
        if not changed:
            continue
        corrections.append({
            "alert_id": t["alert_id"],
            "resolution_status": new_status,
            "pnl": new_pnl,
            "resolved_at": int(resolved_at),
        })
        if old_status in ("won", "lost") and new_status in ("won", "lost") and old_status != new_status: flip += 1
        elif old_status == "pending": back_resolve += 1
        elif new_status == "invalid": void_n += 1
        else: pnl_fix += 1
        sum_old += (old_pnl or 0.0); sum_new += new_pnl

print(f"corrections: {len(corrections)} rows  (label flips: {flip}, back-resolved pendings: {back_resolve}, "
      f"voids->invalid: {void_n}, pnl-only: {pnl_fix})")
print(f"pnl on corrected rows: DB {sum_old:+.2f} -> truth {sum_new:+.2f}")
# expected post-heal totals
all_new = {}
for tid, pairs in fills_by_token.items():
    status, ret, ts = token_truth(tid)
    if status == "pending": continue
    cost = cost_by_asset.get(tid, 0.0); token_pnl = ret - cost
    tot_cash = sum(h["cash"] for _, h in pairs) or 1.0
    for t, h in pairs:
        all_new[t["alert_id"]] = token_pnl * (h["cash"] / tot_cash)
print(f"expected DB realized P&L after heal (filled rows, chain-resolved): {sum(all_new.values()):+.2f}")

if len(sys.argv) > 1 and sys.argv[1] == "APPLY":
    print("applying in batches of 100...")
    for i in range(0, len(corrections), 100):
        batch = corrections[i:i+100]
        data = json.dumps({"corrections": batch, "source": "reconcile-chain"}).encode()
        req = urllib.request.Request(f"{RAILWAY_URL}/api/trades/bulk-correction", data=data,
                                     headers={"X-API-Key": RAILWAY_KEY, "Content-Type": "application/json"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            res = json.loads(r.read())
        print(f"  batch {i//100}: updated={res.get('updated')} errors={len(res.get('errors') or [])}")
    print("done.")
else:
    print("(dry-run; pass APPLY to write)")
