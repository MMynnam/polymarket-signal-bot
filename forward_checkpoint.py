"""
forward_checkpoint.py — scorecard for the FAITHFUL ERA (post side-fix), from chain truth only.

The side-resolution fix deployed 2026-06-11T23:42:17Z (commit 060c07e). Every fill after
FIX_TS finally executes the designed strategy (buy the signal's side at its price). This
script measures that book the only trustworthy way: data-api activity cash + token-id
outcomes. No DB fields are used (te.size_usdc/pnl/labels had a corruption history).

PRE-REGISTERED DECISION GATE (2026-06-12, do not move the goalposts):
  Checkpoint at >= 200 chain-resolved faithful bets (expected ~3-4 weeks):
    * CONTINUE (collect to n=400) iff dollar ROI > 0 AND the Wilson-95% lower bound of
      (win-rate minus mean entry price) > -2pp.
    * SCALE UP (modestly) only at n >= 400 iff dollar-ROI bootstrap 95% CI excludes 0.
    * Otherwise: WIND DOWN — stop the trader, sweep remaining funds to the vault.
  Interim looks are fine (this script), but the gate decision happens at n>=200.

Usage: python forward_checkpoint.py
"""
import json, math, statistics, time, urllib.request, urllib.parse, sys, io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

FIX_TS = 1781221337  # 2026-06-11T23:42:17Z side-fix deploy
WALLET = "0x00BD1F45caAFd08a1FFfEABa7e17c712a8791e9E"
DATA_API = "https://data-api.polymarket.com"

def get(path, params):
    url = f"{DATA_API}{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    for att in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception:
            if att == 3: raise
            time.sleep(2 * (att + 1))

def paginate(path, base, limit=500):
    out, offset = [], 0
    while True:
        page = get(path, {**base, "limit": limit, "offset": offset})
        if isinstance(page, dict):
            page = page.get("data") or page.get("positions") or []
        if not page: break
        out.extend(page)
        if len(page) < limit: break
        offset += limit
        time.sleep(0.25)
    return out

print("pulling ledger...")
activity = paginate("/activity", {"user": WALLET})
positions = paginate("/positions", {"user": WALLET})
closed = paginate("/v1/closed-positions", {"user": WALLET}, limit=50)

trades = [a for a in activity if a["type"] == "TRADE" and a["timestamp"] >= FIX_TS]
closed_assets = {c["asset"]: c for c in closed}
dead_assets = {q["asset"] for q in positions if q.get("redeemable")}
live_assets = {q["asset"] for q in positions if not q.get("redeemable")}
red_by_cid = defaultdict(float)
for r in [a for a in activity if a["type"] == "REDEEM"]:
    red_by_cid[r["conditionId"]] += float(r["usdcSize"])

# faithful-era positions (only tokens FIRST bought after FIX_TS, to avoid pre-fix mixing)
first_buy = {}
for a in activity:
    if a["type"] == "TRADE":
        first_buy[a["asset"]] = min(first_buy.get(a["asset"], 1 << 60), a["timestamp"])
era_assets = {a for a, ts in first_buy.items() if ts >= FIX_TS}

book = defaultdict(lambda: {"cost": 0.0, "shares": 0.0})
for t in trades:
    if t["asset"] not in era_assets: continue
    book[t["asset"]]["cost"] += float(t["usdcSize"])
    book[t["asset"]]["shares"] += float(t["size"])

res = []   # (won, entry_px, cost, ret)
pend_cost = 0.0
for a, b in book.items():
    px = b["cost"] / b["shares"] if b["shares"] else 0.5
    if a in closed_assets:
        ret = red_by_cid.get(closed_assets[a]["conditionId"], 0.0)
        res.append((1, px, b["cost"], ret))
    elif a in dead_assets:
        res.append((0, px, b["cost"], 0.0))
    else:
        pend_cost += b["cost"]

def wilson(k, n, z=1.96):
    if n == 0: return (0, 0)
    ph = k / n; d = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / d
    h = z * math.sqrt((ph * (1 - ph) + z * z / (4 * n)) / n) / d
    return (c - h, c + h)

n = len(res)
days = (time.time() - FIX_TS) / 86400
print("=" * 70)
print(f"FAITHFUL ERA CHECKPOINT  (since 2026-06-11 23:42 UTC, {days:.1f} days)")
print("=" * 70)
if n == 0:
    print(f"resolved faithful positions: 0 (pending cost ${pend_cost:.2f}) — too early")
    sys.exit(0)
W = sum(r[0] for r in res)
cost = sum(r[2] for r in res); ret = sum(r[3] for r in res)
mp = statistics.mean(r[1] for r in res)
lo, hi = wilson(W, n)
roi = (ret - cost) / cost if cost else 0
print(f"resolved: {n}  ({W}W-{n-W}L)   pending cost ${pend_cost:.2f}")
print(f"WR {W/n:.1%} vs mean entry {mp:.1%}  -> edge {W/n-mp:+.1%}  (Wilson CI {lo-mp:+.1%}..{hi-mp:+.1%})")
print(f"cash: wagered ${cost:.2f}  returned ${ret:.2f}  P&L ${ret-cost:+.2f}  ROI {roi:+.1%}")
print()
print("GATE (n>=200):", "NOT YET REACHED" if n < 200 else
      ("CONTINUE" if roi > 0 and (lo - mp) > -0.02 else "WIND DOWN"))
