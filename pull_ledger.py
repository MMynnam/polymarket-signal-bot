"""Pull the authoritative Polymarket ledger for the trading wallet:
  - /activity        : full per-trade history (TRADE/REDEEM/...) with token ids
  - /v1/positions    : current open + redeemable positions
  - /v1/closed-positions : redeemed winners w/ realizedPnl
Writes ledger.json. Read-only, public data-api.
"""
import json, time, urllib.request, urllib.parse

DATA_API = "https://data-api.polymarket.com"
WALLET = "0x00BD1F45caAFd08a1FFfEABa7e17c712a8791e9E"

def get(path, params):
    url = f"{DATA_API}{path}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))

def paginate(path, base, limit=500, max_pages=100):
    out, offset = [], 0
    for _ in range(max_pages):
        page = get(path, {**base, "limit": limit, "offset": offset})
        if isinstance(page, dict):
            page = page.get("data") or page.get("activity") or page.get("positions") or []
        if not page:
            break
        out.extend(page)
        if len(page) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return out

print("activity...")
activity = paginate("/activity", {"user": WALLET}, limit=500)
print("  rows:", len(activity))
print("positions...")
positions = paginate("/v1/positions", {"user": WALLET}, limit=500)
print("  rows:", len(positions))
print("closed-positions...")
closed = paginate("/v1/closed-positions", {"user": WALLET}, limit=50)
print("  rows:", len(closed))

json.dump({"activity": activity, "positions": positions, "closed": closed, "pulled_at": time.time()},
          open("ledger.json", "w"))

from collections import Counter, defaultdict
types = Counter(a.get("type") for a in activity)
print("activity types:", dict(types))
sums = defaultdict(float)
for a in activity:
    sums[(a.get("type"), a.get("side") or "")] += float(a.get("usdcSize") or 0)
for k, v in sorted(sums.items()):
    print(f"  {k}: ${v:.2f}")
if activity:
    ts = [a.get("timestamp") for a in activity if a.get("timestamp")]
    print("activity range:", time.strftime("%Y-%m-%d %H:%M", time.gmtime(min(ts))), "->",
          time.strftime("%Y-%m-%d %H:%M", time.gmtime(max(ts))))
print("sample activity row keys:", list(activity[0].keys()) if activity else None)
