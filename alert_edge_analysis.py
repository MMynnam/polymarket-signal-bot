"""
alert_edge_analysis.py — Which alert weights are predictive / profitable?

Joins three sources:
  1. ao.json        — 6,848 scored alerts (alert_outcomes) w/ full score_breakdown_json
                      + bet_price_at_alert + resolution_status + roi  (SIGNAL universe)
  2. te_all.json    — 357 bot executions (trade_executions) w/ pnl + entry price + alert_id
  3. CSV (on-chain) — ground-truth entries + redemptions for the bot's wallet  (MONEY truth)

Central edge metric:  EDGE = WR - mean_entry_price.
  A binary bet at price p has expected ROI = WR/p - 1, so EDGE>0  <=>  positive expected ROI.
  This is intrinsically price-controlled: a component only has "edge" if its high
  bucket wins MORE OFTEN than the price it pays.

Pure-Python (no numpy/pandas). Outputs ASCII tables to alert_edge_results.txt.
"""

import csv, json, math, statistics
from collections import defaultdict, Counter
from market_classifier import classify_market, bet_price_band

CSV_PATH = r"C:\Users\manny\Downloads\Polymarket-History-2026-06-03.csv"
OUT = open("alert_edge_results.txt", "w", encoding="utf-8")

def p(*a):
    print(*a)
    print(*a, file=OUT)

COMPONENTS = ["timing", "funding_velocity", "win_rate", "size_anomaly",
              "wallet_age", "concentration", "cluster_bonus"]  # underdog dropped (all 0)

# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------
def wilson(k, n, z=1.96):
    """Wilson score interval for a binomial proportion. Returns (lo, hi)."""
    if n == 0:
        return (0.0, 0.0)
    phat = k / n
    denom = 1 + z*z/n
    center = (phat + z*z/(2*n)) / denom
    half = (z*math.sqrt((phat*(1-phat) + z*z/(4*n))/n)) / denom
    return (center - half, center + half)

def point_biserial(xs, ys):
    """Correlation between continuous xs and binary ys (0/1)."""
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = statistics.mean(xs), statistics.mean(ys)
    sx = statistics.pstdev(xs); sy = statistics.pstdev(ys)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x-mx)*(y-my) for x, y in zip(xs, ys)) / n
    return cov/(sx*sy)

def fmt_bucket(label, rows, price_key, won_key):
    """rows: list of dicts. Returns a formatted stats line."""
    n = len(rows)
    if n == 0:
        return f"  {label:16} n=0"
    won = sum(1 for r in rows if r[won_key])
    wr = won/n
    mp = statistics.mean(r[price_key] for r in rows)
    edge = wr - mp
    exp_roi = (wr/mp - 1) if mp > 0 else 0.0
    lo, hi = wilson(won, n)
    edge_lo = lo - mp  # CI on edge (price ~fixed)
    sig = "  *" if (edge_lo > 0 or hi - mp < 0) else ""
    return (f"  {label:16} n={n:5} WR={wr:5.1%} (CI {lo:.0%}-{hi:.0%})  "
            f"price={mp:5.1%}  EDGE={edge:+5.1%}  E[ROI]={exp_roi:+6.1%}{sig}")

# ---------------------------------------------------------------------------
# 1. Load CSV -> per-market ground truth (won = market had a Redeem)
# ---------------------------------------------------------------------------
rows = list(csv.DictReader(open(CSV_PATH, encoding="utf-8-sig")))
buys = [r for r in rows if r["action"] == "Buy"]
redeems = [r for r in rows if r["action"] == "Redeem"]

csv_redeemed_markets = set(r["marketName"].strip() for r in redeems)

# Per-market buy aggregation (entry prices come from cost/shares)
csv_market = defaultdict(lambda: {"cost": 0.0, "shares": 0.0, "side": None, "n": 0,
                                  "first_ts": None, "last_ts": None})
for b in buys:
    m = b["marketName"].strip()
    cost = float(b["usdcAmount"]); shares = float(b["tokenAmount"]); ts = int(b["timestamp"])
    d = csv_market[m]
    d["cost"] += cost; d["shares"] += shares; d["n"] += 1
    d["side"] = b["tokenName"].strip()
    d["first_ts"] = ts if d["first_ts"] is None else min(d["first_ts"], ts)
    d["last_ts"] = ts if d["last_ts"] is None else max(d["last_ts"], ts)

# Per-market redeem proceeds
csv_redeem_amt = defaultdict(float)
for r in redeems:
    csv_redeem_amt[r["marketName"].strip()] += float(r["usdcAmount"])

p("=" * 100)
p("ALERT-WEIGHT EDGE ANALYSIS")
p("=" * 100)
p(f"CSV: {len(buys)} buys / {len(redeems)} redeems / {len(csv_market)} distinct markets bet")
p(f"     {len(csv_redeemed_markets)} markets had a winning redemption (resolved-won on the bot's side)")
tot_cost = sum(d["cost"] for d in csv_market.values())
tot_red = sum(csv_redeem_amt.values())
p(f"     Total staked (all buys): ${tot_cost:.2f}   Total redeemed (so far): ${tot_red:.2f}")
p(f"     NOTE: markets with no redeem are EITHER lost OR not-yet-resolved; resolved-status")
p(f"           is taken from the bot DB (te_all/ao resolved_at), CSV gives the won/lost truth.")

# ---------------------------------------------------------------------------
# 2. Load ao.json — SIGNAL universe
# ---------------------------------------------------------------------------
ao = json.load(open("ao.json"))
for x in ao:
    bd = json.loads(x["score_breakdown_json"])
    x["_bd"] = bd
    x["_won"] = 1 if x["resolution_status"] == "resolved_won" else 0
    x["_price"] = x["bet_price_at_alert"]
    x["_cat"] = classify_market(x.get("market_question") or "")
    for c in COMPONENTS:
        x["_" + c] = bd.get(c) or 0

# sanity: roi semantics
won_rois = [x["roi"] for x in ao if x["_won"]]
lost_rois = [x["roi"] for x in ao if not x["_won"]]
p("")
p(f"ao.roi sanity: mean won-roi={statistics.mean(won_rois):+.3f}  mean lost-roi={statistics.mean(lost_rois):+.3f}")
p(f"               (won-roi should ~= 1/price-1 > 0 ; lost-roi should = -1)")

# ---------------------------------------------------------------------------
# 3. Load te_all.json — bot EXECUTIONS, join to ao for components, validate vs CSV
# ---------------------------------------------------------------------------
te = json.load(open("te_all.json"))
ao_by_id = {x["alert_id"]: x for x in ao}
for t in te:
    a = ao_by_id.get(t["alert_id"])
    t["_bd"] = a["_bd"] if a else None
    t["score"] = a["score"] if a else None
    t["_price"] = t.get("bet_price_filled") or t.get("bet_price_intended") or t.get("bet_price_at_alert")
    t["_cat"] = classify_market(t.get("market_question") or "")
    t["_mq"] = (t.get("market_question") or "").strip()
    # ground-truth outcome from CSV redemption
    t["_csv_won"] = 1 if t["_mq"] in csv_redeemed_markets else 0
    t["_db_status"] = t.get("resolution_status")

# Validate DB labels vs CSV (only for resolved, CSV-known markets)
resolved_te = [t for t in te if t["_db_status"] in ("won", "lost")]
in_csv = [t for t in resolved_te if t["_mq"] in csv_market]
agree = sum(1 for t in in_csv if (t["_db_status"] == "won") == bool(t["_csv_won"]))
p("")
p("-" * 100)
p("LABEL VALIDATION (bot DB resolution_status  vs  CSV on-chain redemption)")
p("-" * 100)
p(f"  bot executions: {len(te)} total, {len(resolved_te)} DB-resolved, {len(in_csv)} also present in CSV market set")
if in_csv:
    p(f"  agreement: {agree}/{len(in_csv)} = {agree/len(in_csv):.1%}")
    mism = [t for t in in_csv if (t["_db_status"]=="won") != bool(t["_csv_won"])]
    for t in mism[:12]:
        p(f"    MISMATCH db={t['_db_status']:4} csv_won={t['_csv_won']}  {t['_mq'][:60]}")

# ---------------------------------------------------------------------------
# ANALYSIS A — SIGNAL PREDICTIVENESS (ao, n=6848)
# ---------------------------------------------------------------------------
p("")
p("=" * 100)
p("ANALYSIS A — SIGNAL PREDICTIVENESS  (all 6,848 scored alerts; outcome = did alerted side win)")
p("=" * 100)
base_wr = sum(x["_won"] for x in ao)/len(ao)
base_price = statistics.mean(x["_price"] for x in ao)
p(f"BASE RATE: WR={base_wr:.1%}  mean entry price={base_price:.1%}  EDGE={base_wr-base_price:+.1%}  "
  f"E[ROI]={base_wr/base_price-1:+.1%}")
p("  ('*' marks buckets whose 95% Wilson CI on EDGE excludes zero)")

# A1. Aggregate score
p("")
p("A1. By AGGREGATE SCORE band:")
def score_band(s):
    if s >= 100: return "100+"
    if s >= 90: return "90-99"
    if s >= 85: return "85-89"
    if s >= 80: return "80-84"
    if s >= 75: return "75-79"
    if s >= 70: return "70-74"
    if s >= 65: return "65-69"
    return "60-64"
bands = defaultdict(list)
for x in ao: bands[score_band(x["score"])].append(x)
for b in ["60-64","65-69","70-74","75-79","80-84","85-89","90-99","100+"]:
    if bands[b]: p(fmt_bucket(b, bands[b], "_price", "_won"))

# A2. Each component, by value tier (terciles by rank within nonzero spread)
p("")
p("A2. By COMPONENT value (does a HIGHER component score -> more edge?):")
for c in COMPONENTS:
    key = "_" + c
    vals = sorted(set(x[key] for x in ao))
    p(f"\n  [{c}]  (max stored={max(vals)})")
    # bucket into low/mid/high by value thresholds at terciles of the value range
    lo_t = vals[len(vals)//3]; hi_t = vals[2*len(vals)//3]
    buckets = {"low": [], "mid": [], "high": []}
    for x in ao:
        v = x[key]
        if v <= lo_t: buckets["low"].append(x)
        elif v <= hi_t: buckets["mid"].append(x)
        else: buckets["high"].append(x)
    for bn in ["low","mid","high"]:
        if buckets[bn]:
            p(fmt_bucket(f"{bn}(<= {lo_t if bn=='low' else hi_t if bn=='mid' else max(vals)})",
                         buckets[bn], "_price", "_won"))
    # correlations
    pb_out = point_biserial([x[key] for x in ao], [x["_won"] for x in ao])
    pb_price = point_biserial([x[key] for x in ao], [x["_price"] for x in ao])
    p(f"    corr(component, WON)={pb_out:+.3f}   corr(component, PRICE)={pb_price:+.3f}   "
      f"{'<-- predictive of price, not outcome' if abs(pb_price)>abs(pb_out)+0.03 else ''}")

OUT.flush()
print("\n[A done]")

# ---------------------------------------------------------------------------
# ANALYSIS C — CONFOUND CONTROL: edge within price bands
# ---------------------------------------------------------------------------
p("")
p("=" * 100)
p("ANALYSIS C — CONFOUND CONTROL: is the score edge REAL after fixing entry price?")
p("=" * 100)
def pband(price):
    if price <= 0.30: return "1.longshot(<=.30)"
    if price <= 0.50: return "2.uncertain(.30-.50)"
    if price <= 0.70: return "3.lean(.50-.70)"
    if price <= 0.90: return "4.fav(.70-.90)"
    return "5.heavyfav(.90+)"
p("\nWR & edge by PRICE BAND (this is the dominant axis):")
pb = defaultdict(list)
for x in ao: pb[pband(x["_price"])].append(x)
for b in sorted(pb):
    p(fmt_bucket(b, pb[b], "_price", "_won"))

p("\nWithin each price band: does HIGH score still beat LOW score? (score>=80 vs <80)")
for b in sorted(pb):
    grp = pb[b]
    hi = [x for x in grp if x["score"] >= 80]
    lo = [x for x in grp if x["score"] < 80]
    if len(hi) >= 25 and len(lo) >= 25:
        hwr = sum(x["_won"] for x in hi)/len(hi)
        lwr = sum(x["_won"] for x in lo)/len(lo)
        p(f"  {b:22} score>=80: WR={hwr:.1%} (n={len(hi)})   score<80: WR={lwr:.1%} (n={len(lo)})   "
          f"delta={hwr-lwr:+.1%}")

# ---------------------------------------------------------------------------
# Market-level dedup robustness (kill pseudo-replication: many alerts per market)
# ---------------------------------------------------------------------------
p("")
p("-" * 100)
p("ROBUSTNESS: market-level dedup (one row per market_id, mean component, market outcome)")
p("-" * 100)
by_mkt = defaultdict(list)
for x in ao: by_mkt[x["market_id"]].append(x)
mkt_rows = []
for mid, xs in by_mkt.items():
    mkt_rows.append({
        "_won": xs[0]["_won"],  # market outcome (same for all alerts on it, in alerted dir)
        "_price": statistics.mean(x["_price"] for x in xs),
        "score": statistics.mean(x["score"] for x in xs),
        **{"_"+c: statistics.mean(x["_"+c] for x in xs) for c in COMPONENTS},
    })
p(f"  unique markets: {len(mkt_rows)} (vs {len(ao)} alerts -> {len(ao)/len(mkt_rows):.1f} alerts/market)")
mb = sum(r["_won"] for r in mkt_rows)/len(mkt_rows)
mpr = statistics.mean(r["_price"] for r in mkt_rows)
p(f"  market base: WR={mb:.1%} price={mpr:.1%} EDGE={mb-mpr:+.1%}")
hi = [r for r in mkt_rows if r["score"] >= 80]; lo = [r for r in mkt_rows if r["score"] < 80]
p(fmt_bucket("score>=80", hi, "_price", "_won"))
p(fmt_bucket("score<80", lo, "_price", "_won"))
for c in COMPONENTS:
    pb_out = point_biserial([r["_"+c] for r in mkt_rows], [r["_won"] for r in mkt_rows])
    p(f"    corr_mktlevel({c}, WON)={pb_out:+.3f}")

OUT.flush()
print("[C done]")

# ---------------------------------------------------------------------------
# ANALYSIS D — CATEGORY & PRICE STRUCTURE (what actually separates)
# ---------------------------------------------------------------------------
p("")
p("=" * 100)
p("ANALYSIS D — CATEGORY STRUCTURE (signal universe)")
p("=" * 100)
cat = defaultdict(list)
for x in ao: cat[x["_cat"]].append(x)
for c in sorted(cat, key=lambda k: -len(cat[k])):
    p(fmt_bucket(c, cat[c], "_price", "_won"))

# crypto sub-split: hourly up/down vs threshold
p("\nCrypto sub-types:")
def crypto_sub(q):
    ql = q.lower()
    if "up or down" in ql: return "crypto:up-down"
    if "between" in ql: return "crypto:range"
    if "above" in ql or "below" in ql: return "crypto:threshold"
    return "crypto:other"
csub = defaultdict(list)
for x in cat.get("crypto", []): csub[crypto_sub(x.get("market_question") or "")].append(x)
for c in sorted(csub, key=lambda k:-len(csub[k])):
    p(fmt_bucket(c, csub[c], "_price", "_won"))

# ---------------------------------------------------------------------------
# ANALYSIS B — REALIZED MONEY (te_all executions, CSV-validated)
# ---------------------------------------------------------------------------
p("")
p("=" * 100)
p("ANALYSIS B — REALIZED PROFITABILITY (bot's own executions; outcome from CSV redemption)")
p("=" * 100)
# Use CSV-confirmed outcome; restrict to executions whose market resolved (in CSV market set
# AND (csv won OR db says lost)). A market in CSV that the bot bet and that has resolved.
def te_resolved(t):
    # resolved if csv-won OR db-resolved-lost
    return t["_csv_won"] == 1 or t["_db_status"] == "lost"
res = [t for t in te if te_resolved(t) and t["_price"]]
p(f"  resolved bot executions used: {len(res)} (of {len(te)})")
def te_roi(t):
    if t["_csv_won"]:
        return (1.0/t["_price"] - 1.0)
    return -1.0
bwr = sum(t["_csv_won"] for t in res)/len(res)
bmp = statistics.mean(t["_price"] for t in res)
broi = statistics.mean(te_roi(t) for t in res)
p(f"  REALIZED: WR={bwr:.1%}  mean entry price={bmp:.1%}  mean ROI/bet={broi:+.1%}  EDGE={bwr-bmp:+.1%}")

p("\nB1. Realized ROI by AGGREGATE SCORE band:")
sb = defaultdict(list)
for t in res:
    if t.get("score") is not None:
        sb[score_band(t["score"])].append(t)
for b in ["60-64","65-69","70-74","75-79","80-84","85-89","90-99","100+"]:
    g = sb[b]
    if g:
        wr = sum(t["_csv_won"] for t in g)/len(g)
        roi = statistics.mean(te_roi(t) for t in g)
        mp = statistics.mean(t["_price"] for t in g)
        lo,hi = wilson(sum(t["_csv_won"] for t in g), len(g))
        p(f"  {b:8} n={len(g):3}  WR={wr:5.1%} (CI {lo:.0%}-{hi:.0%})  price={mp:.1%}  ROI/bet={roi:+6.1%}  EDGE={wr-mp:+.1%}")

p("\nB2. Realized ROI by ENTRY-PRICE band (the favorites-floor question):")
pbd = defaultdict(list)
for t in res: pbd[pband(t["_price"])].append(t)
for b in sorted(pbd):
    g = pbd[b]
    wr = sum(t["_csv_won"] for t in g)/len(g)
    roi = statistics.mean(te_roi(t) for t in g)
    mp = statistics.mean(t["_price"] for t in g)
    lo,hi = wilson(sum(t["_csv_won"] for t in g), len(g))
    p(f"  {b:22} n={len(g):3}  WR={wr:5.1%} (CI {lo:.0%}-{hi:.0%})  price={mp:.1%}  ROI/bet={roi:+6.1%}  EDGE={wr-mp:+.1%}")

p("\nB3. Realized ROI by CATEGORY:")
cbd = defaultdict(list)
for t in res: cbd[t["_cat"]].append(t)
for c in sorted(cbd, key=lambda k:-len(cbd[k])):
    g = cbd[c]
    wr = sum(t["_csv_won"] for t in g)/len(g)
    roi = statistics.mean(te_roi(t) for t in g)
    mp = statistics.mean(t["_price"] for t in g)
    p(f"  {c:14} n={len(g):3}  WR={wr:5.1%}  price={mp:.1%}  ROI/bet={roi:+6.1%}  EDGE={wr-mp:+.1%}")

OUT.flush()
print("[B,D done]")

# ---------------------------------------------------------------------------
# ANALYSIS E — Multivariate logistic regression (marginal contribution)
# ---------------------------------------------------------------------------
p("")
p("=" * 100)
p("ANALYSIS E — MULTIVARIATE LOGISTIC: outcome ~ components + price + category")
p("=" * 100)
p("  (standardized coefficients; |coef| = marginal predictive weight AFTER controlling for the rest)")

# Build feature matrix on ao (market-deduped to reduce pseudo-replication)
cats = ["crypto","sports","politics","entertainment","other"]
feat_names = COMPONENTS + ["price"] + ["cat_"+c for c in cats[:-1]]  # drop last cat (baseline)
X = []; Y = []
for r_src in by_mkt.values():
    # one row per market: mean components, mean price, modal category, outcome
    won = r_src[0]["_won"]
    price = statistics.mean(x["_price"] for x in r_src)
    catmode = Counter(x["_cat"] for x in r_src).most_common(1)[0][0]
    row = [statistics.mean(x["_"+c] for x in r_src) for c in COMPONENTS] + [price] + \
          [1.0 if catmode == c else 0.0 for c in cats[:-1]]
    X.append(row); Y.append(won)

# standardize continuous features
ncont = len(COMPONENTS) + 1
means = [statistics.mean(r[j] for r in X) for j in range(ncont)]
stds = [statistics.pstdev(r[j] for r in X) or 1.0 for j in range(ncont)]
for r in X:
    for j in range(ncont):
        r[j] = (r[j]-means[j])/stds[j]

# logistic regression via gradient descent w/ small L2
def train_logreg(X, Y, lr=0.1, epochs=4000, l2=1e-3):
    n = len(X); d = len(X[0])
    w = [0.0]*d; b = 0.0
    for _ in range(epochs):
        gw = [0.0]*d; gb = 0.0
        for xi, yi in zip(X, Y):
            z = b + sum(w[j]*xi[j] for j in range(d))
            pr = 1/(1+math.exp(-max(-30,min(30,z))))
            err = pr - yi
            for j in range(d): gw[j] += err*xi[j]
            gb += err
        for j in range(d): w[j] -= lr*(gw[j]/n + l2*w[j])
        b -= lr*gb/n
    return w, b
w, b = train_logreg(X, Y)
order = sorted(range(len(feat_names)), key=lambda j: -abs(w[j]))
p(f"  (n={len(X)} markets; intercept={b:+.3f})")
for j in order:
    p(f"    {feat_names[j]:18} coef={w[j]:+.3f}")
# pseudo-accuracy
correct = sum(1 for xi,yi in zip(X,Y) if (1/(1+math.exp(-max(-30,min(30,b+sum(w[j]*xi[j] for j in range(len(w))))))) >= 0.5)==yi)
p(f"  in-sample accuracy={correct/len(X):.1%} vs base-rate {max(mb,1-mb):.1%}")

p("")
p("=" * 100)
p("DONE — see alert_edge_results.txt")
p("=" * 100)
OUT.close()
print("[E done] wrote alert_edge_results.txt")
