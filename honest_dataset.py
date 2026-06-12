"""
honest_dataset.py — builds the per-bet HONEST dataset and runs deliverable-2 analyses.

Per bet (one row per filled execution):
  actual cash cost + actual paid price (activity, matched by token id + nearest timestamp)
  on-chain outcome of the token actually bought (closed=won / dead=lost / live=pending)
  matched_side flag (did the bot buy the side the signal said?)
  intended-side outcome (CLOB winner flag of the INTENDED side's token) — what a faithful copy would have got
  score, category, price band, time

Analyses:
  1. Realized edge by matched vs fallback fills (does following the signal matter?)
  2. Faithful-copy counterfactual: grade EVERY fill on the INTENDED side at its implied price
  3. Price-band edge on the honest realized book
  4. OOS (June 1+) signal universe: replicate predecessor structural claims on unseen data
"""
import json, math, statistics, sys, io, time
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
OUT = open("honest_results.txt", "w", encoding="utf-8")
def p(*a):
    s = " ".join(str(x) for x in a)
    print(s); print(s, file=OUT)

sys.path.insert(0, ".")
from market_classifier import classify_market

L = json.load(open("ledger.json"))
D = json.load(open("fresh_dump.json"))
clob = json.load(open("clob_markets.json"))

# token -> outcome/winner, market -> tokens
tokinfo = {}
mkt_tokens = {}
for cid, m in clob.items():
    if m.get("error"): continue
    mkt_tokens[cid] = m.get("tokens") or []
    for tk in m.get("tokens") or []:
        tokinfo[str(tk["token_id"])] = {"outcome": tk.get("outcome"), "winner": tk.get("winner"), "cid": cid}

trades = [a for a in L["activity"] if a["type"] == "TRADE"]
closed_assets = {c["asset"] for c in L["closed"]}
dead_assets = {q["asset"] for q in L["positions"] if q.get("redeemable")}
live_assets = {q["asset"] for q in L["positions"] if not q.get("redeemable")}

def chain_outcome(tid):
    if tid in closed_assets: return "won"
    if tid in dead_assets: return "lost"
    if tid in live_assets: return "pending"
    return "unknown"

act_by_asset = defaultdict(list)
for t in trades:
    act_by_asset[t["asset"]].append(t)

ao = {a["alert_id"]: a for a in D["ao_te"]}
te = [t for t in D["te"] if t["status"] == "filled"]

rows = []
for t in te:
    tid = str(t.get("clob_token_id") or "").strip()
    info = tokinfo.get(tid)
    if not info: continue
    # nearest activity fill within 15 min
    cands = [a for a in act_by_asset.get(tid, []) if abs(a["timestamp"] - t["created_at"]) < 900]
    if not cands: continue
    a_фill = min(cands, key=lambda a: abs(a["timestamp"] - t["created_at"]))
    paid_px = float(a_фill["price"]); cash = float(a_фill["usdcSize"])
    matched = str(t["bet_side"]).strip().lower() == str(info["outcome"]).strip().lower()
    out = chain_outcome(tid)
    # intended-side counterfactual: which token is the intended side, and did IT win?
    toks = mkt_tokens.get(info["cid"], [])
    int_tok = next((x for x in toks if str(x.get("outcome")).strip().lower() == str(t["bet_side"]).strip().lower()), None)
    int_won = (None if int_tok is None else int_tok.get("winner"))
    alert = ao.get(t["alert_id"]) or {}
    rows.append({
        "tid": tid, "matched": matched, "outcome": out, "paid_px": paid_px, "cash": cash,
        "won": 1 if out == "won" else 0,
        "int_winner": int_won, "alert_px": alert.get("bet_price_at_alert"),
        "score": alert.get("score"), "q": t.get("market_question") or "",
        "cat": classify_market(t.get("market_question") or ""), "ts": t["created_at"],
    })

res = [r for r in rows if r["outcome"] in ("won", "lost")]
p("=" * 86)
p(f"HONEST PER-BET DATASET: {len(rows)} fills matched ({len(res)} resolved)")
p("=" * 86)

def wilson(k, n, z=1.96):
    if n == 0: return (0, 0)
    ph = k / n; d = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / d
    h = z * math.sqrt((ph * (1 - ph) + z * z / (4 * n)) / n) / d
    return (c - h, c + h)

def line(label, rr, px_key="paid_px"):
    n = len(rr)
    if n == 0: return f"  {label:30} n=0"
    w = sum(r["won"] for r in rr); wr = w / n
    mp = statistics.mean(r[px_key] for r in rr)
    roi = statistics.mean((1 / r[px_key] - 1) if r["won"] else -1 for r in rr)
    cash_pnl = sum((r["cash"] * (1 / r[px_key] - 1)) if r["won"] else -r["cash"] for r in rr)
    lo, hi = wilson(w, n)
    return (f"  {label:30} n={n:4} WR={wr:5.1%} px={mp:5.1%} EDGE={wr - mp:+6.1%} "
            f"(CI {lo - mp:+.1%}..{hi - mp:+.1%}) ROI/bet={roi:+6.1%} $={cash_pnl:+8.2f}")

p("")
p("1) DOES FOLLOWING THE SIGNAL MATTER? (matched-side vs token[0]-fallback fills)")
p(line("matched side (faithful copy)", [r for r in res if r["matched"]]))
p(line("fallback token[0] (accident)", [r for r in res if not r["matched"]]))

p("")
p("2) FAITHFUL-COPY COUNTERFACTUAL (grade every fill on the INTENDED side)")
cf = []
for r in res:
    if r["int_winner"] is None: continue
    # implied price of intended side: if matched, paid px; else 1 - paid px (other token)
    px = r["paid_px"] if r["matched"] else round(1 - r["paid_px"], 4)
    if not (0.01 <= px <= 0.99): continue
    cf.append({"won": 1 if r["int_winner"] else 0, "paid_px": px, "cash": r["cash"]})
p(line("counterfactual faithful copy", cf))
p(line("actual book (for reference)", res))

p("")
p("3) PRICE-BAND EDGE — honest realized book (actual paid px, on-chain outcome)")
def pband(x):
    if x <= 0.30: return "<=.30"
    if x <= 0.50: return ".30-.50"
    if x <= 0.70: return ".50-.70"
    if x <= 0.90: return ".70-.90"
    return ".90+"
bands = defaultdict(list)
for r in res: bands[pband(r["paid_px"])].append(r)
for b in ["<=.30", ".30-.50", ".50-.70", ".70-.90", ".90+"]:
    if bands[b]: p(line(f"price {b}", bands[b]))
p("")
p("   by category:")
cats = defaultdict(list)
for r in res: cats[r["cat"]].append(r)
for c in sorted(cats, key=lambda k: -len(cats[k])):
    p(line(f"cat {c}", cats[c]))

p("")
p("4) OUT-OF-SAMPLE SIGNAL UNIVERSE (alerts created >= June 1, never seen by the old study)")
oos = [x for x in D["ao_oos"] if x["resolution_status"] in ("resolved_won", "resolved_lost")]
p(f"   resolved OOS alerts: {len(oos)}  (CAVEAT: graded on the alert's side STRING — same label")
p(f"   validity question as the executed book; treat as signal-level, not cash-level, evidence)")
for x in oos:
    x["won"] = 1 if x["resolution_status"] == "resolved_won" else 0
    x["paid_px"] = x["bet_price_at_alert"]; x["cash"] = 1.0
obands = defaultdict(list)
for x in oos: obands[pband(x["paid_px"])].append(x)
for b in ["<=.30", ".30-.50", ".50-.70", ".70-.90", ".90+"]:
    if obands[b]: p(line(f"OOS price {b}", obands[b]))
p("")
p("   OOS score deciles (does score predict edge on fresh data?)")
for s_lo, s_hi in [(60, 64), (65, 69), (70, 74), (75, 79), (80, 200)]:
    g = [x for x in oos if x["score"] is not None and s_lo <= x["score"] <= s_hi]
    if g: p(line(f"OOS score {s_lo}-{s_hi if s_hi < 200 else '+'}", g))
p("")
p("   OOS Simpson check: corr(score, won) within price halves")
def corr(xs, ys):
    n = len(xs)
    if n < 10: return float("nan")
    mx = statistics.mean(xs); my = statistics.mean(ys)
    sx = statistics.pstdev(xs); sy = statistics.pstdev(ys)
    if sx == 0 or sy == 0: return float("nan")
    return sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / n / (sx * sy)
lo_h = [x for x in oos if x["paid_px"] < 0.50]; hi_h = [x for x in oos if x["paid_px"] >= 0.50]
p(f"   price<0.50: n={len(lo_h)} corr={corr([x['score'] for x in lo_h], [x['won'] for x in lo_h]):+.3f}   "
  f"price>=0.50: n={len(hi_h)} corr={corr([x['score'] for x in hi_h], [x['won'] for x in hi_h]):+.3f}")

json.dump(rows, open("honest_bets.json", "w"))
OUT.close()
print("\nwrote honest_results.txt + honest_bets.json")
