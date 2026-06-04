"""
final_numbers.py — consolidated, verified figures for the report.
Adopts the adversarial panel's corrections:
  - realized: full denominator (all resolved), FILLED price, DB status as truth, flag BTC outlier
  - signal: category x price-band edge with Wilson CI (grounds the 'sports at modest prices' lead)
  - Simpson's-by-price table (score & timing flip sign across price)
  - win_rate binary-vs-graded (track-record dummy)
"""
import csv, json, math, statistics
from collections import defaultdict
from market_classifier import classify_market

OUT = open("final_numbers.txt", "w", encoding="utf-8")
def p(*a):
    print(*a); print(*a, file=OUT)

def wilson(k,n,z=1.96):
    if n==0: return (0,0)
    ph=k/n; d=1+z*z/n
    c=(ph+z*z/(2*n))/d; h=z*math.sqrt((ph*(1-ph)+z*z/(4*n))/n)/d
    return (c-h,c+h)

COMPONENTS=["timing","funding_velocity","win_rate","size_anomaly","wallet_age","concentration","cluster_bonus"]
ao=json.load(open("ao.json"))
for x in ao:
    bd=json.loads(x["score_breakdown_json"])
    x["_won"]=1 if x["resolution_status"]=="resolved_won" else 0
    x["_price"]=x["bet_price_at_alert"]; x["_cat"]=classify_market(x.get("market_question") or "")
    for c in COMPONENTS: x["_"+c]=bd.get(c) or 0

def pband(pr):
    if pr<=0.30: return "<=.30 longshot"
    if pr<=0.50: return ".30-.50 underdog"
    if pr<=0.70: return ".50-.70 lean"
    if pr<=0.90: return ".70-.90 fav"
    return ".90+ heavyfav"

def stat_line(label, rows):
    n=len(rows)
    if n==0: return f"  {label:24} n=0"
    won=sum(r["_won"] for r in rows); wr=won/n
    mp=statistics.mean(r["_price"] for r in rows); edge=wr-mp
    lo,hi=wilson(won,n)
    sig="  SIG" if (lo-mp>0 or hi-mp<0) else ""
    return f"  {label:24} n={n:5} WR={wr:5.1%} price={mp:5.1%} EDGE={edge:+5.1%} (CI {lo-mp:+.1%}..{hi-mp:+.1%}){sig}"

p("="*94)
p("SIGNAL UNIVERSE (ao.json, n=6848 resolved alerts) — CATEGORY x PRICE-BAND edge")
p("="*94)
p("  Grounds the single most actionable lead: where does WR beat the price paid?")
for cat in ["sports","crypto","other","entertainment","politics"]:
    sub=[x for x in ao if x["_cat"]==cat]
    p(f"\n[{cat}]  overall: {stat_line('', sub).strip()}")
    bands=defaultdict(list)
    for x in sub: bands[pband(x["_price"])].append(x)
    for b in ["<=.30 longshot",".30-.50 underdog",".50-.70 lean",".70-.90 fav",".90+ heavyfav"]:
        if bands[b]: p(stat_line(b, bands[b]))

p("\n"+"="*94)
p("SIMPSON'S PARADOX BY PRICE — score & timing flip sign across the price spectrum")
p("="*94)
p("  corr within band: positive = higher score/timing -> more likely to win")
def pb_corr(xs,ys):
    n=len(xs)
    if n<5: return 0.0
    mx=statistics.mean(xs); my=statistics.mean(ys)
    sx=statistics.pstdev(xs); sy=statistics.pstdev(ys)
    if sx==0 or sy==0: return 0.0
    return sum((a-mx)*(b-my) for a,b in zip(xs,ys))/n/(sx*sy)
pbands=defaultdict(list)
for x in ao: pbands[pband(x["_price"])].append(x)
p(f"  {'price band':18} {'n':>5}  corr(score,won)  corr(timing,won)   baseEDGE")
for b in ["<=.30 longshot",".30-.50 underdog",".50-.70 lean",".70-.90 fav",".90+ heavyfav"]:
    g=pbands[b]
    cs=pb_corr([x["score"] for x in g],[x["_won"] for x in g])
    ct=pb_corr([x["_timing"] for x in g],[x["_won"] for x in g])
    wr=sum(x["_won"] for x in g)/len(g); mp=statistics.mean(x["_price"] for x in g)
    p(f"  {b:18} {len(g):5}     {cs:+.3f}           {ct:+.3f}          {wr-mp:+.1%}")

p("\n"+"="*94)
p("win_rate COMPONENT: graded (0-15) vs binary 'have we seen this wallet win'")
p("="*94)
zero=[x for x in ao if x["_win_rate"]==0]; pos=[x for x in ao if x["_win_rate"]>0]
p(stat_line("win_rate==0 (no record)", zero))
p(stat_line("win_rate>0 (has record)", pos))
known=[x for x in ao if x["_win_rate"]>0]
cr=pb_corr([x["_win_rate"] for x in known],[x["_won"] for x in known])
p(f"  Among wallets WITH a record (n={len(known)}): corr(win_rate points, won) = {cr:+.3f}  "
  f"({'gradient adds signal' if cr>0.03 else 'gradient is noise/negative -> use a BINARY flag'})")

p("\n"+"="*94)
p("REALIZED (te_all.json, n=234 resolved) — HONEST cut: full denom, FILLED price, DB truth")
p("="*94)
te=json.load(open("te_all.json"))
ao_by={x["alert_id"]:x for x in ao}
res=[t for t in te if t.get("resolution_status") in ("won","lost")]
for t in res:
    t["_p"]=t.get("bet_price_filled") or t.get("bet_price_intended")
    t["_won"]=1 if t["resolution_status"]=="won" else 0
    t["_cat"]=classify_market(t.get("market_question") or "")
    t["_pnl"]=t.get("pnl") or 0.0
def realized(rows):
    n=len(rows); won=sum(r["_won"] for r in rows); wr=won/n
    mp=statistics.mean(r["_p"] for r in rows)
    roi=statistics.mean((1/r["_p"]-1) if r["_won"] else -1 for r in rows)
    pnl=sum(r["_pnl"] for r in rows)
    lo,hi=wilson(won,n)
    return f"n={n:3} WR={wr:5.1%} price={mp:5.1%} EDGE={wr-mp:+5.1%}(CI {lo-mp:+.1%}..{hi-mp:+.1%}) ROI/bet={roi:+6.1%} $PnL={pnl:+.2f}"
p(f"  ALL resolved:           {realized(res)}")
# BTC outlier
btc=max(res,key=lambda r:r["_pnl"])
p(f"  biggest single bet:     {btc['_pnl']:+.2f}  @entry {btc['_p']:.2f}  '{btc['market_question'][:48]}'")
p(f"  EX biggest-bet:         {realized([r for r in res if r is not btc])}")
p("\n  FILTER_MIN_BET_PRICE=0.50 floor split (does the favorites floor help?):")
p(f"    CUT  (price<0.50):     {realized([r for r in res if r['_p']<0.50])}")
p(f"    KEPT (price>=0.50):    {realized([r for r in res if r['_p']>=0.50])}")
p("\n  By category (realized):")
cbd=defaultdict(list)
for r in res: cbd[r["_cat"]].append(r)
for c in sorted(cbd,key=lambda k:-len(cbd[k])):
    p(f"    {c:14} {realized(cbd[c])}")
p("\n  Sports x price (the lead), realized:")
sp=[r for r in res if r["_cat"]=="sports"]
spb=defaultdict(list)
for r in sp: spb[pband(r["_p"])].append(r)
for b in ["<=.30 longshot",".30-.50 underdog",".50-.70 lean",".70-.90 fav"]:
    if spb[b]: p(f"    sports {b:18} {realized(spb[b])}")
OUT.close()
print("wrote final_numbers.txt")
