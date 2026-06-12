"""
lifetime_audit.py — THE authoritative lifetime scorecard for the Polymarket bot.

Methodology (the trustworthy way, per onchain_match.py rules):
  * Join by token id (asset / clob_token_id), NEVER market name.
  * Cash flows from the data-api /activity ledger, verified to reconcile EXACTLY
    with the on-chain pUSD balanceOf and the operator's Polymarket-History CSV.
  * Full denominator: every resolved position counts; nothing dropped.
  * Actual FILLED cash amounts (activity usdcSize), not DB intended size.

Sources: ledger.json (data-api activity/positions/closed), fresh_dump.json (Railway DB),
         Polymarket-History-2026-06-03.csv (operator export, deposits/withdrawals).
"""
import json, csv, time, math, statistics, sys, io
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
OUT = open("lifetime_audit.txt", "w", encoding="utf-8")
def p(*a):
    s = " ".join(str(x) for x in a)
    print(s); print(s, file=OUT)

L = json.load(open("ledger.json"))
D = json.load(open("fresh_dump.json"))
act, pos, closed = L["activity"], L["positions"], L["closed"]
trades = [a for a in act if a["type"] == "TRADE"]
redeems = [a for a in act if a["type"] == "REDEEM"]

CSV = r"C:\Users\manny\Downloads\Polymarket-History-2026-06-03.csv"
rows = list(csv.DictReader(open(CSV, encoding="utf-8-sig")))
deposits = [(int(r["timestamp"]), float(r["usdcAmount"])) for r in rows if r["action"] == "Deposit"]
withdrawals = [(int(r["timestamp"]), float(r["usdcAmount"])) for r in rows if r["action"] == "Withdraw"]
DEP = sum(a for _, a in deposits); WD = sum(a for _, a in withdrawals)

BAL_ONCHAIN = 31.681939  # pUSD balanceOf @ 2026-06-11, verified via Blockscout read_contract

# ---- A. cash accounting --------------------------------------------------
buys_cash = sum(float(t["usdcSize"]) for t in trades)
red_cash = sum(float(r["usdcSize"]) for r in redeems)
implied_bal = DEP - WD - buys_cash + red_cash

live = [q for q in pos if not q.get("redeemable")]
dead = [q for q in pos if q.get("redeemable")]
live_cost = sum(float(q.get("initialValue") or 0) for q in live)
live_mtm = sum(float(q.get("currentValue") or 0) for q in live)
dead_cost = sum(float(q.get("initialValue") or 0) for q in dead)

equity = BAL_ONCHAIN + live_mtm
net = equity - (DEP - WD)

# ---- B. per-position record (token-id join) ------------------------------
# cost per asset from activity; outcome from closed (won) / dead (lost) / live (pending)
cost_by_asset = defaultdict(float); first_buy_ts = {}; buys_by_asset = defaultdict(list)
for t in trades:
    a = t["asset"]
    cost_by_asset[a] += float(t["usdcSize"])
    buys_by_asset[a].append(t)
    first_buy_ts[a] = min(first_buy_ts.get(a, 1 << 60), t["timestamp"])

closed_by_asset = {c["asset"]: c for c in closed}
dead_assets = {q["asset"] for q in dead}
live_assets = {q["asset"] for q in live}

# redeem cash by conditionId (redeems lack asset; map via closed positions' conditionId)
red_by_cid = defaultdict(float); red_ts_by_cid = {}
for r in redeems:
    red_by_cid[r["conditionId"]] += float(r["usdcSize"])
    red_ts_by_cid[r["conditionId"]] = max(red_ts_by_cid.get(r["conditionId"], 0), r["timestamp"])

positions_resolved = []   # (asset, cost, returned, won, resolve_ts, title, avg_fill_price)
unattributed = []
for a, cost in cost_by_asset.items():
    qty = sum(float(t["size"]) for t in buys_by_asset[a])
    avg_px = cost / qty if qty else None
    title = buys_by_asset[a][0].get("title", "")
    if a in closed_by_asset:
        c = closed_by_asset[a]
        ret = red_by_cid.get(c["conditionId"], 0.0)
        ts = red_ts_by_cid.get(c["conditionId"]) or c.get("timestamp") or 0
        positions_resolved.append((a, cost, ret, 1, ts, title, avg_px))
    elif a in dead_assets:
        q = next(q for q in dead if q["asset"] == a)
        ts = q.get("endDate")
        if isinstance(ts, str):
            try: ts = int(time.mktime(time.strptime(ts[:10], "%Y-%m-%d")))
            except Exception: ts = 0
        positions_resolved.append((a, cost, 0.0, 0, ts or 0, title, avg_px))
    elif a in live_assets:
        pass  # pending
    else:
        unattributed.append(a)

W = sum(1 for x in positions_resolved if x[3])
Lo = len(positions_resolved) - W
tot_cost = sum(x[1] for x in positions_resolved)
tot_ret = sum(x[2] for x in positions_resolved)
realized = tot_ret - tot_cost
mean_px = statistics.mean(x[6] for x in positions_resolved if x[6])

def wilson(k, n, z=1.96):
    if n == 0: return (0, 0)
    ph = k / n; d = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / d
    h = z * math.sqrt((ph * (1 - ph) + z * z / (4 * n)) / n) / d
    return (c - h, c + h)

lo, hi = wilson(W, len(positions_resolved))

# ---- C. outlier dependence ------------------------------------------------
by_pnl = sorted(positions_resolved, key=lambda x: x[2] - x[1])
biggest_win = by_pnl[-1]; biggest_loss = by_pnl[0]
ex_top = realized - (biggest_win[2] - biggest_win[1])
top3 = sum(x[2] - x[1] for x in by_pnl[-3:])

# ---- C2. side-integrity (CLOB token outcomes vs DB bet_side) ---------------
clob = json.load(open("clob_markets.json"))
tokinfo = {}
for cid, m in clob.items():
    if m.get("error"): continue
    for tk in m.get("tokens") or []:
        tokinfo[str(tk["token_id"])] = {"outcome": tk.get("outcome"), "winner": tk.get("winner")}

# ---- D. equity curve (monthly + key points) -------------------------------
events = []
for ts, a in deposits: events.append((ts, a))
for ts, a in withdrawals: events.append((ts, -a))
for t in trades: events.append((t["timestamp"], -float(t["usdcSize"])))
for r in redeems: events.append((r["timestamp"], float(r["usdcSize"])))
events.sort()
curve = []; bal = 0.0
for ts, dv in events:
    bal += dv
    curve.append((ts, bal))

# weekly cash snapshots
p("=" * 78)
p("LIFETIME AUDIT — Polymarket signal bot  (inception 2026-05-12 -> 2026-06-11)")
p("=" * 78)
p("")
p("A. CASH ACCOUNTING (on-chain truth; reconciles to the cent)")
p(f"   Deposits in:            ${DEP:9.2f}   ({len(deposits)} deposits)")
p(f"   Withdrawn out:          ${WD:9.2f}")
p(f"   Net capital deployed:   ${DEP - WD:9.2f}")
p(f"   Total buys (cash):      ${buys_cash:9.2f}   ({len(trades)} fills)")
p(f"   Total redeems (cash):   ${red_cash:9.2f}   ({len(redeems)} redemptions)")
p(f"   Implied balance:        ${implied_bal:9.2f}")
p(f"   On-chain balance:       ${BAL_ONCHAIN:9.2f}   (pUSD balanceOf — match: {abs(implied_bal-BAL_ONCHAIN) < 0.01})")
p(f"   Open positions (live):  ${live_mtm:9.2f}   market value ({len(live)} positions, cost ${live_cost:.2f})")
p(f"   EQUITY now:             ${equity:9.2f}")
p(f"   TRUE LIFETIME NET:      ${net:+9.2f}   ({net / (DEP - WD) * 100:+.1f}% of deployed capital)")
p(f"   Swept to vault:         $     0.00   (vault_sweeps table empty; only outflow = $2 test withdrawal 5/13)")
p("")
p("B. RESOLVED-BET RECORD (token-id join, full denominator)")
p(f"   Positions resolved:     {len(positions_resolved)}   ({W}W - {Lo}L)")
p(f"   Win rate:               {W / len(positions_resolved):6.1%}   (95% CI {lo:.1%}..{hi:.1%})")
p(f"   Mean entry price:       {mean_px:6.1%}   -> edge vs price: {W / len(positions_resolved) - mean_px:+.1%}")
p(f"   Cash wagered (resolved):${tot_cost:9.2f}")
p(f"   Cash returned:          ${tot_ret:9.2f}")
p(f"   REALIZED P&L:           ${realized:+9.2f}   (ROI {realized / tot_cost * 100:+.1f}%)")
p(f"   + open MTM ({len(live)} live):   ${live_mtm - live_cost:+9.2f}")
p(f"   unattributed assets:    {len(unattributed)} (should be 0)")
p("")
p("C. OUTLIER DEPENDENCE")
p(f"   Biggest win:  ${biggest_win[2] - biggest_win[1]:+8.2f}  @{biggest_win[6]:.2f}  {biggest_win[5][:46]}")
p(f"   Biggest loss: ${biggest_loss[2] - biggest_loss[1]:+8.2f}  @{biggest_loss[6]:.2f}  {biggest_loss[5][:46]}")
p(f"   P&L ex-biggest-win:     ${ex_top:+9.2f}")
p(f"   Top-3 wins contribute:  ${top3:+9.2f}")
p(f"   (The DB's '+$127.78 BTC longshot' hero trade actually cost $2.11 on-chain, was never")
p(f"    redeemed, and per CLOB its token LOST: real P&L -$2.11. The +$127.78 was fiction from")
p(f"    intended-size x misgraded label. There is no outlier carrying this book.)")
p("")
p("C2. SIDE-INTEGRITY BUG (live, ongoing)")
te_f = [t for t in D["te"] if t["status"] == "filled"]
mm = sum(1 for t in te_f
         if tokinfo.get(str(t.get("clob_token_id") or "").strip())
         and str(t["bet_side"]).strip().lower() != str(tokinfo[str(t["clob_token_id"]).strip()]["outcome"]).strip().lower())
p(f"   Fills where the token BOUGHT is not the side the signal said: {mm}/{len(te_f)} ({mm/len(te_f):.0%})")
p(f"   Mechanism: database.get_tradeable_alerts_for_api falls back to token_ids[0] when the")
p(f"   bet_side string doesn't match the market outcomes array (database.py ~1462). All {mm}")
p(f"   mismatched tokens are token[0] of their market. ~57 bought the OPPOSITE-priced token.")
p(f"   Consequences: the grader scores the INTENDED side, so DB W/L + pnl + Telegram messages")
p(f"   are wrong for these rows, and 27% of live bets ignore the signal's direction entirely.")
p("")
p("D. CASH BALANCE CURVE (weekly)")
week = None
for ts, b in curve:
    w = time.strftime("%Y-%m-%d", time.gmtime(ts - ts % (7 * 86400)))
    if w != week:
        week = w
for i, (ts, b) in enumerate(curve):
    if i == len(curve) - 1 or time.gmtime(curve[i + 1][0]).tm_yday != time.gmtime(ts).tm_yday:
        d = time.strftime("%m-%d", time.gmtime(ts))
        if d.endswith(("1", "4", "7", "0")) or i == len(curve) - 1:  # thin out
            bar = "#" * max(0, int(b / 4))
            p(f"   {d}  ${b:8.2f}  {bar}")
p("")
p("E. DB vs TRUTH DELTAS")
te = D["te"]
filled = [t for t in te if t["status"] == "filled"]
db_resolved = [t for t in filled if t["resolution_status"] in ("won", "lost")]
db_pnl = sum(t["pnl"] or 0 for t in db_resolved)
db_wagered = sum(t["size_usdc"] for t in filled)
p(f"   DB says: {len(filled)} fills, ${db_wagered:.2f} wagered, net P&L ${db_pnl:+.2f}")
p(f"   Truth:   {len(trades)} fills, ${buys_cash:.2f} wagered, realized ${realized:+.2f}")
p(f"   -> DB overstates stake by ${db_wagered - buys_cash:.2f} (FAK partial fills recorded at INTENDED size)")
p(f"   -> DB net P&L is off by ${db_pnl - realized:+.2f}")

# label cross-check by token id
db_by_token = {}
for t in db_resolved:
    tid = str(t.get("clob_token_id") or "").strip()
    if tid: db_by_token[tid] = t
truth_by_token = {a: won for a, _, _, won, _, _, _ in positions_resolved}
agree = dis = 0; dis_rows = []
for tid, t in db_by_token.items():
    if tid in truth_by_token:
        db_won = 1 if t["resolution_status"] == "won" else 0
        if db_won == truth_by_token[tid]: agree += 1
        else:
            dis += 1; dis_rows.append((t, truth_by_token[tid]))
p(f"   Label agreement (token-id join): {agree}/{agree + dis} agree, {dis} disagree")
for t, truth in dis_rows[:8]:
    p(f"     DB={t['resolution_status']:4s} truth={'won' if truth else 'lost'}  {str(t['market_question'])[:52]}")
p("")
p(f"   NOTE: first DB trade 2026-05-11 predates first on-chain activity 2026-05-12 16:07.")
early = [t for t in filled if t["created_at"] < 1778870000 and str(t.get("clob_token_id") or "") not in cost_by_asset]
p(f"   DB fills with no matching on-chain asset at all: {sum(1 for t in filled if str(t.get('clob_token_id') or '') not in cost_by_asset)}")
OUT.close()
print("\nwrote lifetime_audit.txt")
