import sqlite3, json, datetime

DB = "/data/polymarket_bot.db"
db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

def ts(t): return datetime.datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M UTC') if t else None

# ---------- Row counts by date ----------
print("=== Daily alert volume ===")
for r in db.execute("""
    SELECT DATE(datetime(created_at,'unixepoch')) as d,
           COUNT(*) as n,
           SUM(CASE WHEN resolution_status='resolved_won' THEN 1 ELSE 0 END) as won,
           SUM(CASE WHEN resolution_status='resolved_lost' THEN 1 ELSE 0 END) as lost
    FROM alert_outcomes
    GROUP BY d ORDER BY d
""").fetchall():
    resolved = r['won'] + r['lost']
    wr = f"{r['won']/resolved*100:.0f}%" if resolved > 0 else "n/a"
    print(f"  {r['d']}  n={r['n']:4d}  won={r['won']:3d}  lost={r['lost']:3d}  WR={wr}")

# ---------- Regime boundaries: row counts before/after each key date ----------
print("\n=== Row counts by regime ===")

# Key UTC timestamps (commits were in EDT = UTC-4)
regimes = [
    ("May 2 03:07 UTC — DB start",             1777691269),
    ("May 6 22:34 UTC — cluster/conv zeroed",   1746575640),  # May 6 18:33 EDT -> 22:33 UTC
    ("May 10 03:43 UTC — precision filters on", 1746845000),  # May 10 23:43 EDT -> May 11 03:43 UTC
    ("May 11 23:55 UTC — filters loosened",     1746910200),  # May 11 19:55 EDT -> May 11 23:55 UTC
    ("May 12 16:23 UTC — instant thresh 65",    1746980580),  # May 12 12:23 EDT -> 16:23 UTC
    ("May 14 03:17 UTC — wallet_profiler fix",  1747192620),  # May 13 23:17 EDT -> May 14 03:17 UTC
]

for i in range(len(regimes)-1):
    name, start = regimes[i]
    _, end = regimes[i+1]
    r = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE created_at >= ? AND created_at < ?", (start, end)).fetchone()[0]
    print(f"  {name}")
    print(f"    → {end-start}s window, {r} alerts")

r = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE created_at >= ?", (regimes[-1][1],)).fetchone()[0]
print(f"  {regimes[-1][0]}")
print(f"    → through end, {r} alerts")

# ---------- win_rate component distribution across time ----------
print("\n=== win_rate component score distribution by day ===")
rows = db.execute("SELECT created_at, score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json IS NOT NULL ORDER BY created_at").fetchall()

daily_wr = {}
for row in rows:
    d = datetime.datetime.utcfromtimestamp(row['created_at']).strftime('%Y-%m-%d')
    try:
        bd = json.loads(row['score_breakdown_json'])
        wr = bd.get('win_rate', None)
        if d not in daily_wr:
            daily_wr[d] = {'zero': 0, 'nonzero': 0, 'total': 0, 'sum': 0}
        daily_wr[d]['total'] += 1
        if wr is None or wr == 0:
            daily_wr[d]['zero'] += 1
        else:
            daily_wr[d]['nonzero'] += 1
            daily_wr[d]['sum'] += wr
    except: pass

for d, v in sorted(daily_wr.items()):
    avg = v['sum']/v['nonzero'] if v['nonzero'] > 0 else 0
    print(f"  {d}  total={v['total']:4d}  win_rate=0: {v['zero']:4d}  non-zero: {v['nonzero']:4d}  avg_nonzero={avg:.1f}")

# ---------- cluster_bonus and convergence_bonus distribution over time ----------
print("\n=== cluster_bonus and convergence_bonus daily averages ===")
daily_bonus = {}
for row in rows:
    d = datetime.datetime.utcfromtimestamp(row['created_at']).strftime('%Y-%m-%d')
    try:
        bd = json.loads(row['score_breakdown_json'])
        cb = bd.get('cluster_bonus', 0)
        cv = bd.get('convergence_bonus', None)
        if d not in daily_bonus:
            daily_bonus[d] = {'cb_sum': 0, 'cb_n': 0, 'cv_sum': 0, 'cv_n': 0, 'cv_present': 0, 'total': 0}
        daily_bonus[d]['total'] += 1
        daily_bonus[d]['cb_sum'] += cb
        daily_bonus[d]['cb_n'] += 1
        if cv is not None:
            daily_bonus[d]['cv_sum'] += cv
            daily_bonus[d]['cv_n'] += 1
            daily_bonus[d]['cv_present'] += 1
    except: pass

for d, v in sorted(daily_bonus.items()):
    avg_cb = v['cb_sum']/v['cb_n'] if v['cb_n'] > 0 else 0
    avg_cv = v['cv_sum']/v['cv_n'] if v['cv_n'] > 0 else 0
    print(f"  {d}  total={v['total']:4d}  avg_cluster={avg_cb:.1f}  avg_conv={avg_cv:.1f}  conv_present={v['cv_present']}")

# ---------- Pre-computed columns: which rows have them ----------
print("\n=== Pre-computed column coverage vs time ===")
r = db.execute("""
    SELECT
        MIN(created_at) as first_with,
        MAX(created_at) as last_with,
        COUNT(*) as n
    FROM alert_outcomes WHERE market_category IS NOT NULL
""").fetchone()
print(f"  Rows WITH market_category: {r['n']}")
print(f"    first: {ts(r['first_with'])}")
print(f"    last:  {ts(r['last_with'])}")

r2 = db.execute("""
    SELECT MIN(created_at) as first_without, MAX(created_at) as last_without, COUNT(*) as n
    FROM alert_outcomes WHERE market_category IS NULL
""").fetchone()
print(f"  Rows WITHOUT market_category: {r2['n']}")
print(f"    first: {ts(r2['first_without'])}")
print(f"    last:  {ts(r2['last_without'])}")

# ---------- score column includes bonuses? Check pre/post May 6 ----------
print("\n=== Score col vs component sum (does total include bonuses?) ===")
print("  Sample pre-May 6 row:")
r = db.execute("SELECT score, score_breakdown_json FROM alert_outcomes WHERE created_at < 1746575640 LIMIT 1").fetchone()
if r:
    bd = json.loads(r['score_breakdown_json'])
    print(f"  score={r['score']}  json_total={bd.get('total')}  cluster={bd.get('cluster_bonus')}  conv={bd.get('convergence_bonus')}")

print("  Sample post-May 6 row:")
r = db.execute("SELECT score, score_breakdown_json FROM alert_outcomes WHERE created_at >= 1746575640 LIMIT 1").fetchone()
if r:
    bd = json.loads(r['score_breakdown_json'])
    print(f"  score={r['score']}  json_total={bd.get('total')}  cluster={bd.get('cluster_bonus')}  conv={bd.get('convergence_bonus')}")

print("\nDone.")
