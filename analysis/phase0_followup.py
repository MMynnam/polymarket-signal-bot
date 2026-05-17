import sqlite3, json

DB = "/data/polymarket_bot.db"
db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

# ---------- ROI column ----------
print("=== roi column ===")
has_roi = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE roi IS NOT NULL").fetchone()[0]
no_roi  = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE roi IS NULL").fetchone()[0]
print(f"  populated: {has_roi}   NULL: {no_roi}")

# ROI stats on resolved rows only
row = db.execute("""
    SELECT MIN(roi), MAX(roi), AVG(roi), COUNT(*)
    FROM alert_outcomes
    WHERE roi IS NOT NULL AND resolution_status IN ('resolved_won','resolved_lost')
""").fetchone()
print(f"  resolved rows with roi — min={row[0]:.3f} max={row[1]:.3f} avg={row[2]:.3f} n={row[3]}")

# Sample won/lost roi values
print("\n  Sample ROI — won (first 5):")
for r in db.execute("SELECT resolution_status, bet_price_at_alert, roi FROM alert_outcomes WHERE resolution_status='resolved_won' AND roi IS NOT NULL LIMIT 5").fetchall():
    print(f"    {r[0]:15s} price={r[1]:.3f}  roi={r[2]:.4f}")
print("  Sample ROI — lost (first 5):")
for r in db.execute("SELECT resolution_status, bet_price_at_alert, roi FROM alert_outcomes WHERE resolution_status='resolved_lost' AND roi IS NOT NULL LIMIT 5").fetchall():
    print(f"    {r[0]:15s} price={r[1]:.3f}  roi={r[2]:.4f}")

# ---------- convergence_bonus vs cluster_bonus ----------
print("\n=== convergence_bonus vs cluster_bonus ===")
cb_only   = 0; cv_only   = 0; both   = 0; neither = 0
for r in db.execute("SELECT score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json IS NOT NULL LIMIT 1000").fetchall():
    try:
        d = json.loads(r[0])
        has_cb = "cluster_bonus" in d
        has_cv = "convergence_bonus" in d
        if has_cb and has_cv: both += 1
        elif has_cb: cb_only += 1
        elif has_cv: cv_only += 1
        else: neither += 1
    except: pass
print(f"  cluster_bonus only:    {cb_only}")
print(f"  convergence_bonus only:{cv_only}")
print(f"  both:                  {both}")
print(f"  neither:               {neither}")

# Show examples of each bonus type
print("\n  convergence_bonus example:")
for r in db.execute("SELECT score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json LIKE '%convergence_bonus%' LIMIT 1").fetchall():
    d = json.loads(r[0])
    print(f"    convergence_bonus={d.get('convergence_bonus')}  note={d.get('convergence_note','')[:80]}")
    print(f"    cluster_bonus={d.get('cluster_bonus')}  note={d.get('cluster_note','')[:80]}")

print("\n  cluster_bonus example (no convergence):")
for r in db.execute("SELECT score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json LIKE '%cluster_bonus%' AND score_breakdown_json NOT LIKE '%convergence_bonus%' LIMIT 1").fetchall():
    d = json.loads(r[0])
    print(f"    cluster_bonus={d.get('cluster_bonus')}  note={d.get('cluster_note','')[:80]}")

# ---------- resolution_status completeness ----------
print("\n=== resolution_status full distribution ===")
for r in db.execute("SELECT COALESCE(resolution_status,'NULL'), COUNT(*) FROM alert_outcomes GROUP BY resolution_status").fetchall():
    print(f"  {r[0]:20s} {r[1]}")

# ---------- pre-computed columns population ----------
print("\n=== pre-computed column population ===")
for col in ["size_anomaly_multiple", "hours_to_close_at_alert", "trade_hour_utc", "is_contrarian", "market_category", "bet_price_band"]:
    nn = db.execute(f"SELECT COUNT(*) FROM alert_outcomes WHERE {col} IS NOT NULL").fetchone()[0]
    null = db.execute(f"SELECT COUNT(*) FROM alert_outcomes WHERE {col} IS NULL").fetchone()[0]
    print(f"  {col:30s}  NOT NULL={nn}  NULL={null}")

# ---------- score distribution by market type ----------
print("\n=== bet_side simplified categories ===")
yes_no = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_side IN ('Yes','No')").fetchone()[0]
up_down = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_side IN ('Up','Down')").fetchone()[0]
over_under = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_side IN ('Over','Under')").fetchone()[0]
other = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_side NOT IN ('Yes','No','Up','Down','Over','Under')").fetchone()[0]
total = db.execute("SELECT COUNT(*) FROM alert_outcomes").fetchone()[0]
print(f"  Yes/No:     {yes_no:4d} ({yes_no/total*100:.1f}%)")
print(f"  Up/Down:    {up_down:4d} ({up_down/total*100:.1f}%)")
print(f"  Over/Under: {over_under:4d} ({over_under/total*100:.1f}%)")
print(f"  Other:      {other:4d} ({other/total*100:.1f}%)  <- esports/sports teams")

# Win rate by category (resolved only)
print("\n=== win rate by market category (resolved) ===")
for cat, sides in [("Yes/No", "('Yes','No')"), ("Up/Down", "('Up','Down')"), ("Over/Under", "('Over','Under')")]:
    r = db.execute(f"""
        SELECT
            SUM(CASE WHEN resolution_status='resolved_won' THEN 1 ELSE 0 END) as w,
            COUNT(*) as n
        FROM alert_outcomes
        WHERE bet_side IN {sides}
          AND resolution_status IN ('resolved_won','resolved_lost')
    """).fetchone()
    if r[1] > 0:
        print(f"  {cat:12s} W={r[0]:4d} N={r[1]:4d} WR={r[0]/r[1]*100:.1f}%")

r = db.execute("""
    SELECT
        SUM(CASE WHEN resolution_status='resolved_won' THEN 1 ELSE 0 END) as w,
        COUNT(*) as n
    FROM alert_outcomes
    WHERE bet_side NOT IN ('Yes','No','Up','Down','Over','Under')
      AND resolution_status IN ('resolved_won','resolved_lost')
""").fetchone()
print(f"  Other        W={r[0]:4d} N={r[1]:4d} WR={r[0]/r[1]*100:.1f}%")

# ---------- timestamp range as human dates ----------
import datetime
for r in db.execute("SELECT MIN(created_at), MAX(created_at) FROM alert_outcomes").fetchone():
    print(f"\n  timestamp {r} = {datetime.datetime.utcfromtimestamp(r).strftime('%Y-%m-%d %H:%M UTC')}")

print("\nDone.")
