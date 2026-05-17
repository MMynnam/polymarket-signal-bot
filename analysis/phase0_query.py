import sqlite3, json, sys

DB = "/data/polymarket_bot.db"
db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

# ---------- 1. Tables ----------
tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print("TABLES:", tables)

# ---------- 2. alert_outcomes schema ----------
cols = db.execute("PRAGMA table_info(alert_outcomes)").fetchall()
print("\n--- alert_outcomes schema ---")
for c in cols:
    print(f"  {c[1]:35s} {c[2]}")

# ---------- 3. Row count + timestamp range ----------
row = db.execute("""
    SELECT COUNT(*) as n,
           MIN(created_at) as earliest,
           MAX(created_at) as latest
    FROM alert_outcomes
""").fetchone()
print(f"\nRow count: {row['n']}")
print(f"Earliest:  {row['earliest']}")
print(f"Latest:    {row['latest']}")

# ---------- 4. Resolution status breakdown ----------
print("\n--- resolution_status counts ---")
for r in db.execute("SELECT resolution_status, COUNT(*) as n FROM alert_outcomes GROUP BY resolution_status ORDER BY n DESC").fetchall():
    print(f"  {r[0]:15s} {r[1]}")

# ---------- 5. Are all alerts here or only traded ones? ----------
# Check overlap with trade_executions
traded = db.execute("SELECT COUNT(DISTINCT alert_id) FROM trade_executions").fetchone()[0]
total_ao = db.execute("SELECT COUNT(*) FROM alert_outcomes").fetchone()[0]
in_te = db.execute("SELECT COUNT(*) FROM alert_outcomes ao WHERE EXISTS (SELECT 1 FROM trade_executions te WHERE te.alert_id = ao.alert_id)").fetchone()[0]
not_in_te = total_ao - in_te
print(f"\n--- Traded vs all alerts ---")
print(f"  alert_outcomes rows:              {total_ao}")
print(f"  trade_executions distinct alerts: {traded}")
print(f"  alert_outcomes WITH a trade:      {in_te}")
print(f"  alert_outcomes WITHOUT a trade:   {not_in_te}")

# ---------- 6. Price at alert time ----------
print("\n--- price_at_alert availability ---")
has_price = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_price_at_alert IS NOT NULL").fetchone()[0]
no_price  = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE bet_price_at_alert IS NULL").fetchone()[0]
print(f"  bet_price_at_alert NOT NULL: {has_price}")
print(f"  bet_price_at_alert NULL:     {no_price}")

# ---------- 7. score_breakdown_json ----------
print("\n--- score_breakdown_json population ---")
has_json = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE score_breakdown_json IS NOT NULL AND score_breakdown_json != ''").fetchone()[0]
no_json  = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE score_breakdown_json IS NULL OR score_breakdown_json = ''").fetchone()[0]
print(f"  populated: {has_json}")
print(f"  NULL/empty: {no_json}")

# One real parsed example
row = db.execute("SELECT score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json IS NOT NULL AND score_breakdown_json != '' LIMIT 1").fetchone()
if row:
    try:
        parsed = json.loads(row[0])
        print(f"\n  Example parsed breakdown:")
        for k, v in sorted(parsed.items()):
            print(f"    {k}: {v}")
    except Exception as e:
        print(f"  Parse error: {e}")
        print(f"  Raw: {row[0][:300]}")

# All distinct keys across sample
print("\n  Key inventory (sample of 200):")
keys = set()
for r in db.execute("SELECT score_breakdown_json FROM alert_outcomes WHERE score_breakdown_json IS NOT NULL LIMIT 200").fetchall():
    try:
        keys.update(json.loads(r[0]).keys())
    except:
        pass
print(f"  {sorted(keys)}")

# ---------- 8. Market type / side (UP/DOWN vs YES/NO) ----------
print("\n--- bet_side distribution ---")
for r in db.execute("SELECT bet_side, COUNT(*) as n FROM alert_outcomes GROUP BY bet_side ORDER BY n DESC").fetchall():
    print(f"  {r[0]:10s} {r[1]}")

# Market type signal from title
print("\n--- 'Up or Down' in market_question (proxy for directional markets) ---")
up_down = db.execute("SELECT COUNT(*) FROM alert_outcomes WHERE market_question LIKE '%Up or Down%' OR market_question LIKE '%up or down%'").fetchone()[0]
print(f"  Directional ('Up or Down'): {up_down}")
print(f"  Binary (rest):              {total_ao - up_down}")

# ---------- 9. score column sanity check ----------
print("\n--- score distribution ---")
for r in db.execute("SELECT MIN(score), MAX(score), AVG(score), COUNT(*) FROM alert_outcomes").fetchall():
    print(f"  min={r[0]} max={r[1]} avg={r[2]:.1f} n={r[3]}")

print("\nDone.")
