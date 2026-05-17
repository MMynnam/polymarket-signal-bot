"""
analysis/run_analysis.py
Full Phase 2-5 scorer analysis.  Requires: pandas, numpy, scipy, scikit-learn.

Usage:
    pip install pandas numpy scipy scikit-learn
    python analysis/run_analysis.py analysis/dataset.csv

Design (per brief):
  Shippable spine   -- Regime C, k-fold CV, 6 predictors
  Robustness check  -- 5 stable components A-vs-C side-by-side
  H5 (win_rate)     -- Regime C only, bootstrap CIs
  H3/cluster_bonus  -- Regime A only, feature-reactivation framing
  Primary metric    -- ROI;  win rate is secondary diagnostic
"""

import re
import sys
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# 0. Load data
# ---------------------------------------------------------------------------
CSV = sys.argv[1] if len(sys.argv) > 1 else "analysis/dataset.csv"
df = pd.read_csv(CSV, low_memory=False)

# Re-derive regime from created_at (guards against stale CSV regime column)
REGIME_A_END   = 1778106829  # May 6 22:33 UTC
REGIME_C_START = 1778728669  # May 14 03:17 UTC

def _regime(ts):
    if ts < REGIME_A_END:   return "A"
    if ts < REGIME_C_START: return "B"
    return "C"

df["regime"] = df["created_at"].apply(lambda t: _regime(t) if pd.notna(t) else "?")

NUM_COLS = ["timing", "wallet_age", "win_rate", "size_anomaly",
            "concentration", "funding_velocity",
            "cluster_bonus", "convergence_bonus",
            "roi", "score", "bet_price_at_alert", "outcome", "created_at"]
for c in NUM_COLS:
    df[c] = pd.to_numeric(df[c], errors="coerce")

resolved = df["outcome"].notna()
A     = df[resolved & (df["regime"] == "A")].copy()
B     = df[resolved & (df["regime"] == "B")].copy()
C     = df[resolved & (df["regime"] == "C")].copy()
ALL_R = df[resolved].copy()

SEP = "=" * 72

def hdr(title):
    print(f"\n{SEP}\n{title}\n{SEP}")

def sub(title):
    print(f"\n--- {title} ---")

# ---------------------------------------------------------------------------
# 1. Dataset summary
# ---------------------------------------------------------------------------
hdr("PHASE 1 -- DATASET SUMMARY")

for label, subset, full in [
    ("Full (all regimes, resolved)", ALL_R, df),
    ("Regime A  (May 2-6)",          A,     df[df.regime=="A"]),
    ("Regime B  (May 7-13)",         B,     df[df.regime=="B"]),
    ("Regime C  (May 14-17)",        C,     df[df.regime=="C"]),
]:
    n_total   = len(full)
    n_resolved = len(subset)
    n_pending  = n_total - n_resolved
    n_won  = int(subset["outcome"].sum()) if n_resolved else 0
    n_lost = n_resolved - n_won
    wr       = n_won / n_resolved * 100 if n_resolved else 0
    mean_roi = subset["roi"].mean() if n_resolved else float("nan")
    print(f"\n{label}")
    print(f"  total={n_total:5d}  resolved={n_resolved:5d}  pending={n_pending:3d}")
    print(f"  won={n_won:4d}  lost={n_lost:4d}  WR={wr:.1f}%  mean_ROI={mean_roi:+.4f}")

sub("Market type breakdown (resolved, all regimes)")
for mtype in ["yes_no", "up_down", "over_under", "esports_sports"]:
    s  = ALL_R[ALL_R.market_type == mtype]
    wr = s.outcome.mean()*100 if len(s) else 0
    mr = s.roi.mean()          if len(s) else float("nan")
    print(f"  {mtype:18s} n={len(s):5d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")

print("\nROI caveat: roi = (1-price)/price on wins, -1.0 on losses (no-slippage counterfactual).")
print("Relative component rankings are robust to a constant slippage drag.")

# ---------------------------------------------------------------------------
# 2. Power check for Regime C spine
# ---------------------------------------------------------------------------
hdr("PHASE 1 -- POWER CHECK: REGIME C SPINE")
n_C        = len(C)
n_wins     = int(C["outcome"].sum())
n_losses   = n_C - n_wins
EPV        = min(n_wins, n_losses) / 6  # events-per-variable, 6 predictors
print(f"Regime C resolved: n={n_C}, won={n_wins}, lost={n_losses}")
print(f"EPV (6 predictors, minority class): {EPV:.1f}  "
      f"(threshold >=10: {'OK' if EPV >= 10 else 'MARGINAL'})")

STABLE_COMPS = ["timing", "wallet_age", "size_anomaly", "concentration", "funding_velocity"]
SPINE_COMPS  = STABLE_COMPS + ["win_rate"]

# ---------------------------------------------------------------------------
# Helper: smart univariate analysis (quartile with binary fallback)
# ---------------------------------------------------------------------------
def univariate_table(data, component, n_buckets=4):
    d = data[data[component].notna() & data["outcome"].notna()].copy()
    if len(d) < 20:
        return None, None, "n<20"

    # Try quartile bucketing; fall back to binary (zero vs positive) for sparse components
    try:
        d["bucket"] = pd.qcut(d[component], q=n_buckets, duplicates="drop", labels=False)
        n_unique = d["bucket"].nunique()
    except Exception:
        n_unique = 0

    if n_unique < 2:
        # Binary split: 0 vs >0
        d["bucket"] = (d[component] > 0).astype(int)
        method = "binary"
    else:
        method = "quartile"

    rows = []
    for b in sorted(d["bucket"].dropna().unique()):
        g = d[d["bucket"] == b]
        rows.append({
            "bucket": b,
            "min": g[component].min(),
            "max": g[component].max(),
            "n": len(g),
            "wr": g["outcome"].mean() * 100,
            "mean_roi": g["roi"].mean(),
        })

    bot = d[d["bucket"] == d["bucket"].min()]["roi"].dropna()
    top = d[d["bucket"] == d["bucket"].max()]["roi"].dropna()
    if len(bot) > 1 and len(top) > 1:
        _, p = stats.mannwhitneyu(bot, top, alternative="two-sided")
    else:
        p = float("nan")

    return rows, p, method


def print_univariate(data, component, regime_label, hypothesis=None):
    rows, p, method = univariate_table(data, component)
    if rows is None:
        print(f"  {component} [{regime_label}]: {method}")
        return
    tag = f"  [{hypothesis}] " if hypothesis else "  "
    p_str = f"{p:.4f}" if not (p != p) else "n/a"  # nan check
    print(f"{tag}{component}  [{regime_label}]  MW-p={p_str}  method={method}")
    print(f"    {'Bucket':>6} {'Range':>18} {'N':>5} {'WR%':>6} {'meanROI':>9}")
    for r in rows:
        flag = "**" if r["n"] < 20 else "  "
        print(f"  {flag}  {r['bucket']:>4}   [{r['min']:6.1f}-{r['max']:6.1f}]  "
              f"{r['n']:5d}  {r['wr']:5.1f}%  {r['mean_roi']:+8.4f}")
    if any(r["n"] < 20 for r in rows):
        print("    ** cell n<20: inconclusive")


# ---------------------------------------------------------------------------
# 3. Phase 2 -- Univariate analysis
# ---------------------------------------------------------------------------
hdr("PHASE 2 -- UNIVARIATE ANALYSIS")

sub("2A. Regime C -- all 6 spine components")
for comp in SPINE_COMPS:
    print_univariate(C, comp, "Regime C")

sub("2B. Pre-registered hypothesis tests")

print("\nH1: wallet_age -- anti-predictive? (higher score -> lower ROI)")
print_univariate(C, "wallet_age", "Regime C", "H1")
print_univariate(A, "wallet_age", "Regime A", "H1-robustness")

print("\nH2: size_anomaly -- strongest positive predictor?")
print_univariate(C, "size_anomaly", "Regime C", "H2")
print_univariate(A, "size_anomaly", "Regime A", "H2-robustness")

print("\nH4: timing -- weak or semantically inverted?")
print_univariate(C, "timing", "Regime C", "H4")
print_univariate(A, "timing", "Regime A", "H4-robustness")

print("\nH7: concentration -- noise?")
print_univariate(C, "concentration", "Regime C", "H7")
print_univariate(A, "concentration", "Regime A", "H7-robustness")

print("\nfunding_velocity (no pre-registered H, included for completeness)")
print_univariate(C, "funding_velocity", "Regime C")
print_univariate(A, "funding_velocity", "Regime A")

sub("H6: bet_side YES vs NO -- Regime C primary")
for side in ["Yes", "No", "Up", "Down", "Over", "Under"]:
    s = C[C["bet_side"] == side]
    if len(s) >= 5:
        wr = s.outcome.mean()*100
        mr = s.roi.mean()
        flag = "**" if len(s) < 20 else "  "
        print(f"  {flag}{side:8s}  n={len(s):4d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")

sub("H6: bet_side YES vs NO -- Regime A robustness")
for side in ["Yes", "No"]:
    s = A[A["bet_side"] == side]
    wr = s.outcome.mean()*100 if len(s) else 0
    mr = s.roi.mean()          if len(s) else float("nan")
    print(f"  {side:8s}  n={len(s):4d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")

# MW test YES vs NO on ROI
yes_C = C[C.bet_side == "Yes"]["roi"].dropna()
no_C  = C[C.bet_side == "No"]["roi"].dropna()
if len(yes_C) > 1 and len(no_C) > 1:
    _, p_h6 = stats.mannwhitneyu(yes_C, no_C, alternative="two-sided")
    print(f"  YES vs NO MW-p (Regime C) = {p_h6:.4f}  "
          f"{'SIGNIFICANT' if p_h6 < 0.05 else 'not significant'}")

sub("H8: Up/Down market type -- Regime C vs Regime A")
for regime_label, regime_data in [("Regime C", C), ("Regime A", A)]:
    print(f"\n  {regime_label}:")
    for mtype, label in [("up_down","Up/Down"), ("yes_no","Yes/No"),
                         ("over_under","Over/Under"), ("esports_sports","Esports/Sports")]:
        s = regime_data[regime_data.market_type == mtype]
        if len(s) < 5:
            continue
        mr  = s.roi.mean()
        wr  = s.outcome.mean()*100
        flag = "**" if len(s) < 20 else "  "
        print(f"    {flag}{label:18s}  n={len(s):4d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")

sub("2C. Market-type segmentation: do components behave differently? (Regime C)")
for mtype in ["yes_no", "esports_sports", "up_down"]:
    s = C[C.market_type == mtype]
    if len(s) < 20:
        print(f"\n  {mtype} (n={len(s)}): insufficient data")
        continue
    print(f"\n  {mtype} (n={len(s)} resolved in Regime C):")
    for comp in ["timing", "size_anomaly", "wallet_age", "concentration", "funding_velocity"]:
        rows, p, method = univariate_table(s, comp)
        if rows is None:
            print(f"    {comp}: {method}")
            continue
        p_str = f"{p:.4f}" if not (p != p) else "n/a"
        top_roi = max(r["mean_roi"] for r in rows)
        bot_roi = min(r["mean_roi"] for r in rows)
        print(f"    {comp:22s}  MW-p={p_str}  ROI_range=[{bot_roi:+.4f},{top_roi:+.4f}]  "
              f"n_buckets={len(rows)}  method={method}")

# ---------------------------------------------------------------------------
# 4. Phase 3 -- Multivariate: Logistic regression on Regime C
# ---------------------------------------------------------------------------
hdr("PHASE 3 -- MULTIVARIATE: LOGISTIC REGRESSION ON REGIME C")

C_model = C[SPINE_COMPS + ["outcome", "roi"]].dropna(subset=SPINE_COMPS + ["outcome"])
X  = C_model[SPINE_COMPS].values
y  = C_model["outcome"].values.astype(int)
scaler = StandardScaler()
Xs = scaler.fit_transform(X)

print(f"Logistic regression dataset: n={len(C_model)}, won={y.sum()}, lost={len(y)-y.sum()}")
print(f"EPV (minority class / 6 predictors): {min(y.sum(), len(y)-y.sum())/6:.1f}")

cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
lr = LogisticRegression(max_iter=1000, C=1.0)
cv_roc = cross_val_score(lr, Xs, y, cv=cv, scoring="roc_auc")
cv_acc = cross_val_score(lr, Xs, y, cv=cv, scoring="accuracy")
print(f"\n5-fold CV ROC-AUC: {cv_roc.mean():.4f} +/- {cv_roc.std():.4f}")
print(f"5-fold CV Accuracy: {cv_acc.mean():.4f} +/- {cv_acc.std():.4f}")

lr.fit(Xs, y)
print("\nLogistic regression coefficients (standardized -- comparable magnitude):")
print(f"  {'Component':22s}  {'Coef':>8}  {'Direction':>10}")
coefs_sorted = sorted(zip(SPINE_COMPS, lr.coef_[0]), key=lambda x: abs(x[1]), reverse=True)
for comp, coef in coefs_sorted:
    direction = "POSITIVE" if coef > 0 else "negative"
    print(f"  {comp:22s}  {coef:+8.4f}  {direction}")
print(f"  Intercept: {lr.intercept_[0]:+.4f}")

sub("Shallow decision tree (max_depth=3, min_samples_leaf=20)")
dt = DecisionTreeClassifier(max_depth=3, min_samples_leaf=20, random_state=42)
dt.fit(X, y)
print(export_text(dt, feature_names=SPINE_COMPS))
dt_cv = cross_val_score(dt, X, y, cv=cv, scoring="roc_auc")
print(f"Decision tree 5-fold ROC-AUC: {dt_cv.mean():.4f} +/- {dt_cv.std():.4f}")

sub("Phase 3 robustness: Regime A -- 5 stable components (sign/magnitude stability)")
A_model = A[STABLE_COMPS + ["outcome"]].dropna(subset=STABLE_COMPS + ["outcome"])
ya  = A_model["outcome"].values.astype(int)
scaler_a = StandardScaler()
Xas = scaler_a.fit_transform(A_model[STABLE_COMPS].values)
lr_a = LogisticRegression(max_iter=1000, C=1.0)
lr_a.fit(Xas, ya)
print(f"Regime A n={len(A_model)}, won={ya.sum()}, lost={len(ya)-ya.sum()}")

# Regime C 5-comp model for side-by-side
C5_model = C[STABLE_COMPS + ["outcome"]].dropna(subset=STABLE_COMPS + ["outcome"])
scaler_c5 = StandardScaler()
Xc5s = scaler_c5.fit_transform(C5_model[STABLE_COMPS].values)
lr_c5 = LogisticRegression(max_iter=1000, C=1.0)
lr_c5.fit(Xc5s, C5_model["outcome"].values.astype(int))
coef_c5 = dict(zip(STABLE_COMPS, lr_c5.coef_[0]))

print(f"\n  {'Component':22s}  {'Regime A':>10}  {'Regime C':>10}  {'Sign stable':>12}")
for comp, coef_a in zip(STABLE_COMPS, lr_a.coef_[0]):
    coef_c = coef_c5[comp]
    stable = "YES" if (coef_a > 0) == (coef_c > 0) else "NO -- FLIPPED"
    print(f"  {comp:22s}  {coef_a:+10.4f}  {coef_c:+10.4f}  {stable:>12}")

# ---------------------------------------------------------------------------
# 5. Phase 4 -- H5: Win Rate diagnostic (Regime C only, bootstrap)
# ---------------------------------------------------------------------------
hdr("PHASE 4 -- H5: WIN RATE COMPONENT DIAGNOSTIC (REGIME C ONLY)")

sub("win_rate score distribution (Regime C)")
wr_vals = C["win_rate"].dropna()
zero_pct = (wr_vals == 0).mean() * 100
print(f"  n={len(wr_vals)}  zeros={zero_pct:.1f}%  mean={wr_vals.mean():.2f}  "
      f"median={wr_vals.median():.2f}  max={wr_vals.max():.2f}")

print("\n  Histogram (approximate):")
for lo, hi in [(0,0), (1,4), (5,9), (10,14), (15,19), (20,25), (25,100)]:
    n = ((wr_vals >= lo) & (wr_vals <= hi)).sum()
    bar = "|" * min(n // 3, 40)
    print(f"    [{lo:2d}-{hi:2d}]  n={n:4d}  {bar}")

sub("H5 core: win_rate=0 (no resolved history) vs win_rate>0")
wr_data = C[C["win_rate"].notna() & C["roi"].notna()].copy()
zero_wr = wr_data[wr_data["win_rate"] == 0]
pos_wr  = wr_data[wr_data["win_rate"] > 0]
print(f"  win_rate=0:   n={len(zero_wr):4d}  mean_ROI={zero_wr.roi.mean():+.4f}  "
      f"WR={zero_wr.outcome.mean()*100:.1f}%")
print(f"  win_rate>0:   n={len(pos_wr):4d}  mean_ROI={pos_wr.roi.mean():+.4f}  "
      f"WR={pos_wr.outcome.mean()*100:.1f}%")
if len(zero_wr) > 1 and len(pos_wr) > 1:
    _, p_mw = stats.mannwhitneyu(zero_wr["roi"].dropna(), pos_wr["roi"].dropna(),
                                  alternative="two-sided")
    print(f"  Mann-Whitney p={p_mw:.4f}  "
          f"{'SIGNIFICANT' if p_mw < 0.05 else 'not significant'}")

def bootstrap_ci(arr, n_boot=2000, ci=0.95):
    if len(arr) < 5:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed=42)
    boots = [rng.choice(arr, size=len(arr), replace=True).mean() for _ in range(n_boot)]
    lo = np.percentile(boots, (1 - ci) / 2 * 100)
    hi = np.percentile(boots, (1 + ci) / 2 * 100)
    return lo, hi

sub("win_rate ROI by value bucket (Regime C, bootstrap 95% CI)")
wr_data["bucket"] = pd.cut(wr_data["win_rate"],
                            bins=[-1, 0, 5, 10, 15, 100],
                            labels=["=0","1-5","6-10","11-15","16+"])
print(f"  {'Bucket':>6}  {'N':>5}  {'WR%':>6}  {'meanROI':>9}  {'95% CI':>22}")
for b in ["=0", "1-5", "6-10", "11-15", "16+"]:
    g = wr_data[wr_data["bucket"] == b]
    if len(g) < 5:
        print(f"  {b:>6}  {len(g):>5}  -- n<5, skip --")
        continue
    lo, hi = bootstrap_ci(g["roi"].values)
    flag = "**" if len(g) < 20 else "  "
    print(f"  {flag}{b:>4}  {len(g):>5}  {g.outcome.mean()*100:5.1f}%  "
          f"{g.roi.mean():+8.4f}  [{lo:+.4f}, {hi:+.4f}]")

sub("H5: 50-resolved-trade cap artifact check (from win_rate_note)")
notes = C["win_rate_note"].fillna("")
trade_counts = []
for note in notes:
    m = re.search(r"(\d+)\s+resolved", str(note))
    if m:
        trade_counts.append(int(m.group(1)))
if trade_counts:
    tc = np.array(trade_counts)
    print(f"  Wallets with parseable resolved count: {len(tc)}")
    print(f"  min={tc.min()}  p25={np.percentile(tc,25):.0f}  "
          f"p50={np.percentile(tc,50):.0f}  p75={np.percentile(tc,75):.0f}  max={tc.max()}")
    n_at_50 = (tc == 50).sum()
    n_near   = (tc >= 48).sum()
    print(f"  n with exactly 50 resolved: {n_at_50}  ({n_at_50/len(tc)*100:.1f}%)")
    print(f"  n with 48-50 resolved (near cap): {n_near}  ({n_near/len(tc)*100:.1f}%)")
else:
    print("  Could not parse trade counts from win_rate_note.")

# ---------------------------------------------------------------------------
# 6. H3: cluster_bonus and convergence_bonus (Regime A only)
# ---------------------------------------------------------------------------
hdr("PHASE 3/H3 -- CLUSTER_BONUS AND CONVERGENCE_BONUS (REGIME A ONLY)")
print("Framing: should we re-enable these? Output is 're-enable and measure in C'")
print("         not a tuned weight.\n")

sub("cluster_bonus: 0 vs 10 (Regime A)")
A_cb = A[A["cluster_bonus"].notna()].copy()
for val in [0, 10]:
    s = A_cb[A_cb["cluster_bonus"] == val]
    if len(s) == 0:
        continue
    wr = s.outcome.mean()*100
    mr = s.roi.mean()
    flag = "**" if len(s) < 20 else "  "
    print(f"  {flag}cluster_bonus={val:2d}  n={len(s):5d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")
cb0  = A_cb[A_cb["cluster_bonus"] == 0]["roi"].dropna()
cb10 = A_cb[A_cb["cluster_bonus"] == 10]["roi"].dropna()
if len(cb0) > 1 and len(cb10) > 1:
    _, p_cb = stats.mannwhitneyu(cb0, cb10, alternative="two-sided")
    print(f"  MW-p={p_cb:.4f}  {'SIGNIFICANT' if p_cb < 0.05 else 'not significant'}")

sub("cluster_bonus coverage by market_type (does it fire universally?)")
for mtype in ["yes_no", "up_down", "over_under", "esports_sports"]:
    s = A[A.market_type == mtype]
    frac = (s["cluster_bonus"] == 10).mean() * 100 if len(s) else 0
    print(f"  {mtype:18s}  n={len(s):5d}  cluster_bonus=10: {frac:.1f}%")

sub("convergence_bonus: absent vs present (Regime A)")
A_cv = A[A["convergence_bonus"].notna()].copy()
A_cv["conv_present"] = A_cv["convergence_bonus"] > 0
for flag_val, label in [(False, "conv=0 (absent) "), (True, "conv>0 (present)")]:
    s = A_cv[A_cv["conv_present"] == flag_val]
    if len(s) == 0:
        continue
    wr = s.outcome.mean()*100
    mr = s.roi.mean()
    flag = "**" if len(s) < 20 else "  "
    print(f"  {flag}{label}  n={len(s):5d}  WR={wr:.1f}%  mean_ROI={mr:+.4f}")
cv0   = A_cv[A_cv["conv_present"] == False]["roi"].dropna()
cvpos = A_cv[A_cv["conv_present"] == True]["roi"].dropna()
if len(cv0) > 1 and len(cvpos) > 1:
    _, p_cv = stats.mannwhitneyu(cv0, cvpos, alternative="two-sided")
    print(f"  MW-p={p_cv:.4f}  {'SIGNIFICANT' if p_cv < 0.05 else 'not significant'}")

sub("H3 (convergence_bonus): ROI by value bucket (Regime A, conv>0 subset)")
print_univariate(A_cv[A_cv["conv_present"]], "convergence_bonus",
                 "Regime A (conv>0 subset)", "H3")

# ---------------------------------------------------------------------------
# 7. Phase 5 -- Proposed reweighting + Regime C validation
# ---------------------------------------------------------------------------
hdr("PHASE 5 -- PROPOSED REWEIGHTING AND REGIME C VALIDATION")

CURRENT_MAX = {
    "timing": 15, "wallet_age": 30, "win_rate": 20,
    "size_anomaly": 15, "concentration": 10, "funding_velocity": 10,
}

sub("Evidence summary per component (Regime C LR coefficients)")
coef_dict = dict(zip(SPINE_COMPS, lr.coef_[0]))
for comp in SPINE_COMPS:
    coef    = coef_dict[comp]
    cur_max = CURRENT_MAX.get(comp, "?")
    dirn    = "POSITIVE" if coef > 0 else "NEGATIVE (inverted signal)"
    print(f"  {comp:22s}  LR_coef={coef:+.4f}  current_max={str(cur_max):>4}  {dirn}")

# Proposed weights: proportional to |coef|, scaled to 100 pts total
raw = {c: abs(coef_dict[c]) for c in SPINE_COMPS}
total_raw = sum(raw.values())
TOTAL_PTS = 100
proposed  = {c: round(raw[c] / total_raw * TOTAL_PTS) for c in SPINE_COMPS}
# Fix rounding to exactly 100
delta = TOTAL_PTS - sum(proposed.values())
if delta != 0:
    adj = max(proposed, key=lambda c: raw[c])
    proposed[adj] += delta

sub("Proposed weights vs current (evidence-driven, sign-corrected)")
print(f"  {'Component':22s}  {'Current':>8}  {'Proposed':>9}  {'Change':>8}  Note")
for comp in sorted(SPINE_COMPS, key=lambda c: -proposed[c]):
    cur  = CURRENT_MAX.get(comp, "?")
    prop = proposed[comp]
    delta_str = f"{prop - cur:+d}" if isinstance(cur, int) else "?"
    coef = coef_dict[comp]
    note = "INVERTED SIGNAL -- consider gating/removing" if coef < 0 else ""
    print(f"  {comp:22s}  {str(cur):>8}  {prop:>9}  {delta_str:>8}  {note}")

print("\n  CAUTION: weights derived from n=789 / 4 days. Treat as directional, not final.")
print("  Inverted-signal components need domain review before re-weighting.")

sub("Regime C validation: current score vs proposed score correlation with ROI")
C_val = C[SPINE_COMPS + ["outcome", "roi", "score"]].dropna(
    subset=SPINE_COMPS + ["outcome"]).copy()
NORM = {c: CURRENT_MAX.get(c, 10) for c in SPINE_COMPS}
C_val["proposed_score"] = sum(
    (C_val[comp] / NORM[comp]).clip(0, 1) * proposed[comp]
    for comp in SPINE_COMPS
)

cur_corr, _  = stats.spearmanr(C_val["score"], C_val["roi"])
prop_corr, _ = stats.spearmanr(C_val["proposed_score"], C_val["roi"])
print(f"\n  Spearman(score, ROI)          = {cur_corr:+.4f}")
print(f"  Spearman(proposed_score, ROI) = {prop_corr:+.4f}")
print(f"  Direction: {'IMPROVED' if prop_corr > cur_corr else 'WORSE or SAME'}")

for score_col, label in [("score", "Current score"), ("proposed_score", "Proposed score")]:
    try:
        C_val["_q"] = pd.qcut(C_val[score_col], q=4, duplicates="drop", labels=False)
        print(f"\n  {label} quartiles vs ROI (Regime C, n={len(C_val)}):")
        for b in sorted(C_val["_q"].dropna().unique()):
            g = C_val[C_val["_q"] == b]
            print(f"    Q{int(b)+1}  n={len(g):4d}  WR={g.outcome.mean()*100:.1f}%  "
                  f"mean_ROI={g.roi.mean():+.4f}")
    except Exception as e:
        print(f"  {label}: {e}")

# ---------------------------------------------------------------------------
# 8. What this analysis cannot tell us
# ---------------------------------------------------------------------------
hdr("WHAT THIS ANALYSIS CANNOT TELL US")
print("""
1. REGIME GENERALIZABILITY: Shippable weights come from n=789 Regime C rows (4 days).
   Any filter, price-band, or scorer change restarts the measurement window.

2. WIN_RATE DATA QUALITY: Regime C win_rate reflects the fixed API endpoint.
   Cache invalidation was rolling; early Regime C rows (May 14 ~03-05 UTC) may
   still carry stale zero scores. The ~8% zero-rate on May 14 is consistent with
   a 2h cache lag, not a structural artifact.

3. CLUSTER/CONVERGENCE SIGNALS: H3 answered on Regime A only (different market
   mix, pre-filter regime). Whether these signals fire and predict in current Regime
   C market composition is unmeasured. Re-enabling requires a live A/B period.

4. ROI IS COUNTERFACTUAL: Assumes execution at bet_price_at_alert, zero slippage.
   Actual bot ROI is lower. Relative component rankings are slippage-robust;
   absolute ROI targets are not.

5. PENDING ROWS: 335 Regime C rows are still pending. If resolution timing
   correlates with component scores, the resolved-only subset is biased.

6. ESPORTS/SPORTS VALIDITY: 42% of scored alerts are match-winner markets.
   Whether funding_velocity, timing, and wallet_age have the same semantic meaning
   on match markets as on crypto binary markets is untested.

7. SMALL CELLS: Any n<20 cell is flagged ** and treated as inconclusive.
""")

hdr("ANALYSIS COMPLETE")
print(f"  Dataset: {CSV}")
print(f"  Regime A resolved: n={len(A)}  |  Regime C resolved: n={len(C)}")
print(f"  Regime C 5-fold CV ROC-AUC: {cv_roc.mean():.4f} +/- {cv_roc.std():.4f}")
