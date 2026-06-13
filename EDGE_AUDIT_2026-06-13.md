# Edge Audit — "Why a 50/50 coin flip, and can it be fixed?"

**Date:** 2026-06-13 · **Wallet:** ~$110 equity ($4.48 free + $106 open MTM, 55 positions)
**Method:** clean CLOB-verified labels on 14,553 resolved alerts (May 2 – Jun 12), strict
train (<Jun 1) / holdout (≥Jun 1) split, 5 parallel analysts + 4 independent verifiers,
cluster-bootstrap CIs by market, Benjamini-Hochberg multiplicity control, ≤5 pre-registered
holdout looks per analyst. Every headline number below was reproduced to the digit by a
verifier who wrote fresh code and never read the analysts'.

---

## Bottom line

**There is no exploitable edge in this signal, at any price band, category, wallet cohort,
time-to-close, or score level. The question "can it be fixed?" — meaning "can it be made to
have a trading edge" — answers NO, with high confidence.** Separately, two *mechanical* leaks
(not edges) were found and one is now fixed in code; they were making a zero-edge strategy
actively lose, but removing them only gets the bot to ~break-even-minus-costs, not to profit.

**Recommendation: do NOT scale capital. Run the bot as the entertainment product it now is
(feed v2), at minimum stakes, with the vig leak plugged — or wind down and recover the ~$110.
Keep the vault sweep disarmed. Re-decide only at the pre-registered n≥200 forward gate.**

---

## 1. Why it's a coin flip — the direct answer

The wallets the bot copies have **no predictive information the market hasn't already priced.**
Across 14,553 clean-labeled alerts:

| Stream | n | Win rate | Mean price | Edge (WR − price) | 95% CI |
|---|---|---|---|---|---|
| Tradeable (first alert per market+side), **TRAIN** | 1,925 | 61.2% | 61.8% | **−0.57pp** | [−1.99, +0.81] |
| Tradeable, **HOLDOUT** (36h-censored) | 1,385 | 61.1% | 60.3% | **+0.83pp** | [−0.67, +2.35] |
| Bot's live filter (score≥65, px≤0.85), TRAIN | 1,314 | 53.3% | 55.2% | −1.92pp | [−3.7, −0.0] |
| Bot's live filter, HOLDOUT | 624 | 58.3% | 56.5% | +1.81pp | [−1.2, +4.7] |

Win rate tracks price almost exactly. Both tradeable-stream CIs straddle zero; the sign even
flips between train and holdout. The bot's own score filter makes it *worse* on train and the
filter effect is itself noise (flips sign on holdout). **The market has already absorbed these
"insider" bets by the time we copy them.**

Supporting findings (all independently re-derived):
- **Wallet skill heterogeneity is exactly zero.** τ² = 0, I² = 0%, hierarchical logit σ̂ = 0
  across 2,222 wallets; split-half edge persistence r = −0.07. There is no subset of "smart
  wallets" hiding inside "all large trades" — the per-wallet edge variance is *under* the
  binomial-luck null. (This replicates the prior 0/190-credible-wallets finding, now on clean
  labels.)
- **The score carries no information.** Price-band-stratified rank AUC(score→outcome) = 0.48
  train / 0.52 holdout — zero-information is not rejected, and the direction is unstable (PR#17
  retuned the formula mid-holdout). Every score component fails BH; the only nominal hits
  (funding +5.7pp, cluster −2.7pp) are inconsistent across streams = noise.
- **No structural cell survives.** 0 of 47 category×band / market-type / time-to-close cells
  survive BH q=0.10 on train. Both prior band hypotheses **failed with sign flips** on holdout
  (.30–.50 came out −2.5pp not positive; ≥.70 came out +2.1pp not negative). The
  favorite-longshot bias is present in shape but not significant after market clustering.
- **Fading doesn't work either.** The Simpson "score anti-predictive above 0.50" hint replicated
  nominally on train (corr −0.05, p=0.04) but the actionable fade rule **lost on holdout**
  (−2.4pp, −4.2pp after costs). There is no regime where betting the opposite is the edge.
- **Selectivity doesn't help.** Of 24 ranking×depth selectivity cells, only "extreme prices
  |px−0.5|≥0.40" survived BH on train (+3.9pp) — and it **failed holdout** (−1.1pp, n collapsed
  to 18) and is just the favorite-longshot microstructure, not insider signal. *Narrowing hard
  does not beat the coin flip, because there is no cell to narrow toward.*

**What would change this conclusion:** a holdout-confirmed, cost-cleared, capacity-material
cell. None exists. The one tantalizing number — June "directional" (one-sided-alert) markets at
**+7.48pp [+4.4,+10.6]** — is a *lookahead artifact*: a market is only "directional" because no
second-side alert ever arrived, which you can't know at trade time. Its causal version ("trade
the first-arriving side") is −0.08pp on train. Confirmed by two verifiers.

---

## 2. Why it was *losing* (not just flat) — two mechanical leaks

A zero-edge strategy should break even minus costs. The bot did worse because of mechanics,
not signal:

**(a) Opposite-side vig — the big one.** 33% of alerted markets get alerts on *both* sides, and
the bot copied both. Holding side A at price pₐ then buying side B at p_b with pₐ+p_b > 1 spends
>$1 to guarantee a $1 return (exactly one side wins) — a **mechanical locked loss** of (pₐ+p_b−1)
per share. Verified: these pair-rows have **WR = 50% by identity** (zero label inconsistency
across 807 pairs), mean price-sum 1.07–1.10, and they are **45–54% of the entire tradeable
stream**, dragging it −3.5 to −4.9pp. This is the single largest controllable bleed.
→ **FIXED in this commit** (see §4). Would have blocked 49/82 executed second-legs lifetime
(~$20.50 locked overround avoided) while still allowing 33 genuine cheap hedges.

**(b) Execution-recording overstatement.** On-chain fills cost ~2% of notional more than the DB
records: ~+1.24pp from `bet_price_filled` being a favorably-rounded quoted price below the true
VWAP, plus ~+0.64pp Polymarket fee embedded in the on-chain cost. So bot-reported P&L overstates
reality by ~1pp of notional per fill. This is a **grading/recording** issue, not execution —
the official forward gate (`forward_checkpoint.py`) already uses on-chain cash truth, so it's
unaffected; only the Telegram "all-time" cosmetic line is rosy. → staged recommendation, §5.

**Not leaks (ruled out):** slippage is ~0pp on average (a non-problem); FOK fill failures are
*capacity* (sports order books don't exist pre-game — 63% of faithful-era attempts 404), not
adverse selection; limit-vs-FOK saves <0.5pp and the momentum signal failed holdout; gas is
relayer-paid. The historical gate-inversion bug (which fed the opposite token's price into the
slippage gate, p≈0.05 "edge" on the blocked flow) is a **forensic artifact of the
already-fixed token[0] bug** — marginal, dollar-fragile, zero realized P&L behind it, not
bankable. A regression guard is added anyway (§4).

---

## 3. The honest verdict on capital

At $110 bankroll, **nothing is worth scaling into.** Every confirmed cell is negative after
costs:
- Whole tradeable stream at the holdout +0.83pp: −$1.0/day after 1pp cost.
- Bot's live filter (train −1.92pp): −$7.6/day if fully traded.
- The prior ".30–.50 positive band" anti-replicated (train −3.0pp): −$10.9/day, ~$330/month bleed.
- The only after-cost-positive candidates are *unconfirmed exploratory* cells (crypto×0.70–0.85,
  crypto 1–7d) with $0.2–3/day ceilings and n≈75 mostly-BTC-strike samples.

Even the lookahead "directional +7.5pp" mirage would be only ~13%/day on $110 — and it isn't
causally capturable. **Scaling a zero-edge book just multiplies the variance and the vig.**

---

## 4. What shipped (this commit) — risk reducers only, no edge claim

1. **Opposite-side vig gate.** Once we hold one side of a market, a *different* side is taken
   only if the two entry prices sum to ≤ `TRADING_OPPOSITE_SIDE_MAX_SUM` (default **0.98**, i.e.
   the pair must lock in a small profit / be a real arb). Pure risk-reducer: only ever blocks,
   never adds; fails safe to "no skip" on missing data; configurable (≥1.0 disables). Tested in
   `test_vig_gate.py`.
2. **Gate-orientation regression guard.** Runtime WARNING if the price-inversion signature
   (current ≈ 1−alert, far from alert) ever reappears — so a silent token-side regression can't
   quietly delete the best flow again. Observability only.

Untouched: sizing, the $5/trade cap, $15 daily-loss, circuit breaker, geoblock pause, vault
sweep. **Capital-at-risk strictly decreases.**

## 5. Staged plan & open recommendations

- **Now:** plug the vig leak (done), keep stakes at $2, keep the vault sweep DISARMED, keep the
  bot running as the feed-v2 entertainment product. This is the lowest-regret state: bounded
  downside, real comedic/spectator value, honest scoreboard.
- **Accounting (staged, not auto-applied to protect live accounting):** grade realized P&L from
  on-chain `usdcSize` (fee-inclusive) instead of `te.bet_price_filled`, to stop the ~2%/fill
  overstatement in the Telegram all-time line. Low priority; the forward gate is already honest.
- **Forward gate (unchanged):** `forward_checkpoint.py` at **n ≥ 200** chain-resolved
  faithful-era bets. Currently n=27 (WR 70.4% vs 64.0% entry, edge +6.4% but Wilson CI
  −12.5%..+20.1% — pure noise). Decision rule stays pre-registered: CONTINUE only if dollar
  ROI>0 AND Wilson-95 edge lower-bound > −2pp; SCALE only at n≥400 with bootstrap CI excluding
  0; otherwise WIND DOWN. **Do not move the goalposts.**
- **If wind-down is chosen:** redeem the 197 unredeemed winners (currently $0 redeemable value —
  they're losers; the 55 open positions hold the $106 MTM), let open positions resolve, sweep
  residual to the vault, stop the trader. The signal has been studied to exhaustion; further
  May-backtests won't find what three clean-data lenses couldn't.

**Confidence:** high on "no edge" (holdout + independent re-derivation + zero wallet
heterogeneity is a strong, convergent result). The only way the picture improves is if the
*now-clean* execution (post all 2026-06-11/13 fixes) shows something the *historical* signal
couldn't — which is exactly what the n≥200 forward gate is for. Until then, treat as no-edge.
