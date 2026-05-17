"""
scorer.py — Insider Confidence Score (0–100).

Each component is isolated in its own function with detailed comments
explaining the weight rationale and the math used.

All max-point values are env-driven (set via Railway env vars; see config.py).
Defaults below reflect the May 2026 evidence-backed reweight.

Score layout (env-driven defaults):
  Component              Max pts  Rationale
  ─────────────────────  ───────  ──────────────────────────────────────────
  Size anomaly              33    Strongest cross-regime predictor (backtest)
  Timing                    20    Positive in current regime; partial cut from 25
  Win rate                  15    Restored gradation via API cap fix + wider band
  Funding velocity          10    Borderline positive; held
  Concentration             10    Noise in current regime; held
  Wallet age                12    Inverted signal in current regime; cut sharply
  Underdog bet               0    DISABLED — 128-alert backtest: 14% WR, -0.60 ROI
  Cluster bonus              0    DISABLED — fired universally, no information
  Convergence bonus          0    DISABLED — anti-predictive in production data
  ─────────────────────  ───────
  TOTAL (max)              100

Score >= ALERT_INSTANT_THRESHOLD (default 65) -> immediate Telegram alert.
Score >= ALERT_DIGEST_THRESHOLD  (default 60) -> buffered into periodic digest.

Rollback: set env vars SCORE_MAX_TIMING=25, SCORE_MAX_WALLET_AGE=25,
SCORE_MAX_WIN_RATE=10, SCORE_MAX_SIZE_ANOMALY=20, SCORE_MAX_FUNDING_VELOCITY=10,
SCORE_MAX_CONCENTRATION=10, WINRATE_HIGH_THRESHOLD=0.80, WINRATE_LOW_THRESHOLD=0.50
and redeploy. No code change required.
"""

import logging
import math
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import config
from wallet_profiler import WalletProfile

log = logging.getLogger("scorer")


# ---------------------------------------------------------------------------
# Score breakdown data structure
# ---------------------------------------------------------------------------

@dataclass
class ScoreBreakdown:
    total: int = 0

    # Individual component scores (None = component skipped / data unavailable)
    timing: Optional[int] = None
    funding_velocity: Optional[int] = None
    win_rate: Optional[int] = None
    size_anomaly: Optional[int] = None
    wallet_age: Optional[int] = None
    concentration: Optional[int] = None
    underdog: Optional[int] = None
    cluster_bonus: int = 0
    convergence_bonus: int = 0
    size_anomaly_multiple: Optional[float] = None

    # Human-readable notes for each component (shown in alert)
    timing_note: str = ""
    funding_velocity_note: str = ""
    win_rate_note: str = ""
    size_anomaly_note: str = ""
    wallet_age_note: str = ""
    concentration_note: str = ""
    underdog_note: str = ""
    cluster_note: str = "No"
    convergence_note: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Component 1 — Timing (0–25 pts)
# ---------------------------------------------------------------------------

def _score_timing(
    hours_to_resolution: Optional[float],
    max_pts: int = config.SCORE_MAX_TIMING,
) -> tuple[int, str]:
    """
    Non-linear decay curve: score drops steeply as distance from resolution
    grows. We use an exponential decay so that:

      • 0–2 hours out  → 25 pts (near-max conviction window)
      • 24 hours out   → ~13 pts
      • 7 days out     → ~0 pts

    Formula: score = max_pts × exp(−λ × hours) clamped to [0, max_pts]
    where λ is derived from the constraint that at TIMING_ZERO_SCORE_HOURS
    the score should be ≈ 0 (specifically ≤ 0.5 pts).

    Rationale: An insider placing a bet in the final 2 hours before resolution
    has much less time for the market to correct, implying maximum conviction
    and/or private information. A bet placed 7 days out is far less unusual.
    """
    if hours_to_resolution is None:
        return 0, "N/A (no end date)"

    if hours_to_resolution < 0:
        # Market already closed — score 0, this trade may be stale data.
        return 0, "Market past end date"

    # If we're inside the "near-max" window, give full score.
    if hours_to_resolution <= config.TIMING_MAX_SCORE_HOURS:
        return max_pts, f"{hours_to_resolution:.1f}h to close (FINAL WINDOW)"

    # λ such that at TIMING_ZERO_SCORE_HOURS the score = 0.5 / max_pts
    # i.e. exp(-λ × zero_hours) = 0.5 / max_pts
    # → λ = -ln(0.5 / max_pts) / zero_hours
    zero_hours = config.TIMING_ZERO_SCORE_HOURS  # 168h (7 days)
    lam = -math.log(0.5 / max_pts) / zero_hours

    raw = max_pts * math.exp(-lam * hours_to_resolution)
    score = max(0, min(max_pts, round(raw)))

    if hours_to_resolution < 24:
        note = f"{hours_to_resolution:.1f}h to close"
    elif hours_to_resolution < 168:
        note = f"{hours_to_resolution / 24:.1f}d to close"
    else:
        note = f"{hours_to_resolution / 24:.0f}d to close"

    return score, note


# ---------------------------------------------------------------------------
# Component 2 — Funding Velocity (0–20 pts)
# ---------------------------------------------------------------------------

def _score_funding_velocity(
    trade_timestamp: Optional[float],
    profile: WalletProfile,
    max_pts: int = config.SCORE_MAX_FUNDING_VELOCITY,
) -> tuple[int, str]:
    """
    Measures the gap between the wallet's most recent inbound transfer and
    the trade timestamp. A short gap means the wallet was funded and deployed
    rapidly — a hallmark of purpose-built insider accounts.

    Gap = trade_timestamp − last_inbound_transfer_ts

      • Gap ≤ FUNDING_VELOCITY_FAST_HOURS (1h) → max_pts
      • Gap > FUNDING_VELOCITY_SLOW_HOURS (7 days) → 0 pts
      • Between: linear decay

    Rationale: Insiders often fund a fresh wallet, place the bet immediately,
    and withdraw. The funding-to-bet velocity is a direct proxy for
    premeditation. Wallets where funds sat for weeks before the bet are less
    suspicious (could be long-term traders). Wallets funded within an hour
    of the bet are highly anomalous.

    Graceful degradation: if Alchemy is not configured or the fetch failed,
    profile.last_inbound_transfer_ts will be None and this component returns 0.
    """
    if trade_timestamp is None or profile.last_inbound_transfer_ts is None:
        return 0, "N/A (no Alchemy data)"

    gap_seconds = trade_timestamp - profile.last_inbound_transfer_ts
    if gap_seconds < 0:
        # Transfer timestamped after the trade — data anomaly, skip.
        return 0, "N/A (transfer after trade)"

    gap_hours = gap_seconds / 3600
    fast = config.FUNDING_VELOCITY_FAST_HOURS   # 1.0h
    slow = config.FUNDING_VELOCITY_SLOW_HOURS   # 168.0h (7 days)

    if gap_hours <= fast:
        score = max_pts
        note = f"Funded {gap_hours * 60:.0f}m before bet (RAPID)"
    elif gap_hours >= slow:
        score = 0
        note = f"Funded {gap_hours / 24:.0f}d before bet"
    else:
        score = round(max_pts * (1 - (gap_hours - fast) / (slow - fast)))
        score = max(0, min(max_pts, score))
        if gap_hours < 24:
            note = f"Funded {gap_hours:.1f}h before bet"
        else:
            note = f"Funded {gap_hours / 24:.1f}d before bet"

    return score, note


# ---------------------------------------------------------------------------
# Component 3 — Win Rate (0–10 pts)
# ---------------------------------------------------------------------------

def _score_win_rate(
    profile: WalletProfile,
    max_pts: int = config.SCORE_MAX_WIN_RATE,
) -> tuple[int, str]:
    """
    Historical win rate on resolved bets, weighted by sample size.

      • win_rate ≥ WINRATE_HIGH_THRESHOLD (80%) → max_pts (full weight)
      • win_rate ≤ WINRATE_LOW_THRESHOLD  (50%) → 0 pts
      • Between: linear interpolation
      • Sample size < WINRATE_SIGNIFICANCE_BETS (20): score scaled by
        (resolved_trades / significance_bets) so thin histories don't
        dominate the signal.

    Rationale: A wallet with 86% win rate across 30 resolved trades has
    demonstrated systematic edge. One lucky win from 2 trades does not.
    """
    if profile.win_rate is None or profile.resolved_trades == 0:
        return 0, "N/A (no resolved trades)"

    high = config.WINRATE_HIGH_THRESHOLD  # 0.80
    low  = config.WINRATE_LOW_THRESHOLD   # 0.50
    sig  = config.WINRATE_SIGNIFICANCE_BETS  # 20

    wr = profile.win_rate
    if wr <= low:
        raw_score = 0
    elif wr >= high:
        raw_score = max_pts
    else:
        raw_score = round(max_pts * (wr - low) / (high - low))

    # Downweight thin samples.
    weight = min(1.0, profile.resolved_trades / sig)
    score = max(0, min(max_pts, round(raw_score * weight)))

    note = (
        f"{wr:.0%} on {profile.resolved_trades} resolved "
        f"({'full weight' if weight >= 1.0 else f'{weight:.0%} weight'})"
    )
    return score, note


# ---------------------------------------------------------------------------
# Component 5 — Size Anomaly (0–20 pts)
# ---------------------------------------------------------------------------

def _score_size_anomaly(
    trade_size_usd: float,
    profile: WalletProfile,
    max_pts: int = config.SCORE_MAX_SIZE_ANOMALY,
) -> tuple[int, str, Optional[float]]:
    """
    Measures how abnormal this bet is relative to the wallet's historical
    median bet size.

    Multiple = trade_size_usd / median_bet_usd
      • Multiple ≥ SIZE_ANOMALY_HIGH_MULTIPLE (5×) → max_pts
      • Multiple < SIZE_ANOMALY_LOW_MULTIPLE (1.5×) → 0 pts
      • Between: linear interpolation

    Special case — first large bet (no history):
      If median_bet_usd is None (no trade history), we cannot compare but
      the fact that the bet crosses our $500 threshold with no history
      is itself suspicious. We award a fixed 12/20 pts.

    Rationale: A wallet that typically bets $100 dropping $5,000 on one
    outcome is a strong signal. Wallets that consistently bet large amounts
    are less remarkable (though still scored on other components).
    """
    if "trade_history" in profile.missing_components:
        return 0, "N/A (data unavailable)", None

    if profile.median_bet_usd is None or profile.median_bet_usd == 0:
        # First large bet — suspicious but we have no baseline.
        note = f"${trade_size_usd:,.0f} (no prior history)"
        return 12, note, None

    multiple = trade_size_usd / profile.median_bet_usd
    low = config.SIZE_ANOMALY_LOW_MULTIPLE    # 1.5×
    high = config.SIZE_ANOMALY_HIGH_MULTIPLE  # 5×

    if multiple < low:
        score = 0
    elif multiple >= high:
        score = max_pts
    else:
        score = round(max_pts * (multiple - low) / (high - low))

    score = max(0, min(max_pts, score))
    note = (
        f"${trade_size_usd:,.0f} vs median ${profile.median_bet_usd:,.0f} "
        f"({multiple:.1f}x)"
    )
    return score, note, multiple


# ---------------------------------------------------------------------------
# Component 6 — Wallet Age (0–25 pts)
# ---------------------------------------------------------------------------

def _score_wallet_age(
    profile: WalletProfile,
    max_pts: int = config.SCORE_MAX_WALLET_AGE,
) -> tuple[int, str]:
    """
    Inverted scoring: newer wallets score higher.

    Rationale: Wallets created within 30 days of a significant bet are
    often purpose-built for a single tip or are "burner" proxy wallets
    associated with insiders who do not want to contaminate their main
    trading history. Old wallets with consistent history are less likely
    to be fresh insider instruments (though not impossible).

    Curve:
      age ≤ WALLET_AGE_NEW_DAYS (30d)  → max_pts
      age ≥ WALLET_AGE_OLD_DAYS (365d) → 0 pts
      Between: linear decay
    """
    if "wallet_age" in profile.missing_components or profile.wallet_age_days is None:
        return 0, "N/A (Etherscan unavailable)"

    age = profile.wallet_age_days
    new_thresh = config.WALLET_AGE_NEW_DAYS    # 30 days
    old_thresh = config.WALLET_AGE_OLD_DAYS    # 365 days

    if age <= new_thresh:
        score = max_pts
        label = "NEW"
    elif age >= old_thresh:
        score = 0
        label = f"{age:.0f}d old"
    else:
        # Linear decay from max_pts to 0 as age goes from new_thresh to old_thresh.
        score = round(max_pts * (1 - (age - new_thresh) / (old_thresh - new_thresh)))
        score = max(0, min(max_pts, score))
        label = f"{age:.0f}d old"

    note = f"{age:.0f} days ({label})"
    return score, note


# ---------------------------------------------------------------------------
# Component 7 — Concentration (0–10 pts)
# ---------------------------------------------------------------------------

def _score_concentration(
    trade_size_usd: float,
    profile: WalletProfile,
    max_pts: int = config.SCORE_MAX_CONCENTRATION,
) -> tuple[int, str]:
    """
    What fraction of the wallet's observable capital is going into this
    single bet?

    concentration = trade_size_usd / observable_capital_usd
      • ≥ CONCENTRATION_HIGH_PCT (70%) → max_pts
      • < CONCENTRATION_LOW_PCT (10%)  → 0 pts
      • Between: linear

    If observable_capital is 0 (no open positions data), we fall back to
    scoring based solely on whether this is a large absolute bet vs.
    the wallet's total historical volume.

    Rationale: An insider who bets 80% of their total visible capital on
    one outcome is demonstrating extreme conviction. Diversified bettors are
    less likely to be acting on a specific tip.
    """
    if "open_positions" in profile.missing_components:
        return 0, "N/A (position data unavailable)"

    # If we have no observable capital at all, use total volume as denominator.
    capital = profile.observable_capital_usd
    denominator_label = "open capital"

    if capital <= 0:
        if profile.total_volume_usd > 0:
            capital = profile.total_volume_usd
            denominator_label = "historical volume"
        else:
            # Genuinely can't compute this — first bet, no data.
            return 5, "First bet / no capital baseline"

    pct = trade_size_usd / capital
    low = config.CONCENTRATION_LOW_PCT    # 0.10
    high = config.CONCENTRATION_HIGH_PCT  # 0.70

    if pct < low:
        score = 0
    elif pct >= high:
        score = max_pts
    else:
        score = round(max_pts * (pct - low) / (high - low))

    score = max(0, min(max_pts, score))
    note = f"{pct:.0%} of {denominator_label} (${capital:,.0f})"
    return score, note


# ---------------------------------------------------------------------------
# Component 8 — Underdog Bet — DISABLED (always 0 pts)
# ---------------------------------------------------------------------------

def _score_underdog(
    price: float,
    max_pts: int = config.SCORE_MAX_UNDERDOG,
) -> tuple[int, str]:
    # Disabled: 128-alert backtest showed 14% win rate, -0.60 ROI.
    # Underdog price correlated with losses, not insider edge.
    # Original scoring logic preserved below for reference.
    implied_prob = round(max(0.0, min(1.0, price)) * 100)
    return 0, f"{implied_prob}% implied (disabled)"

    # --- Original logic (disabled) ---
    # price = max(0.0, min(1.0, price))
    # low_price = config.UNDERDOG_MAX_PRICE   # 0.30
    # high_price = config.UNDERDOG_MIN_PRICE  # 0.60
    # if price <= low_price:
    #     score = max_pts
    #     note = f"${price:.2f} ({round(price*100)}% implied — UNDERDOG)"
    # elif price >= high_price:
    #     score = 0
    #     note = f"${price:.2f} ({round(price*100)}% implied — favorite)"
    # else:
    #     score = round(max_pts * (1 - (price - low_price) / (high_price - low_price)))
    #     score = max(0, min(max_pts, score))
    #     note = f"${price:.2f} ({round(price*100)}% implied)"
    # return score, note


# ---------------------------------------------------------------------------
# Component 7 — Cluster Bonus (0 or +10)
# ---------------------------------------------------------------------------

def _score_cluster(
    profile: WalletProfile,
    bonus_pts: int = config.SCORE_CLUSTER_BONUS,
) -> tuple[int, str]:
    """
    Binary bonus: if Alchemy transfer tracing identified this wallet as
    belonging to a cluster of wallets funded from the same source,
    add a flat bonus.

    Rationale: Coordinated wallets placing similar bets across multiple
    accounts is a classic pattern for insiders trying to avoid position
    size limits or detection. A single wallet may not look anomalous;
    a cluster of 3–5 wallets all hitting the same market in the same hour is.

    This is the only component with an additive bonus (vs. a capped range)
    because cluster membership is a categorical signal, not a continuous one.
    It can push a score above 100 in theory; we clamp total to 110 max.
    """
    if profile.in_cluster and profile.cluster_id:
        note = f"YES — cluster {profile.cluster_id[:10]}..."
        return bonus_pts, note
    return 0, "No"


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------

def compute_score(
    trade_size_usd: float,
    price: float,
    market_end_date: Optional[str],
    profile: WalletProfile,
    current_market_id: Optional[str] = None,
    trade_timestamp: Optional[float] = None,
) -> ScoreBreakdown:
    """
    Compute the full Insider Confidence Score for a single trade + wallet.

    Parameters
    ----------
    trade_size_usd : float
        USD notional of the trade being scored.
    price : float
        CLOB price of the outcome that was purchased (0.0–1.0).
    market_end_date : str | None
        ISO-8601 datetime string for market resolution. None degrades timing
        component gracefully.
    profile : WalletProfile
        Pre-built wallet profile (from wallet_profiler.get_wallet_profile).
    current_market_id : str | None
        The conditionId of the market being bet on. Used for concentration
        component to subtract current position if already held.
    trade_timestamp : float | None
        Unix timestamp of the trade. Used with profile.last_inbound_transfer_ts
        to compute funding velocity. Degrades gracefully if None.

    Returns
    -------
    ScoreBreakdown
        Dataclass with total score and all per-component details.
    """
    breakdown = ScoreBreakdown()

    # --- Compute hours to resolution ---
    hours_to_resolution: Optional[float] = None
    if market_end_date:
        try:
            from datetime import datetime, timezone

            # Handle both "2024-11-05T00:00:00Z" and "2024-11-05T00:00:00+00:00"
            end_str = market_end_date.replace("Z", "+00:00")
            end_dt = datetime.fromisoformat(end_str)
            now_dt = datetime.now(timezone.utc)
            delta_seconds = (end_dt - now_dt).total_seconds()
            hours_to_resolution = delta_seconds / 3600
        except Exception as exc:
            log.warning("Failed to parse market end_date '%s': %s", market_end_date, exc)

    # --- Score each component ---

    breakdown.timing, breakdown.timing_note = _score_timing(hours_to_resolution)
    log.debug("Timing score: %d — %s", breakdown.timing, breakdown.timing_note)

    breakdown.funding_velocity, breakdown.funding_velocity_note = _score_funding_velocity(
        trade_timestamp, profile
    )
    log.debug("Funding velocity score: %d — %s", breakdown.funding_velocity, breakdown.funding_velocity_note)

    breakdown.win_rate, breakdown.win_rate_note = _score_win_rate(profile)
    log.debug("Win rate score: %d — %s", breakdown.win_rate, breakdown.win_rate_note)

    breakdown.size_anomaly, breakdown.size_anomaly_note, breakdown.size_anomaly_multiple = (
        _score_size_anomaly(trade_size_usd, profile)
    )
    log.debug("Size anomaly score: %d — %s", breakdown.size_anomaly, breakdown.size_anomaly_note)

    breakdown.wallet_age, breakdown.wallet_age_note = _score_wallet_age(profile)
    log.debug("Wallet age score: %d — %s", breakdown.wallet_age, breakdown.wallet_age_note)

    breakdown.concentration, breakdown.concentration_note = _score_concentration(
        trade_size_usd, profile
    )
    log.debug("Concentration score: %d — %s", breakdown.concentration, breakdown.concentration_note)

    breakdown.underdog, breakdown.underdog_note = _score_underdog(price)
    log.debug("Underdog score: %d — %s", breakdown.underdog, breakdown.underdog_note)

    breakdown.cluster_bonus, breakdown.cluster_note = _score_cluster(profile)
    log.debug("Cluster bonus: %d — %s", breakdown.cluster_bonus, breakdown.cluster_note)

    # --- Sum components (treat None as 0 for degraded components) ---
    component_sum = (
        (breakdown.timing or 0)
        + (breakdown.funding_velocity or 0)
        + (breakdown.win_rate or 0)
        + (breakdown.size_anomaly or 0)
        + (breakdown.wallet_age or 0)
        + (breakdown.concentration or 0)
        + (breakdown.underdog or 0)
        + breakdown.cluster_bonus
    )

    # Clamp to 0–110 (100 max from components + 10 cluster bonus).
    # Convergence bonus (+0–20) is applied in main.py after this, clamping to 130.
    breakdown.total = max(0, min(110, component_sum))

    log.info(
        "Score computed: %d/110 (cluster: +%d) | "
        "timing=%s funding=%s winrate=%s size=%s age=%s conc=%s",
        breakdown.total,
        breakdown.cluster_bonus,
        breakdown.timing,
        breakdown.funding_velocity,
        breakdown.win_rate,
        breakdown.size_anomaly,
        breakdown.wallet_age,
        breakdown.concentration,
    )

    return breakdown
