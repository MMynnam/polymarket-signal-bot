"""
resolution_checker.py — Background task that grades fired alerts once markets resolve.

Design:
  • Runs as a supervised asyncio.Task alongside the other bot loops.
  • Each cycle (default: every 1 hour) fetches all pending outcome rows,
    groups them by market_id to minimise Gamma API calls, and checks each
    unique market for resolution via GET /markets?conditionIds={id}.
  • When a market is resolved, every pending alert on that market is graded
    won/lost/invalid and its ROI is computed.
  • All Gamma API calls reuse the _build_http_client / _get_with_retry helpers
    from market_discovery so error-handling behaviour is identical.
  • Failures on individual markets are logged and skipped; the loop never
    crashes — it retries the same pending rows next cycle.

Resolution schema (Gamma /markets endpoint):
  closed          : bool   — market is closed for trading
  outcomes        : list   — outcome names, e.g. ["Yes", "No"]
  outcomePrices   : list   — parallel float strings; winning outcome ≈ 1.0,
                             losing ≈ 0.0.  All zeros → market voided.
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Optional

import config
import database
from market_discovery import _build_http_client, _get_with_retry

log = logging.getLogger("resolution_checker")

# A price this high on one outcome is treated as a decisive resolution.
_WIN_PRICE_THRESHOLD: float = 0.99

# If every outcomePrices entry is at or below this, the market is voided.
_VOID_MAX_PRICE: float = 0.01


# ---------------------------------------------------------------------------
# Resolution parsing helpers
# ---------------------------------------------------------------------------

def _parse_price_list(raw: object) -> list[float]:
    """
    Parse outcomePrices from the Gamma API.
    The field arrives as a Python list of stringified floats
    (e.g. ["0.999998", "0.000001"]) — handle both list and JSON-string forms.
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw:
        try:
            result.append(float(item))
        except (ValueError, TypeError):
            pass
    return result


def _parse_outcome_list(raw: object) -> list[str]:
    """Parse the outcomes array, handling both list and JSON-string forms."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(raw, list):
        return []
    return [str(o) for o in raw]


def _determine_resolution(
    market_data: dict,
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Inspect a Gamma market dict and return (status, winning_outcome, roi_multiplier).

    Returns:
        (None, None, None)                          — market not yet resolved; skip
        ("resolved_won",  winning_outcome, roi)     — bet_side won
        ("resolved_lost", winning_outcome, -1.0)    — bet_side lost
        ("resolved_invalid", None, 0.0)             — voided / refunded

    Note: ROI is computed by the caller once we know the bet_side.
    This function only determines the winning outcome; ROI calculation
    happens in _grade_alert() so the price is taken into account.
    """
    if not market_data.get("closed", False):
        return None, None, None

    prices   = _parse_price_list(market_data.get("outcomePrices", []))
    outcomes = _parse_outcome_list(market_data.get("outcomes", []))

    if not prices or not outcomes or len(prices) != len(outcomes):
        # Insufficient data — treat as unresolved for now.
        return None, None, None

    max_price = max(prices)

    if max_price >= _WIN_PRICE_THRESHOLD:
        winning_idx     = prices.index(max_price)
        winning_outcome = outcomes[winning_idx]
        return "resolved", winning_outcome, None  # ROI computed per-alert below

    if max_price <= _VOID_MAX_PRICE:
        # All prices near zero — market voided / refunded.
        return "resolved_invalid", None, 0.0

    # Prices are neither decisive nor voided — market is in dispute or pending UMA.
    return None, None, None


def _grade_alert(
    bet_side: str,
    bet_price: float,
    winning_outcome: Optional[str],
    base_status: str,
) -> tuple[str, Optional[float]]:
    """
    Given a base resolution status and the winning outcome, compute the
    final status and ROI for a specific alert.

    ROI formula:
      Won:     (1 / bet_price_at_alert) - 1
      Lost:    -1.0
      Invalid: 0.0  (assume full stake refund)
    """
    if base_status == "resolved_invalid":
        return "resolved_invalid", 0.0

    if winning_outcome is None:
        return "resolved_invalid", 0.0

    won = bet_side.strip().lower() == winning_outcome.strip().lower()

    if won:
        roi = (1.0 / bet_price) - 1.0 if bet_price > 0 else 0.0
        return "resolved_won", roi
    else:
        return "resolved_lost", -1.0


# ---------------------------------------------------------------------------
# Per-market resolution check
# ---------------------------------------------------------------------------

async def _check_market(
    client,
    market_id: str,
    pending_alerts: list[dict],
) -> int:
    """
    Fetch resolution data for one market and grade all pending alerts on it.
    Returns the number of alerts graded (0 if market not yet resolved).
    """
    url = f"{config.GAMMA_API_BASE}/markets"
    try:
        data = await _get_with_retry(client, url, params={"conditionIds": market_id})
    except RuntimeError as exc:
        log.error(
            "[ResolutionChecker] Gamma API failed for market %s: %s — will retry next cycle",
            market_id, exc,
        )
        return 0

    # /markets returns a list; take the first matching entry.
    if isinstance(data, list):
        if not data:
            log.debug("[ResolutionChecker] Empty response for market %s", market_id)
            return 0
        market_data = data[0]
    elif isinstance(data, dict):
        market_data = data
    else:
        log.warning(
            "[ResolutionChecker] Unexpected response shape for market %s: %s",
            market_id, type(data),
        )
        return 0

    base_status, winning_outcome, _ = _determine_resolution(market_data)

    if base_status is None:
        log.debug("[ResolutionChecker] Market %s not yet resolved", market_id)
        return 0

    resolved_at = int(time.time())
    graded = 0

    for alert in pending_alerts:
        alert_id  = alert["alert_id"]
        bet_side  = alert["bet_side"]
        bet_price = float(alert["bet_price_at_alert"])

        status, roi = _grade_alert(bet_side, bet_price, winning_outcome, base_status)

        try:
            database.update_outcome_resolution(
                alert_id=alert_id,
                status=status,
                winning_outcome=winning_outcome,
                roi=roi,
                resolved_at=resolved_at,
            )
            log.info(
                "[ResolutionChecker] Graded alert %s — market=%s bet=%s status=%s roi=%s",
                alert_id, market_id, bet_side, status,
                f"{roi:+.2f}" if roi is not None else "N/A",
            )
            graded += 1
        except Exception as exc:
            log.error(
                "[ResolutionChecker] Failed to update outcome for alert %s: %s",
                alert_id, exc,
            )

    return graded


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def resolution_checker_loop() -> None:
    """
    Long-running coroutine. Each cycle fetches pending outcome rows, groups
    them by market, and grades resolved markets via the Gamma API.
    Designed to run as an asyncio.Task under the supervised_task wrapper.
    """
    log.info(
        "[ResolutionChecker] Started (interval=%ds)",
        config.RESOLUTION_CHECK_INTERVAL_SECONDS,
    )

    async with _build_http_client() as client:
        while True:
            cycle_start = time.monotonic()
            log.info("[ResolutionChecker] Starting resolution cycle...")

            try:
                await _run_cycle(client)
            except Exception as exc:
                log.exception("[ResolutionChecker] Unhandled error in cycle: %s", exc)

            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(
                0.0,
                config.RESOLUTION_CHECK_INTERVAL_SECONDS - elapsed,
            )
            log.debug("[ResolutionChecker] Sleeping %.1fs until next cycle", sleep_secs)
            await asyncio.sleep(sleep_secs)


async def _run_cycle(client) -> None:
    """Execute one resolution-check cycle."""
    pending = database.get_pending_outcomes()

    if not pending:
        log.info("[ResolutionChecker] No pending outcomes — nothing to check")
        return

    log.info(
        "[ResolutionChecker] %d pending alert(s) across markets — checking...",
        len(pending),
    )

    # Group alerts by market_id to batch API calls.
    by_market: dict[str, list[dict]] = defaultdict(list)
    for alert in pending:
        by_market[alert["market_id"]].append(alert)

    total_graded = 0
    for market_id, alerts in by_market.items():
        graded = await _check_market(client, market_id, alerts)
        total_graded += graded
        # Brief pause between markets to be polite to the Gamma API.
        await asyncio.sleep(0.25)

    log.info(
        "[ResolutionChecker] Cycle complete: %d/%d alert(s) graded",
        total_graded, len(pending),
    )
