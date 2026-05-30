"""
resolution_checker.py — Background task that grades fired alerts once markets resolve.

Design:
  • Runs as a supervised asyncio.Task alongside the other bot loops.
  • Each cycle (default: every 1 hour) fetches all pending outcome rows,
    groups them by market_id to minimise Gamma API calls, and checks each
    unique market for resolution.
  • Resolution data comes from the LOCAL markets table, which is refreshed
    continuously by market_discovery_loop(). This avoids direct Gamma API
    calls for each market and sidesteps the Gamma conditionIds lookup bug
    (see NOTE below).
  • For markets that are past their end_date but whose local raw_json doesn't
    yet have closed=True (stale snapshot), we also try prices-based resolution
    directly from outcomePrices, and fall back to a targeted Gamma refresh
    to fetch the current closed state.
  • All failures are logged and skipped — the loop retries the same pending
    rows next cycle.

NOTE: The Gamma GET /markets?conditionIds={id} endpoint returns incorrect
markets for some conditionIds (returns a completely different market with a
different conditionId). We therefore do NOT use it as the primary resolution
source. The local markets table (populated by market_discovery via the events
endpoint) is reliable and preferred. Targeted Gamma refreshes are attempted
as a fallback for stale markets, with a conditionId verification step to
catch and discard wrong responses.

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
from datetime import datetime, timedelta, timezone
from typing import Optional

import config
import database
from market_discovery import _build_http_client, _get_with_retry, _parse_market

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


def _is_market_past_end_date(end_date_str: Optional[str], days: float = 0) -> bool:
    """Return True if the market's end_date is more than `days` days in the past."""
    if not end_date_str:
        return False
    try:
        end_str = end_date_str.replace("Z", "+00:00")
        end_dt = datetime.fromisoformat(end_str)
        return datetime.now(timezone.utc) > end_dt + timedelta(days=days)
    except Exception:
        return False


def _determine_resolution(
    market_data: dict,
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Inspect a Gamma market dict and return (status, winning_outcome, roi_multiplier).
    Requires closed=True to proceed — use _determine_resolution_by_prices() for
    markets that are past their end_date but whose snapshot predates the close.

    Returns:
        (None, None, None)                       — not yet resolved; skip
        ("resolved", winning_outcome, None)      — bet_side won (ROI computed per-alert)
        ("resolved_invalid", None, 0.0)          — voided / refunded
    """
    if not market_data.get("closed", False):
        return None, None, None

    return _resolve_from_prices(market_data)


def _determine_resolution_by_prices(
    market_data: dict,
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    Like _determine_resolution() but does NOT require closed=True.

    Used for markets that are past their end_date — at that point, outcomePrices
    reflect the final settled state even if the closed flag wasn't captured in
    our local snapshot (which was stored while the market was still open).
    """
    return _resolve_from_prices(market_data)


def _resolve_from_prices(
    market_data: dict,
) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """Shared price-inspection logic for both _determine_resolution variants."""
    prices   = _parse_price_list(market_data.get("outcomePrices", []))
    outcomes = _parse_outcome_list(market_data.get("outcomes", []))

    if not prices or not outcomes or len(prices) != len(outcomes):
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
# Targeted Gamma refresh (fallback for stale local data)
# ---------------------------------------------------------------------------

async def _refresh_market_from_gamma(
    client,
    market_id: str,
) -> Optional[dict]:
    """
    Fetch one specific market from Gamma and update the local DB.
    Returns the updated local market dict, or None if the fetch failed or Gamma
    returned a market with a different conditionId.

    Strategy: prefer the reliable by-id endpoint (/markets/{gamma_id}) using the
    numeric Gamma id stored in the local snapshot's raw_json. The legacy
    ?conditionIds= query returns the WRONG market for many ids (known Gamma bug:
    it ignores the filter and returns an unrelated, still-open market), which is
    why stale resolved markets never refreshed. The conditionIds query is kept
    only as a last-resort fallback, and every response is conditionId-verified.
    """
    # Pull the numeric Gamma id from the local snapshot (set by market_discovery).
    gamma_id = None
    local = database.get_market(market_id)
    if local:
        rj = local.get("raw_json") or {}
        if isinstance(rj, str):
            try:
                rj = json.loads(rj)
            except (ValueError, TypeError):
                rj = {}
        if isinstance(rj, dict):
            gamma_id = rj.get("id")

    market_data = None

    # Primary: reliable by-id lookup.
    if gamma_id:
        try:
            data = await _get_with_retry(
                client, f"{config.GAMMA_API_BASE}/markets/{gamma_id}"
            )
            if isinstance(data, list):
                market_data = data[0] if data else None
            elif isinstance(data, dict):
                market_data = data
        except RuntimeError as exc:
            log.warning(
                "[ResolutionChecker] Gamma by-id refresh failed for %s (id=%s): %s",
                market_id, gamma_id, exc,
            )

    # Fallback: legacy conditionIds query (unreliable; verified below).
    if market_data is None:
        try:
            data = await _get_with_retry(
                client, f"{config.GAMMA_API_BASE}/markets",
                params={"conditionIds": market_id},
            )
        except RuntimeError as exc:
            log.warning(
                "[ResolutionChecker] Gamma refresh failed for %s: %s", market_id, exc
            )
            return None
        if isinstance(data, list):
            market_data = data[0] if data else None
        elif isinstance(data, dict):
            market_data = data
        if market_data is None:
            return None

    # Verify Gamma returned the market we asked for.
    returned_id = (
        market_data.get("conditionId")
        or market_data.get("condition_id")
        or ""
    )
    if returned_id and returned_id.lower() != market_id.lower():
        log.warning(
            "[ResolutionChecker] Gamma returned wrong conditionId for %s (got %s) "
            "— ignoring response, using local data only",
            market_id, returned_id,
        )
        return None

    parsed = _parse_market(market_data)
    if not parsed:
        return None

    try:
        database.upsert_market(
            condition_id=parsed["condition_id"],
            title=parsed["title"],
            clob_token_ids=parsed["clob_token_ids"],
            end_date=parsed["end_date"],
            raw_json=parsed["raw"],
            active=parsed["active"],
        )
        log.debug(
            "[ResolutionChecker] Refreshed market %s from Gamma (closed=%s)",
            market_id, market_data.get("closed"),
        )
        return database.get_market(market_id)
    except Exception as exc:
        log.error(
            "[ResolutionChecker] Failed to store refreshed market %s: %s",
            market_id, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Per-market resolution check
# ---------------------------------------------------------------------------

async def _check_market(
    client,
    market_id: str,
    pending_alerts: list[dict],
) -> int:
    """
    Check one market for resolution using local DB data, falling back to a
    targeted Gamma refresh for stale snapshots.
    Returns the number of alerts graded (0 if market not yet resolved).

    Resolution cascade:
      1. Try standard resolution from local raw_json (requires closed=True).
      2. If past end_date: try prices-based resolution (closed flag not required).
      3. If still unresolved and past end_date by >1 day: targeted Gamma refresh,
         then re-apply steps 1+2 on the refreshed data.
      4. If >30 days past end_date with no resolution: mark resolved_invalid.
    """
    # Gamma conditionIds lookup returns incorrect markets for some IDs.
    # Using local markets table (refreshed by market_discovery) instead.
    market = database.get_market(market_id)

    if market is None:
        log.warning(
            "[ResolutionChecker] Market %s not found in local DB — skipping",
            market_id,
        )
        return 0

    raw_json      = market.get("raw_json", {})
    end_date_str  = market.get("end_date")

    # Step 1: standard resolution (requires closed=True in raw_json).
    base_status, winning_outcome, _ = _determine_resolution(raw_json)

    # Step 2: past end_date → try prices directly (closed flag not required).
    if base_status is None and _is_market_past_end_date(end_date_str, days=0):
        base_status, winning_outcome, _ = _determine_resolution_by_prices(raw_json)
        if base_status is not None:
            log.info(
                "[ResolutionChecker] Market %s resolved from prices (past end_date, "
                "closed flag not set in local snapshot)",
                market_id,
            )

    # Step 3: still unresolved and >1 day past end_date → try Gamma refresh.
    if base_status is None and _is_market_past_end_date(end_date_str, days=1):
        log.info(
            "[ResolutionChecker] Market %s unresolved >1 day past end_date — "
            "attempting targeted Gamma refresh",
            market_id,
        )
        refreshed = await _refresh_market_from_gamma(client, market_id)
        if refreshed:
            refreshed_raw = refreshed.get("raw_json", {})
            base_status, winning_outcome, _ = _determine_resolution(refreshed_raw)
            if base_status is None:
                base_status, winning_outcome, _ = _determine_resolution_by_prices(
                    refreshed_raw
                )

    # Step 4: very stale market (>30 days past end_date) with no resolution data.
    if base_status is None and _is_market_past_end_date(end_date_str, days=30):
        log.warning(
            "[ResolutionChecker] Market %s is >30 days past end_date with no "
            "resolution data — marking resolved_invalid",
            market_id,
        )
        base_status    = "resolved_invalid"
        winning_outcome = None

    if base_status is None:
        log.debug("[ResolutionChecker] Market %s not yet resolved", market_id)
        return 0

    resolved_at = int(time.time())
    graded = 0

    for alert in pending_alerts:
        alert_id  = alert["alert_id"]
        bet_side  = alert.get("bet_side") or "UNKNOWN"
        try:
            bet_price = float(alert["bet_price_at_alert"])
        except (TypeError, ValueError):
            bet_price = 0.0

        status, roi = _grade_alert(bet_side, bet_price, winning_outcome, base_status)

        alert_created_at = alert.get("created_at") or resolved_at
        latency_hours = (resolved_at - alert_created_at) / 3600.0

        try:
            database.update_outcome_resolution(
                alert_id=alert_id,
                status=status,
                winning_outcome=winning_outcome,
                roi=roi,
                resolved_at=resolved_at,
                resolution_latency_hours=latency_hours,
            )
            log.info(
                "[ResolutionChecker] Graded alert %s — market=%s bet=%s "
                "status=%s roi=%s",
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
    them by market, and grades resolved markets using local DB data.
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

    # Group alerts by market_id so we do one DB lookup per unique market.
    by_market: dict[str, list[dict]] = defaultdict(list)
    for alert in pending:
        by_market[alert["market_id"]].append(alert)

    total_graded = 0
    for market_id, alerts in by_market.items():
        graded = await _check_market(client, market_id, alerts)
        total_graded += graded
        # Small pause between markets; only matters when Gamma refresh is triggered.
        await asyncio.sleep(0.1)

    log.info(
        "[ResolutionChecker] Cycle complete: %d/%d alert(s) graded",
        total_graded, len(pending),
    )
