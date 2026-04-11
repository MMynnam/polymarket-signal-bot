"""
market_discovery.py — Polls the Gamma API every 5 minutes to discover and
register active Polymarket markets.

Endpoint: GET https://gamma-api.polymarket.com/events
Extracts per market: conditionId, clobTokenIds, endDate, title, active status.
Stores everything in SQLite via database.py.
"""

import asyncio
import logging
import time
from typing import Any, Optional

import httpx

import config
import database

log = logging.getLogger("market_discovery")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _build_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": "polymarket-signal-bot/1.0"},
    )


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
    max_retries: int = config.HTTP_MAX_RETRIES,
) -> Any:
    """
    GET `url` with exponential backoff retry. Returns parsed JSON or raises
    after max_retries exhausted.
    """
    backoff = config.HTTP_RETRY_BACKOFF_SECONDS
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            log.warning(
                "Gamma API HTTP %d on attempt %d/%d: %s",
                exc.response.status_code, attempt, max_retries, url,
            )
            last_exc = exc
        except (httpx.RequestError, Exception) as exc:
            log.warning(
                "Gamma API request error on attempt %d/%d: %s — %s",
                attempt, max_retries, url, exc,
            )
            last_exc = exc

        if attempt < max_retries:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    raise RuntimeError(
        f"Gamma API unreachable after {max_retries} attempts: {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Market parsing
# ---------------------------------------------------------------------------

def _parse_market(market: dict) -> Optional[dict]:
    """
    Extract the fields we care about from a Gamma 'markets' sub-object.
    Returns None if the market is missing critical fields.

    Gamma nests markets inside events. Each event can have multiple markets.
    """
    condition_id = market.get("conditionId") or market.get("condition_id")
    if not condition_id:
        return None

    # clobTokenIds can come as a JSON string or a list depending on Gamma version.
    clob_raw = market.get("clobTokenIds") or market.get("clob_token_ids") or []
    if isinstance(clob_raw, str):
        import json as _json
        try:
            clob_token_ids = _json.loads(clob_raw)
        except Exception:
            clob_token_ids = [clob_raw]
    else:
        clob_token_ids = list(clob_raw)

    return {
        "condition_id": condition_id,
        "clob_token_ids": clob_token_ids,
        "title": market.get("question") or market.get("title") or "",
        "end_date": market.get("endDate") or market.get("end_date"),
        "active": market.get("active", True),
        "raw": market,
    }


def _parse_events_response(data: Any) -> list[dict]:
    """
    Flatten a Gamma /events response into a list of parsed market dicts.

    Gamma returns either:
      • A list of event objects, each with a "markets" list, OR
      • A dict with a "data" key containing the list above.
    """
    if isinstance(data, dict):
        data = data.get("data") or data.get("events") or []

    if not isinstance(data, list):
        log.error("Unexpected Gamma response type: %s", type(data))
        return []

    parsed = []
    for event in data:
        event_slug = event.get("slug") or ""
        # Each event contains a list of markets.
        markets_raw = event.get("markets") or []
        for m in markets_raw:
            # Inject event slug so we can build a valid Polymarket URL later.
            m["_event_slug"] = event_slug
            result = _parse_market(m)
            if result:
                parsed.append(result)

    return parsed


# ---------------------------------------------------------------------------
# Core fetch function
# ---------------------------------------------------------------------------

async def fetch_and_store_markets(client: httpx.AsyncClient) -> int:
    """
    Fetch all active markets from Gamma and upsert them into SQLite.
    Returns the number of markets processed.
    """
    url = f"{config.GAMMA_API_BASE}/events"
    params = {
        "active": "true",
        "closed": "false",
        "limit": config.GAMMA_MARKETS_LIMIT,
        "offset": 0,
    }

    all_markets: list[dict] = []
    page = 0

    # Gamma uses offset pagination. Keep fetching until we get fewer results
    # than the page limit (meaning we've hit the last page).
    while True:
        params["offset"] = page * config.GAMMA_MARKETS_LIMIT
        log.debug("Fetching Gamma events page %d (offset=%d)", page, params["offset"])

        try:
            data = await _get_with_retry(client, url, params=params)
        except RuntimeError as exc:
            log.error("Market discovery fetch failed: %s", exc)
            break

        batch = _parse_events_response(data)
        log.debug("Gamma page %d returned %d markets", page, len(batch))

        if not batch:
            break

        all_markets.extend(batch)

        # Stop paginating if we got a partial page (last page).
        if len(batch) < config.GAMMA_MARKETS_LIMIT:
            break

        page += 1

    # Upsert everything into SQLite.
    count = 0
    for m in all_markets:
        try:
            database.upsert_market(
                condition_id=m["condition_id"],
                title=m["title"],
                clob_token_ids=m["clob_token_ids"],
                end_date=m["end_date"],
                raw_json=m["raw"],
                active=m["active"],
            )
            count += 1
        except Exception as exc:
            log.error(
                "Failed to upsert market %s: %s", m.get("condition_id"), exc
            )

    log.info(
        "Market discovery: %d markets upserted (total active in DB: %d)",
        count,
        len(database.get_all_active_markets()),
    )
    return count


# ---------------------------------------------------------------------------
# Background polling loop
# ---------------------------------------------------------------------------

async def market_discovery_loop() -> None:
    """
    Long-running coroutine. Polls Gamma every MARKET_DISCOVERY_INTERVAL_SECONDS.
    Designed to run as an asyncio.Task alongside the WebSocket monitor.
    Logs a heartbeat on each cycle so Railway stdout shows the bot is alive.
    """
    log.info(
        "Market discovery loop started (interval=%ds)",
        config.MARKET_DISCOVERY_INTERVAL_SECONDS,
    )

    async with _build_http_client() as client:
        while True:
            cycle_start = time.monotonic()
            log.info("[MarketDiscovery] Starting discovery cycle...")

            try:
                n = await fetch_and_store_markets(client)
                elapsed = time.monotonic() - cycle_start
                log.info(
                    "[MarketDiscovery] Cycle complete: %d markets in %.1fs", n, elapsed
                )
            except Exception as exc:
                log.exception("[MarketDiscovery] Unhandled error in cycle: %s", exc)

            # Sleep until next cycle, accounting for time already spent.
            elapsed = time.monotonic() - cycle_start
            sleep_secs = max(
                0.0,
                config.MARKET_DISCOVERY_INTERVAL_SECONDS - elapsed,
            )
            log.debug("[MarketDiscovery] Sleeping %.1fs until next cycle", sleep_secs)
            await asyncio.sleep(sleep_secs)


# ---------------------------------------------------------------------------
# Lookup helpers used by other modules
# ---------------------------------------------------------------------------

def get_market_end_date(condition_id: str) -> Optional[str]:
    """
    Return the endDate string for a market (ISO-8601), or None.
    Used by scorer.py for time-to-resolution calculation.
    """
    market = database.get_market(condition_id)
    if market is None:
        return None
    return market.get("end_date")


def get_market_title(condition_id: str) -> str:
    """Return the market question/title, falling back to the condition ID."""
    market = database.get_market(condition_id)
    if market is None:
        return condition_id
    return market.get("title") or condition_id


def get_market_slug(condition_id: str) -> Optional[str]:
    """
    Return the event-level slug for building a Polymarket URL.
    Polymarket URLs are https://polymarket.com/event/{event_slug}.
    The event slug is injected as _event_slug during discovery; falls back
    to the market's own slug for single-market events where they match.
    """
    market = database.get_market(condition_id)
    if market is None:
        return None
    raw = market.get("raw_json", {})
    return raw.get("_event_slug") or raw.get("slug") or None
