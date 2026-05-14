"""
wallet_profiler.py — Builds a full wallet profile from three sources:

  1. Data API (Polymarket)
       GET /trades?user={address}&limit=10000   → trade history
       GET /positions?user={address}            → open positions
       GET /closed-positions?user={address}     → resolved position wins/losses

  2. Etherscan V2 (Polygon, chainid=137)
       /api?module=account&action=txlist ...    → wallet age, first tx block
       Same endpoint (sort=desc)               → recent activity

  3. Alchemy (optional, cluster detection only)
       alchemy_getAssetTransfers               → funding source tracing

All results are cached in SQLite for WALLET_CACHE_TTL_SECONDS (default 6h).
Failed sub-fetches degrade gracefully: the profile is returned with None
for unavailable fields and the scorer skips those components.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx

import config
import database

log = logging.getLogger("wallet_profiler")

# ---------------------------------------------------------------------------
# Rate limiter for Etherscan (3 calls/sec)
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Token bucket rate limiter. Thread-safe via asyncio lock."""

    def __init__(self, calls_per_second: float):
        self._interval = 1.0 / calls_per_second
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()


_etherscan_limiter = _RateLimiter(config.ETHERSCAN_RATE_LIMIT_CALLS_PER_SEC)


# ---------------------------------------------------------------------------
# Wallet profile data structure
# ---------------------------------------------------------------------------

@dataclass
class WalletProfile:
    address: str

    # --- From Data API ---
    total_trades: int = 0
    resolved_trades: int = 0          # From closed-positions
    win_count: int = 0
    loss_count: int = 0
    win_rate: Optional[float] = None  # None if < 1 resolved trade

    median_bet_usd: Optional[float] = None
    mean_bet_usd: Optional[float] = None
    total_volume_usd: float = 0.0

    open_positions: int = 0           # Count of open positions
    open_markets: list[str] = field(default_factory=list)   # condition_ids

    # Estimated observable capital = sum of open position notional values
    observable_capital_usd: float = 0.0

    # --- From Etherscan V2 ---
    wallet_age_days: Optional[float] = None   # None if Etherscan unavailable
    first_tx_timestamp: Optional[float] = None
    recent_tx_count: int = 0          # Last 100 txs — proxy for activity level

    # --- Cluster detection ---
    cluster_id: Optional[str] = None  # Funding source address if flagged
    in_cluster: bool = False

    # --- Funding velocity ---
    last_inbound_transfer_ts: Optional[float] = None  # Unix ts of most recent inbound transfer

    # --- Metadata ---
    profile_complete: bool = True     # False if any sub-fetch failed
    missing_components: list[str] = field(default_factory=list)
    fetched_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "WalletProfile":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _build_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=config.HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": "polymarket-signal-bot/1.0"},
    )


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
    max_retries: int = config.HTTP_MAX_RETRIES,
    component_name: str = "unknown",
) -> tuple[Any, Optional[str]]:
    """
    GET with retry. Returns (data, error_message).
    error_message is None on success, a string on failure.
    This signature lets callers detect failure without raising.
    """
    backoff = config.HTTP_RETRY_BACKOFF_SECONDS
    last_error: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json(), None
        except httpx.HTTPStatusError as exc:
            last_error = f"HTTP {exc.response.status_code}"
            log.warning(
                "[%s] %s on attempt %d/%d: %s",
                component_name, last_error, attempt, max_retries, url,
            )
        except Exception as exc:
            last_error = str(exc)
            log.warning(
                "[%s] Request error attempt %d/%d: %s — %s",
                component_name, attempt, max_retries, url, exc,
            )

        if attempt < max_retries:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    log.error("[%s] All %d attempts failed for %s", component_name, max_retries, url)
    return None, last_error


# ---------------------------------------------------------------------------
# Data API fetchers
# ---------------------------------------------------------------------------

async def _fetch_trade_history(
    client: httpx.AsyncClient, address: str
) -> tuple[list[dict], Optional[str]]:
    """Fetch all of a wallet's Polymarket trades (up to limit)."""
    url = f"{config.DATA_API_BASE}/trades"
    params = {
        "user": address,
        "limit": config.WALLET_TRADE_HISTORY_LIMIT,
    }
    data, err = await _get_with_retry(client, url, params=params, component_name="trades")
    if err or data is None:
        return [], err

    if isinstance(data, dict):
        data = data.get("data") or data.get("trades") or []
    return data if isinstance(data, list) else [], None


async def _fetch_open_positions(
    client: httpx.AsyncClient, address: str
) -> tuple[list[dict], Optional[str]]:
    """Fetch current open positions."""
    url = f"{config.DATA_API_BASE}/positions"
    params = {"user": address}
    data, err = await _get_with_retry(client, url, params=params, component_name="positions")
    if err or data is None:
        return [], err

    if isinstance(data, dict):
        data = data.get("data") or data.get("positions") or []
    return data if isinstance(data, list) else [], None


async def _fetch_closed_positions(
    client: httpx.AsyncClient, address: str
) -> tuple[list[dict], Optional[str]]:
    """Fetch resolved (closed) positions — source of win/loss data."""
    url = f"{config.DATA_API_BASE}/v1/closed-positions"
    params = {"user": address, "limit": 500}
    data, err = await _get_with_retry(client, url, params=params, component_name="closed-pos")
    if err or data is None:
        return [], err

    if isinstance(data, dict):
        data = data.get("data") or data.get("positions") or []
    return data if isinstance(data, list) else [], None


# ---------------------------------------------------------------------------
# Etherscan V2 fetchers
# ---------------------------------------------------------------------------

async def _fetch_wallet_age(
    client: httpx.AsyncClient, address: str
) -> tuple[Optional[float], Optional[str]]:
    """
    Return (first_tx_unix_timestamp, error) for `address` on Polygon.
    Uses Etherscan V2 with chainid=137.
    Fetches only the first transaction (offset=1, sort=asc).
    """
    if not config.ETHERSCAN_API_KEY:
        return None, "ETHERSCAN_API_KEY not configured"

    await _etherscan_limiter.acquire()

    params = {
        "chainid": config.ETHERSCAN_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "sort": "asc",
        "page": 1,
        "offset": 1,
        "apikey": config.ETHERSCAN_API_KEY,
    }
    data, err = await _get_with_retry(
        client,
        config.ETHERSCAN_BASE_URL,
        params=params,
        component_name="etherscan-age",
    )

    if err or data is None:
        return None, err

    # Etherscan V2 response: {"status":"1","result":[{...}]}
    if data.get("status") != "1":
        msg = data.get("message") or data.get("result") or "no result"
        log.warning("[etherscan-age] Non-success status for %s: %s", address, msg)
        return None, str(msg)

    txs = data.get("result") or []
    if not txs or not isinstance(txs, list):
        return None, "no transactions found"

    try:
        first_tx_ts = float(txs[0].get("timeStamp", 0))
        return first_tx_ts, None
    except (TypeError, ValueError, IndexError) as exc:
        return None, str(exc)


async def _fetch_recent_tx_count(
    client: httpx.AsyncClient, address: str
) -> tuple[int, Optional[str]]:
    """
    Fetch the last 100 Polygon txs to gauge recent activity level.
    Returns the count of those transactions (0–100).
    """
    if not config.ETHERSCAN_API_KEY:
        return 0, "ETHERSCAN_API_KEY not configured"

    await _etherscan_limiter.acquire()

    params = {
        "chainid": config.ETHERSCAN_CHAIN_ID,
        "module": "account",
        "action": "txlist",
        "address": address,
        "sort": "desc",
        "page": 1,
        "offset": 100,
        "apikey": config.ETHERSCAN_API_KEY,
    }
    data, err = await _get_with_retry(
        client,
        config.ETHERSCAN_BASE_URL,
        params=params,
        component_name="etherscan-activity",
    )

    if err or data is None:
        return 0, err

    if data.get("status") != "1":
        return 0, str(data.get("message") or "no result")

    txs = data.get("result") or []
    return len(txs) if isinstance(txs, list) else 0, None


# ---------------------------------------------------------------------------
# Alchemy — cluster / funding-source detection
# ---------------------------------------------------------------------------

async def _trace_funding_source(address: str) -> Optional[str]:
    """
    Use alchemy_getAssetTransfers to find what wallet initially funded `address`.
    Returns the funding wallet address if found, None otherwise.

    This is called ONLY once per new wallet (not on every trade) and only
    when ALCHEMY_RPC_URL is configured.

    We look for the first inbound MATIC or USDC transfer to the wallet.
    The sender of that first funding transfer is the cluster_id.
    """
    if not config.ALCHEMY_RPC_URL:
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "alchemy_getAssetTransfers",
        "params": [{
            "toAddress": address,
            "category": ["external", "erc20"],
            "order": "asc",
            "maxCount": "0x5",        # First 5 inbound transfers
            "withMetadata": False,
        }],
    }

    try:
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as client:
            resp = await client.post(config.ALCHEMY_RPC_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()

        transfers = (
            data.get("result", {}).get("transfers") or []
        )

        if not transfers:
            return None

        # The first inbound transfer's 'from' field is the funding source.
        first_from = transfers[0].get("from") or ""
        if first_from and len(first_from) == 42:
            log.info(
                "[Alchemy] Funding source for %s: %s", address, first_from
            )
            return first_from.lower()

    except Exception as exc:
        log.warning("[Alchemy] Transfer trace failed for %s: %s", address, exc)

    return None


async def _fetch_last_inbound_ts(address: str) -> Optional[float]:
    """
    Fetch the Unix timestamp of the most recent inbound MATIC or USDC transfer
    to `address` via Alchemy's alchemy_getAssetTransfers (order=desc, maxCount=1,
    withMetadata=True).

    Used by scorer.py to compute funding velocity — how quickly this wallet
    received external funds before placing the bet. A short gap is a strong
    insider signal (funded and deployed immediately).

    Returns None if Alchemy is unconfigured or the call fails.
    """
    if not config.ALCHEMY_RPC_URL:
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "alchemy_getAssetTransfers",
        "params": [{
            "toAddress": address,
            "category": ["external", "erc20"],
            "order": "desc",
            "maxCount": "0x1",       # Only the most recent transfer
            "withMetadata": True,    # Required to get blockTimestamp
        }],
    }

    try:
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as client:
            resp = await client.post(config.ALCHEMY_RPC_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()

        transfers = data.get("result", {}).get("transfers") or []
        if not transfers:
            return None

        # metadata.blockTimestamp is ISO-8601 string when withMetadata=True
        ts_str = (transfers[0].get("metadata") or {}).get("blockTimestamp")
        if not ts_str:
            return None

        from datetime import datetime, timezone
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()

    except Exception as exc:
        log.warning("[Alchemy] Last inbound ts fetch failed for %s: %s", address, exc)
        return None


# ---------------------------------------------------------------------------
# Profile assembly
# ---------------------------------------------------------------------------

def _compute_win_rate(closed_positions: list[dict]) -> tuple[int, int, Optional[float]]:
    """
    Count wins and losses from closed positions.
    v1/closed-positions schema: {"realizedPnl": float, "curPrice": float, ...}
    Returns (win_count, loss_count, win_rate_or_None).
    """
    wins = 0
    losses = 0

    for pos in closed_positions:
        # v1 API uses realizedPnl (positive = won, negative = lost)
        pnl = pos.get("realizedPnl")
        if pnl is None:
            pnl = pos.get("pnl") if pos.get("pnl") is not None else pos.get("profit")

        if pnl is not None:
            try:
                wins += 1 if float(pnl) > 0 else 0
                losses += 1 if float(pnl) <= 0 else 0
                continue
            except (TypeError, ValueError):
                pass

        # Fallback: curPrice (resolved winner token = 1.0, loser = 0.0)
        cur_price = pos.get("curPrice")
        if cur_price is not None:
            try:
                wins += 1 if float(cur_price) >= 0.999 else 0
                losses += 1 if float(cur_price) < 0.999 else 0
                continue
            except (TypeError, ValueError):
                pass

        # Legacy fallback: compare outcome vs resolved outcome
        resolved = pos.get("resolvedOutcome") or pos.get("resolved_outcome") or ""
        outcome = pos.get("outcome") or pos.get("side") or ""
        if resolved and outcome:
            if resolved.upper() == outcome.upper():
                wins += 1
            else:
                losses += 1

    total = wins + losses
    rate = wins / total if total > 0 else None
    return wins, losses, rate


def _compute_median(values: list[float]) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
    return sorted_vals[mid]


def _extract_trade_sizes(trades: list[dict]) -> list[float]:
    """Pull USD trade sizes from the Data API trade objects."""
    sizes = []
    for t in trades:
        size_usd = None
        # Try multiple field names defensively.
        for field_name in ("amount", "usdcSize", "size"):
            raw = t.get(field_name)
            if raw is not None:
                try:
                    v = float(raw)
                    if field_name == "size":
                        price = float(t.get("price", 1.0) or 1.0)
                        v = v * price  # shares × price = USD
                    size_usd = v
                    break
                except (TypeError, ValueError):
                    continue
        if size_usd and size_usd > 0:
            sizes.append(size_usd)
    return sizes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_wallet_profile(address: str) -> WalletProfile:
    """
    Return a full WalletProfile for `address`.

    Checks SQLite cache first (6-hour TTL). On cache miss, fetches from all
    sources in parallel, assembles the profile, and writes it to cache.

    Failed sub-fetches are logged and noted in missing_components.
    They do NOT raise — the scorer degrades gracefully.
    """
    address = address.lower()

    # --- Cache check ---
    cached = database.get_cached_wallet_profile(address)
    if cached is not None:
        log.info("[WalletProfiler] Cache hit for %s", address)
        return WalletProfile.from_dict(cached)

    log.info("[WalletProfiler] Fetching profile for %s", address)

    profile = WalletProfile(address=address)
    missing: list[str] = []

    async with _build_client() as client:
        # --- Fire all Data API calls in parallel ---
        trades_task = asyncio.create_task(_fetch_trade_history(client, address))
        positions_task = asyncio.create_task(_fetch_open_positions(client, address))
        closed_task = asyncio.create_task(_fetch_closed_positions(client, address))
        age_task = asyncio.create_task(_fetch_wallet_age(client, address))
        activity_task = asyncio.create_task(_fetch_recent_tx_count(client, address))

        trades_raw, trades_err = await trades_task
        positions_raw, positions_err = await positions_task
        closed_raw, closed_err = await closed_task
        first_tx_ts, age_err = await age_task
        recent_tx_count, activity_err = await activity_task

    # --- Trade history ---
    if trades_err:
        log.error("[WalletProfiler] Trade history failed for %s: %s", address, trades_err)
        missing.append("trade_history")
    else:
        sizes = _extract_trade_sizes(trades_raw)
        profile.total_trades = len(trades_raw)
        profile.total_volume_usd = sum(sizes)
        profile.median_bet_usd = _compute_median(sizes)
        profile.mean_bet_usd = sum(sizes) / len(sizes) if sizes else None
        log.debug(
            "[WalletProfiler] %s: %d trades, median=$%.2f",
            address, profile.total_trades, profile.median_bet_usd or 0,
        )

    # --- Open positions ---
    if positions_err:
        log.error("[WalletProfiler] Open positions failed for %s: %s", address, positions_err)
        missing.append("open_positions")
    else:
        profile.open_positions = len(positions_raw)
        profile.open_markets = [
            p.get("conditionId") or p.get("condition_id") or ""
            for p in positions_raw
            if p.get("conditionId") or p.get("condition_id")
        ]
        # Observable capital: sum of current position values
        total_cap = 0.0
        for pos in positions_raw:
            val = pos.get("value") or pos.get("currentValue") or 0
            try:
                total_cap += float(val)
            except (TypeError, ValueError):
                pass
        profile.observable_capital_usd = total_cap
        log.debug(
            "[WalletProfiler] %s: %d open positions, capital=$%.2f",
            address, profile.open_positions, profile.observable_capital_usd,
        )

    # --- Closed positions (win/loss) ---
    if closed_err:
        log.error("[WalletProfiler] Closed positions failed for %s: %s", address, closed_err)
        missing.append("closed_positions")
    else:
        wins, losses, rate = _compute_win_rate(closed_raw)
        profile.resolved_trades = wins + losses
        profile.win_count = wins
        profile.loss_count = losses
        profile.win_rate = rate
        log.debug(
            "[WalletProfiler] %s: %d resolved, %d wins (%.1f%%)",
            address, profile.resolved_trades, wins, (rate or 0) * 100,
        )

    # --- Wallet age (Etherscan V2) ---
    if age_err:
        log.warning("[WalletProfiler] Wallet age unavailable for %s: %s", address, age_err)
        missing.append("wallet_age")
    elif first_tx_ts:
        profile.first_tx_timestamp = first_tx_ts
        age_days = (time.time() - first_tx_ts) / 86400
        profile.wallet_age_days = age_days
        log.debug("[WalletProfiler] %s: wallet age=%.1f days", address, age_days)

    # --- Recent activity ---
    if activity_err:
        log.debug("[WalletProfiler] Recent activity unavailable for %s: %s", address, activity_err)
        # Not critical — don't add to missing_components
    else:
        profile.recent_tx_count = recent_tx_count

    # --- Cluster detection (Alchemy) ---
    existing_cluster = database.get_cluster_id_for_wallet(address)
    if existing_cluster:
        profile.cluster_id = existing_cluster
        profile.in_cluster = True
        log.info("[WalletProfiler] %s already in cluster %s", address, existing_cluster)
    else:
        funding_source = await _trace_funding_source(address)
        if funding_source and funding_source != address:
            # Check if the funding source itself is already flagged.
            source_cluster = database.get_cluster_id_for_wallet(funding_source)
            cluster_key = source_cluster or funding_source

            database.flag_wallet_cluster(address, cluster_key)
            database.flag_wallet_cluster(funding_source, cluster_key)

            profile.cluster_id = cluster_key
            profile.in_cluster = True
            log.info(
                "[WalletProfiler] New cluster: %s funded by %s (cluster_id=%s)",
                address, funding_source, cluster_key,
            )

    # --- Last inbound transfer timestamp (for funding velocity scoring) ---
    last_inbound_ts = await _fetch_last_inbound_ts(address)
    if last_inbound_ts is not None:
        profile.last_inbound_transfer_ts = last_inbound_ts
        log.debug(
            "[WalletProfiler] %s: last inbound transfer ts=%.0f (%.1f days ago)",
            address, last_inbound_ts, (time.time() - last_inbound_ts) / 86400,
        )

    # --- Finalize ---
    if missing:
        profile.profile_complete = False
        profile.missing_components = missing

    # Persist to cache
    database.save_wallet_profile(address, profile.to_dict())

    log.info(
        "[WalletProfiler] Profile complete for %s: trades=%d, win_rate=%s, "
        "age=%s days, cluster=%s, missing=%s",
        address,
        profile.total_trades,
        f"{profile.win_rate:.1%}" if profile.win_rate is not None else "N/A",
        f"{profile.wallet_age_days:.0f}" if profile.wallet_age_days is not None else "N/A",
        profile.cluster_id or "None",
        missing or "none",
    )

    return profile
