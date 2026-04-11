"""
trade_monitor.py — Real-time trade surveillance via WebSocket + REST fallback.

Primary feed:
  WebSocket wss://ws-subscriptions-clob.polymarket.com/ws/market
  • Subscribe to last_trade_price events per market.
  • Send PING every 10 seconds.
  • Max 5 concurrent connections (each covers multiple markets).
  • Exponential backoff on disconnect.
  • Filter client-side: only forward trades above TRADE_MIN_SIZE_USD.

Fallback feed (when WS is down or market not yet subscribed):
  GET https://data-api.polymarket.com/trades?market={condition_id}&filterType=CASH&filterAmount=500&limit=50
  • Polled every TRADE_POLL_INTERVAL_SECONDS.
  • Deduplication via last-seen trade ID stored in SQLite.
  • No server-side timestamp filter — use offset pagination from last known ID.

Both paths emit Trade objects to a shared asyncio.Queue consumed by main.py.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

import httpx
import websockets
import websockets.exceptions

import config
import database

log = logging.getLogger("trade_monitor")

# ---------------------------------------------------------------------------
# Canonical trade data structure
# ---------------------------------------------------------------------------

@dataclass
class Trade:
    """Normalised trade event from either the WS or REST feed."""
    trade_id: str
    market_id: str           # condition_id
    outcome: str             # "YES" / "NO" or token address
    price: float             # 0.0–1.0
    size_usd: float          # notional USD value
    maker_address: str       # wallet that placed the order (taker side)
    taker_address: str       # counterparty
    timestamp: float         # Unix epoch seconds
    source: str = "ws"       # "ws" | "rest"
    raw: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# HTTP helpers (REST fallback)
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
    backoff = config.HTTP_RETRY_BACKOFF_SECONDS
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            log.warning(
                "Data API HTTP %d on attempt %d/%d: %s",
                exc.response.status_code, attempt, max_retries, url,
            )
            last_exc = exc
        except Exception as exc:
            log.warning(
                "Data API request error attempt %d/%d: %s — %s",
                attempt, max_retries, url, exc,
            )
            last_exc = exc

        if attempt < max_retries:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    raise RuntimeError(
        f"Data API unreachable after {max_retries} attempts: {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Trade parsing — shared between WS and REST paths
# ---------------------------------------------------------------------------

def _parse_trade_from_rest(raw: dict, market_id: str) -> Optional[Trade]:
    """
    Parse a single trade object from the Data API REST response.

    Data API trade schema (representative — validate defensively):
      id, conditionId, outcome, price, size, makerAddress, takerAddress,
      timestamp (milliseconds epoch string or int)
    """
    try:
        trade_id = str(
            raw.get("id") or raw.get("tradeId") or raw.get("transactionHash") or ""
        )
        if not trade_id:
            return None

        price = float(raw.get("price", 0))
        size = float(raw.get("size", 0))

        # Data API returns size in shares; amount/usdcSize are often None.
        # price * size gives USD notional (price is already in USDC per share).
        size_usd = float(raw.get("amount") or raw.get("usdcSize") or (price * size) or 0)

        if size_usd < config.TRADE_MIN_SIZE_USD:
            return None

        # Timestamps: API returns millisecond strings or ints.
        ts_raw = raw.get("timestamp") or raw.get("createdAt") or 0
        try:
            ts = float(ts_raw) / 1000 if float(ts_raw) > 1e10 else float(ts_raw)
        except (TypeError, ValueError):
            ts = time.time()

        outcome = str(raw.get("outcome") or raw.get("side") or "UNKNOWN")

        # Data API uses proxyWallet — fall back to legacy field names.
        wallet = str(
            raw.get("takerAddress") or raw.get("taker")
            or raw.get("proxyWallet")
            or raw.get("makerAddress") or raw.get("maker")
            or ""
        )

        return Trade(
            trade_id=trade_id,
            market_id=market_id,
            outcome=outcome,
            price=price,
            size_usd=size_usd,
            maker_address="",
            taker_address=wallet,
            timestamp=ts,
            source="rest",
            raw=raw,
        )
    except Exception as exc:
        log.debug("Failed to parse REST trade: %s — raw=%s", exc, raw)
        return None


def _parse_trade_from_ws(msg: dict) -> Optional[Trade]:
    """
    Parse a last_trade_price WebSocket message.

    Polymarket WS message shape:
      {
        "event_type": "last_trade_price",
        "asset_id": "<token_id>",     # clobTokenId
        "market": "<condition_id>",
        "price": "0.34",
        "size": "2400",
        "side": "BUY",
        "id": "<trade_id>",
        "timestamp": "1712345678",
        "maker_address": "0x...",
        "taker_address": "0x..."
      }
    """
    try:
        if msg.get("event_type") != "last_trade_price":
            return None

        trade_id = str(
            msg.get("id") or msg.get("trade_id") or msg.get("transaction_hash") or ""
        )
        market_id = str(msg.get("market") or msg.get("condition_id") or "")

        if not trade_id or not market_id:
            return None

        price = float(msg.get("price", 0))
        size = float(msg.get("size", 0))
        size_usd = price * size  # shares * price = USD notional

        if size_usd < config.TRADE_MIN_SIZE_USD:
            log.debug(
                "WS trade filtered (size $%.2f < threshold $%.2f): %s",
                size_usd, config.TRADE_MIN_SIZE_USD, trade_id,
            )
            return None

        ts_raw = msg.get("timestamp") or time.time()
        try:
            ts = float(ts_raw)
            if ts > 1e12:  # milliseconds
                ts /= 1000
        except (TypeError, ValueError):
            ts = time.time()

        outcome = str(msg.get("side") or msg.get("outcome") or "UNKNOWN")

        return Trade(
            trade_id=trade_id,
            market_id=market_id,
            outcome=outcome,
            price=price,
            size_usd=size_usd,
            maker_address=str(msg.get("maker_address") or msg.get("maker") or ""),
            taker_address=str(msg.get("taker_address") or msg.get("taker") or ""),
            timestamp=ts,
            source="ws",
            raw=msg,
        )
    except Exception as exc:
        log.debug("Failed to parse WS trade: %s — msg=%s", exc, msg)
        return None


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------

class WebSocketManager:
    """
    Manages up to WS_MAX_CONNECTIONS WebSocket connections to Polymarket.
    Each connection subscribes to a shard of active markets.
    """

    def __init__(
        self,
        trade_queue: asyncio.Queue,
        get_active_market_ids: Callable[[], list[str]],
        hot_queue: Optional[asyncio.Queue] = None,
    ):
        self._queue = trade_queue
        self._get_markets = get_active_market_ids
        self._hot_queue = hot_queue
        self._tasks: list[asyncio.Task] = []

    async def run(self) -> None:
        """Start and maintain WebSocket connections forever."""
        log.info("WebSocket manager started")
        while True:
            markets = self._get_markets()
            if not markets:
                log.info("No active markets yet — waiting 15s for discovery")
                await asyncio.sleep(15)
                continue

            # Shard markets across max connections.
            shards = _shard_list(markets, config.WS_MAX_CONNECTIONS)
            log.info(
                "Distributing %d markets across %d WebSocket connections",
                len(markets), len(shards),
            )

            # Cancel stale tasks if market list has changed.
            for t in self._tasks:
                if not t.done():
                    t.cancel()
            self._tasks.clear()

            for shard_idx, shard_markets in enumerate(shards):
                task = asyncio.create_task(
                    self._connection_loop(shard_idx, shard_markets),
                    name=f"ws-shard-{shard_idx}",
                )
                self._tasks.append(task)

            # Wait until all tasks die (they run forever with backoff restart),
            # then re-shard with updated market list.
            try:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            except asyncio.CancelledError:
                break

            # Re-evaluate shards every cycle after all tasks complete/crash.
            log.info("All WS tasks ended — re-sharding in 5s")
            await asyncio.sleep(5)

    async def _connection_loop(self, shard_idx: int, market_ids: list[str]) -> None:
        """
        Single WebSocket connection with exponential backoff reconnect logic.
        Subscribes to all markets in `market_ids`.
        """
        backoff = config.WS_RECONNECT_BASE_SECONDS

        while True:
            log.info(
                "[WS shard-%d] Connecting to %s for %d markets",
                shard_idx, config.WS_URL, len(market_ids),
            )
            try:
                await self._run_single_connection(shard_idx, market_ids)
                # If connection ended cleanly, reset backoff.
                backoff = config.WS_RECONNECT_BASE_SECONDS
            except asyncio.CancelledError:
                log.info("[WS shard-%d] Task cancelled — exiting", shard_idx)
                return
            except Exception as exc:
                log.error(
                    "[WS shard-%d] Connection error: %s — reconnecting in %.1fs",
                    shard_idx, exc, backoff,
                )

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX_SECONDS)

    async def _run_single_connection(
        self, shard_idx: int, market_ids: list[str]
    ) -> None:
        """Open a single WebSocket, subscribe, and process messages."""
        async with websockets.connect(
            config.WS_URL,
            ping_interval=None,        # We handle PING manually (10s Polymarket req)
            ping_timeout=None,
            max_size=10 * 1024 * 1024, # 10 MB max message size
        ) as ws:
            log.info("[WS shard-%d] Connected", shard_idx)

            # Subscribe to all token IDs in one message.
            # Polymarket CLOB WS requires {"type":"subscribe","assets_ids":[...]}
            # with CLOB token IDs (NOT condition IDs).
            subscribe_msg = json.dumps({
                "type": "subscribe",
                "assets_ids": market_ids,
            })
            await ws.send(subscribe_msg)
            log.debug("[WS shard-%d] Subscribed to %d token IDs", shard_idx, len(market_ids))

            ping_task = asyncio.create_task(
                self._ping_loop(shard_idx, ws),
                name=f"ws-ping-{shard_idx}",
            )

            try:
                async for raw_message in ws:
                    await self._handle_message(shard_idx, raw_message)
            finally:
                ping_task.cancel()

    async def _ping_loop(self, shard_idx: int, ws: Any) -> None:
        """
        Send a PING every WS_PING_INTERVAL_SECONDS.
        Polymarket closes idle connections after ~15s without a ping.
        """
        while True:
            await asyncio.sleep(config.WS_PING_INTERVAL_SECONDS)
            try:
                await ws.ping()
                log.debug("[WS shard-%d] PING sent", shard_idx)
            except Exception as exc:
                log.warning("[WS shard-%d] PING failed: %s", shard_idx, exc)
                break

    async def _handle_message(self, shard_idx: int, raw: str | bytes) -> None:
        """Parse an incoming WS message and push valid trades to the queue."""
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.debug("[WS shard-%d] Could not parse message: %s", shard_idx, exc)
            return

        # Messages may be single objects or lists of events.
        if isinstance(data, list):
            events = data
        elif isinstance(data, dict):
            events = [data]
        else:
            return

        for event in events:
            # Ignore book snapshots and price_change events — only care about trades.
            if event.get("event_type") != "last_trade_price":
                continue

            market_id = str(event.get("market") or "")
            price = float(event.get("price") or 0)
            size = float(event.get("size") or 0)
            size_usd = price * size

            if not market_id or size_usd < config.TRADE_MIN_SIZE_USD:
                continue

            log.info(
                "[WS shard-%d] Large trade detected: market=%s price=%.3f size=$%.2f "
                "— queuing for REST enrichment",
                shard_idx, market_id, price, size_usd,
            )

            # WS messages don't include wallet addresses. Signal the REST poller
            # to immediately fetch full trade data (with wallets) for this market.
            if self._hot_queue is not None:
                await self._hot_queue.put(market_id)


# ---------------------------------------------------------------------------
# REST polling fallback
# ---------------------------------------------------------------------------

class RestTradePoller:
    """
    Polls the Data API for trades on all active markets.

    Architecture:
      A single shared rate limiter caps total REST requests at 2/sec regardless
      of how many paths are active. This prevents 429s.

      Hot path  — drains hot_queue (markets WS flagged) with priority.
                  Each item is polled as soon as a rate-limit slot is free.
      Background — sequentially walks all active markets as a safety net for
                   anything WS missed. Yields immediately if hot items arrive.

    At 2 req/s the background sweep of 55k markets takes ~7.6 hours, which is
    fine — WS is the real-time detection mechanism; background is just a net.
    """

    # Seconds between any two REST requests (shared across hot + background).
    # 2 req/s = well under Polymarket Data API limits.
    _REQUEST_INTERVAL: float = 0.5

    def __init__(
        self,
        trade_queue: asyncio.Queue,
        hot_queue: Optional[asyncio.Queue] = None,
    ):
        self._queue = trade_queue
        self._hot_queue = hot_queue
        # Single lock enforces serial REST requests at _REQUEST_INTERVAL spacing.
        self._rate_lock = asyncio.Lock()
        self._last_request_at: float = 0.0

    async def _rate_limited_poll(self, client: httpx.AsyncClient, market_id: str) -> None:
        """Acquire rate-limit slot, sleep if needed, then poll."""
        async with self._rate_lock:
            gap = time.monotonic() - self._last_request_at
            if gap < self._REQUEST_INTERVAL:
                await asyncio.sleep(self._REQUEST_INTERVAL - gap)
            try:
                await self._poll_market(client, market_id)
            except Exception as exc:
                log.error("[REST] Poll failed for %s: %s", market_id, exc)
            self._last_request_at = time.monotonic()

    async def run(self) -> None:
        log.info(
            "REST trade poller started (interval=%ds, min_size=$%.0f)",
            config.TRADE_POLL_INTERVAL_SECONDS,
            config.TRADE_MIN_SIZE_USD,
        )
        async with _build_http_client() as client:
            await asyncio.gather(
                self._hot_path_loop(client),
                self._background_scan_loop(client),
            )

    async def _hot_path_loop(self, client: httpx.AsyncClient) -> None:
        """
        Drain hot_queue: poll any market the WebSocket flagged.
        Shares the rate limiter with the background scan so both paths
        combined never exceed _REQUEST_INTERVAL between calls.
        """
        if self._hot_queue is None:
            return

        while True:
            market_id = await self._hot_queue.get()
            log.info("[REST/hot] Enriching WS-flagged market %s", market_id)
            await self._rate_limited_poll(client, market_id)
            self._hot_queue.task_done()

    async def _background_scan_loop(self, client: httpx.AsyncClient) -> None:
        """
        Walk all active markets sequentially as a fallback safety net.
        Pauses between each market to honour the shared rate limit.
        Yields to the hot path naturally since both share _rate_lock.
        """
        while True:
            markets = database.get_all_active_markets()

            if not markets:
                log.debug("[REST/bg] No active markets — waiting 30s")
                await asyncio.sleep(30)
                continue

            log.info("[REST/bg] Starting background sweep of %d markets", len(markets))
            cycle_start = time.monotonic()

            for m in markets:
                await self._rate_limited_poll(client, m["condition_id"])

            elapsed = time.monotonic() - cycle_start
            log.info("[REST/bg] Background sweep done in %.0fs", elapsed)
            # No extra sleep — the per-request interval already paces us.

    async def _poll_market(self, client: httpx.AsyncClient, market_id: str) -> None:
        """
        Fetch recent large trades for one market.
        Compares against last-seen trade ID to avoid re-processing.
        No server-side timestamp filter exists — we use ID-based dedup.
        """
        url = f"{config.DATA_API_BASE}/trades"
        params = {
            "market": market_id,
            "filterType": "CASH",
            "filterAmount": int(config.TRADE_FILTER_AMOUNT),
            "limit": config.TRADE_POLL_LIMIT,
        }

        try:
            data = await _get_with_retry(client, url, params=params)
        except RuntimeError as exc:
            log.error("[REST] Fetch failed for market %s: %s", market_id, exc)
            return

        if not isinstance(data, list):
            # Some responses are wrapped in {"data": [...]}
            if isinstance(data, dict):
                data = data.get("data") or data.get("trades") or []
            else:
                log.warning("[REST] Unexpected response type for market %s", market_id)
                return

        if not data:
            return

        last_seen = database.get_last_seen_trade_id(market_id)
        new_trades = []
        newest_id: Optional[str] = None

        for raw_trade in data:
            trade = _parse_trade_from_rest(raw_trade, market_id)
            if trade is None:
                continue

            # Stop processing once we hit a trade we've seen before.
            if last_seen and trade.trade_id == last_seen:
                log.debug(
                    "[REST] Reached last-seen trade %s for market %s",
                    last_seen, market_id,
                )
                break

            new_trades.append(trade)
            if newest_id is None:
                newest_id = trade.trade_id  # API returns newest first

        if not new_trades:
            log.debug("[REST] No new trades for market %s", market_id)
            return

        log.info(
            "[REST] %d new trades found for market %s (newest: %s)",
            len(new_trades), market_id, newest_id,
        )

        # Persist the newest trade ID for next poll.
        if newest_id:
            database.set_last_seen_trade_id(market_id, newest_id)

        # Enqueue for processing — enqueue oldest first so scorer sees
        # chronological order (API returns newest-first, so reverse).
        for trade in reversed(new_trades):
            await self._queue.put(trade)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _shard_list(items: list, n: int) -> list[list]:
    """Split `items` into at most `n` approximately equal shards."""
    if not items:
        return []
    n = min(n, len(items))
    size = (len(items) + n - 1) // n
    return [items[i:i + size] for i in range(0, len(items), size)]
