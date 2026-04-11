"""
main.py — Orchestrator. Wires all components together and runs the event loop.

Architecture:
  All loops run as asyncio.Task objects. The main coroutine waits for them
  all (they are infinite) so a crash in one is caught and restarted rather
  than silently killing the whole bot.

Tasks:
  1. market_discovery_loop   — Gamma API, every 5 minutes
  2. ws_manager.run          — WebSocket feed (primary), auto-sharded
  3. rest_poller.run         — REST fallback, every 30 seconds
  4. alert_queue.run         — Drain queue → Telegram at ≤1 msg/1.5s
  5. trade_processor_loop    — Consumes trade queue, profiles wallets, scores
  6. resolution_checker_loop — Grades alert outcomes, every 1 hour

Usage:
  python main.py              # Normal operation
  python main.py --dry-run    # Run full pipeline, suppress Telegram sends
"""

import argparse
import asyncio
import logging
import signal
import sys
import time
from typing import Optional

import config
import database
import market_discovery
import resolution_checker
import scorer
import alerter as alerter_module
from alerter import AlertPayload, AlertQueue
from digest import DigestBuffer, digest_loop
from market_discovery import get_market_end_date, get_market_title, get_market_slug
from trade_monitor import Trade, WebSocketManager, RestTradePoller
from wallet_profiler import get_wallet_profile

log = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Trade processing — the core pipeline
# ---------------------------------------------------------------------------

async def process_trade(
    trade: Trade,
    alert_queue: AlertQueue,
    digest_buffer: DigestBuffer,
) -> None:
    """
    Full pipeline for one trade event:
      1. Skip if already alerted (dedup).
      2. Profile the wallet (cache-aware).
      3. Fetch market metadata.
      4. Compute Insider Confidence Score.
      5. If score ≥ threshold, enqueue alert.

    Designed to be fast enough to keep up with real-time WS feed.
    Wallet profiling is the slowest step (~1-3 seconds on first fetch).
    """
    log.info(
        "[Pipeline] Processing trade %s | market=%s outcome=%s "
        "price=%.3f size=$%.2f src=%s",
        trade.trade_id,
        trade.market_id,
        trade.outcome,
        trade.price,
        trade.size_usd,
        trade.source,
    )

    # --- Deduplication ---
    if database.has_alert_been_sent_for_trade(trade.trade_id):
        log.debug("[Pipeline] Trade %s already alerted — skipping", trade.trade_id)
        return

    # --- Determine wallet address ---
    wallet_addr = trade.taker_address or trade.maker_address
    if not wallet_addr:
        log.warning("[Pipeline] Trade %s has no wallet address — skipping", trade.trade_id)
        return

    # --- Wallet profiling (cache-aware, degrades gracefully) ---
    try:
        profile = await get_wallet_profile(wallet_addr)
    except Exception as exc:
        log.error(
            "[Pipeline] Wallet profiling failed for %s: %s — skipping trade %s",
            wallet_addr, exc, trade.trade_id,
        )
        return

    # --- Market metadata ---
    market_title = get_market_title(trade.market_id)
    market_end_date = get_market_end_date(trade.market_id)
    market_slug = get_market_slug(trade.market_id)

    # Compute hours to resolution for the alert formatter too.
    hours_to_resolution: Optional[float] = None
    if market_end_date:
        try:
            from datetime import datetime, timezone
            end_str = market_end_date.replace("Z", "+00:00")
            end_dt = datetime.fromisoformat(end_str)
            now_dt = datetime.now(timezone.utc)
            hours_to_resolution = (end_dt - now_dt).total_seconds() / 3600
        except Exception:
            pass

    # --- Score ---
    try:
        breakdown = scorer.compute_score(
            trade_size_usd=trade.size_usd,
            price=trade.price,
            market_end_date=market_end_date,
            profile=profile,
            current_market_id=trade.market_id,
        )
    except Exception as exc:
        log.error(
            "[Pipeline] Scoring failed for trade %s: %s",
            trade.trade_id, exc, exc_info=True,
        )
        return

    log.info(
        "[Pipeline] Score=%d (instant≥%d, digest≥%d) for trade %s wallet=%s",
        breakdown.total,
        config.ALERT_INSTANT_THRESHOLD,
        config.ALERT_DIGEST_THRESHOLD,
        trade.trade_id,
        wallet_addr,
    )

    # --- Threshold check ---
    if breakdown.total < config.ALERT_DIGEST_THRESHOLD:
        log.info(
            "[Pipeline] Score %d below digest threshold %d — suppressed",
            breakdown.total, config.ALERT_DIGEST_THRESHOLD,
        )
        return

    payload = AlertPayload(
        trade=trade,
        profile=profile,
        breakdown=breakdown,
        market_title=market_title,
        market_end_date=market_end_date,
        hours_to_resolution=hours_to_resolution,
        market_slug=market_slug,
    )

    # --- Route: instant (≥ ALERT_INSTANT_THRESHOLD) or digest (60–79) ---
    if breakdown.total >= config.ALERT_INSTANT_THRESHOLD:
        await alert_queue.enqueue(payload)
        log.info(
            "[Pipeline] INSTANT alert enqueued: score=%d trade=%s market='%s'",
            breakdown.total, trade.trade_id, market_title,
        )
    else:
        # Persist outcome row immediately — same point as instant alerts.
        # The buffer is in-memory; a crash before the 2-hour flush would
        # otherwise lose this trade from alert_outcomes permanently.
        try:
            import json as _json
            database.insert_alert_outcome(
                alert_id=trade.trade_id,
                market_id=trade.market_id,
                market_question=market_title or "",
                wallet_address=wallet_addr,
                score=breakdown.total,
                score_breakdown_json=_json.dumps(breakdown.to_dict()),
                bet_side=trade.outcome or "UNKNOWN",
                bet_price_at_alert=trade.price,
                bet_size_usd=trade.size_usd,
            )
            log.debug(
                "[Pipeline] Outcome row inserted for digest trade %s (score=%d)",
                trade.trade_id, breakdown.total,
            )
        except Exception as exc:
            log.error(
                "[Pipeline] Failed to insert outcome for digest trade %s: %s",
                trade.trade_id, exc,
            )

        await digest_buffer.add(payload)
        log.info(
            "[Pipeline] DIGEST signal buffered: score=%d trade=%s market='%s'",
            breakdown.total, trade.trade_id, market_title,
        )


# ---------------------------------------------------------------------------
# Trade processor loop — consumes from trade_queue
# ---------------------------------------------------------------------------

async def trade_processor_loop(
    trade_queue: asyncio.Queue,
    alert_queue: AlertQueue,
    digest_buffer: DigestBuffer,
) -> None:
    """
    Drain the trade queue and run each trade through the full pipeline.
    Runs tasks concurrently (up to 10) to prevent wallet-profiling latency
    from blocking the queue.
    """
    log.info("[TradeProcessor] Started (concurrency=10)")
    semaphore = asyncio.Semaphore(10)

    async def _bounded(trade: Trade) -> None:
        async with semaphore:
            await process_trade(trade, alert_queue, digest_buffer)

    while True:
        trade = await trade_queue.get()
        asyncio.create_task(_bounded(trade))
        trade_queue.task_done()


# ---------------------------------------------------------------------------
# Heartbeat — periodic health log
# ---------------------------------------------------------------------------

async def heartbeat_loop(interval_seconds: int = 300) -> None:
    """
    Log a stats summary every `interval_seconds` to confirm the bot is alive.
    Railway captures stdout; this makes it easy to see at-a-glance health.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            stats = database.get_stats()
            log.info(
                "[Heartbeat] active_markets=%d cached_wallets=%d "
                "alerts_sent=%d alerts_dry=%d cluster_wallets=%d",
                stats["active_markets"],
                stats["cached_wallets"],
                stats["alerts_sent"],
                stats["alerts_dry_run"],
                stats["flagged_cluster_wallets"],
            )
        except Exception as exc:
            log.error("[Heartbeat] Stats error: %s", exc)


# ---------------------------------------------------------------------------
# Task supervisor — restart crashed tasks
# ---------------------------------------------------------------------------

async def supervised_task(coro_factory, name: str, restart_delay: float = 5.0):
    """
    Run `coro_factory()` as an infinite loop, restarting on unhandled exception.
    Logs crashes with full tracebacks so Railway captures them.
    """
    while True:
        log.info("[Supervisor] Starting task: %s", name)
        try:
            await coro_factory()
        except asyncio.CancelledError:
            log.info("[Supervisor] Task cancelled: %s", name)
            return
        except Exception as exc:
            log.exception(
                "[Supervisor] Task '%s' crashed: %s — restarting in %.1fs",
                name, exc, restart_delay,
            )
            await asyncio.sleep(restart_delay)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def amain(dry_run: bool) -> None:
    """
    Async main. Initialises all components and runs all loops indefinitely.
    """
    log.info("=" * 60)
    log.info("Polymarket Insider Signal Bot starting")
    log.info(
        "dry_run=%s | instant≥%d | digest≥%d | digest_interval=%ds | min_trade=$%.0f",
        dry_run,
        config.ALERT_INSTANT_THRESHOLD,
        config.ALERT_DIGEST_THRESHOLD,
        config.DIGEST_INTERVAL_SECONDS,
        config.TRADE_MIN_SIZE_USD,
    )
    log.info("=" * 60)

    # --- Validate configuration ---
    try:
        config.validate_config()
    except ValueError as exc:
        log.critical("Configuration invalid:\n%s", exc)
        sys.exit(1)

    # --- Initialise database ---
    database.init_db()
    stats = database.get_stats()
    log.info(
        "Database loaded: %d active markets, %d cached wallets, %d prior alerts",
        stats["active_markets"], stats["cached_wallets"], stats["alerts_sent"],
    )

    # --- Shared queues ---
    trade_queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=5000)
    # hot_queue: WS signals large trades here; REST poller drains it immediately.
    hot_queue: asyncio.Queue[str] = asyncio.Queue()

    # --- Components ---
    alert_queue = AlertQueue(dry_run=dry_run)
    digest_buffer = DigestBuffer()

    ws_manager = WebSocketManager(
        trade_queue=trade_queue,
        get_active_market_ids=lambda: [
            token_id
            for m in database.get_all_active_markets()
            for token_id in m["clob_token_ids"]
        ],
        hot_queue=hot_queue,
    )

    rest_poller = RestTradePoller(trade_queue=trade_queue, hot_queue=hot_queue)

    # --- Build tasks ---
    tasks = [
        asyncio.create_task(
            supervised_task(
                market_discovery.market_discovery_loop,
                name="market-discovery",
            ),
            name="market-discovery",
        ),
        asyncio.create_task(
            supervised_task(
                lambda: ws_manager.run(),
                name="websocket-manager",
            ),
            name="websocket-manager",
        ),
        asyncio.create_task(
            supervised_task(
                lambda: rest_poller.run(),
                name="rest-poller",
            ),
            name="rest-poller",
        ),
        asyncio.create_task(
            supervised_task(
                lambda: alert_queue.run(),
                name="alert-queue",
            ),
            name="alert-queue",
        ),
        asyncio.create_task(
            supervised_task(
                lambda: trade_processor_loop(trade_queue, alert_queue, digest_buffer),
                name="trade-processor",
            ),
            name="trade-processor",
        ),
        asyncio.create_task(
            supervised_task(
                resolution_checker.resolution_checker_loop,
                name="resolution-checker",
            ),
            name="resolution-checker",
        ),
        asyncio.create_task(
            supervised_task(
                lambda: digest_loop(digest_buffer, dry_run=dry_run),
                name="digest-loop",
            ),
            name="digest-loop",
        ),
        asyncio.create_task(
            heartbeat_loop(interval_seconds=300),
            name="heartbeat",
        ),
    ]

    log.info("All %d tasks started", len(tasks))

    # --- Graceful shutdown on SIGINT / SIGTERM ---
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler(sig):
        log.info("Received signal %s — initiating graceful shutdown", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler, sig)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals.
            pass

    # Wait until shutdown signal or a task crashes unrecoverably.
    try:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_EXCEPTION,
        )
        # If we get here, a task ended (shouldn't happen with supervised_task).
        for task in done:
            if task.exception():
                log.critical(
                    "Task '%s' exited with unhandled exception: %s",
                    task.get_name(), task.exception(),
                )
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down — cancelling all tasks")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Clean shutdown complete")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket Insider Signal Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                  # Live mode — sends Telegram alerts
  python main.py --dry-run        # Full pipeline, no Telegram messages sent
  DRY_RUN=true python main.py     # Same via env var
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=config.DRY_RUN,
        help="Run the full pipeline but suppress Telegram sends (default: %(default)s)",
    )
    args = parser.parse_args()

    dry_run = args.dry_run

    try:
        asyncio.run(amain(dry_run=dry_run))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as exc:
        log.critical("Fatal error in main: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
