"""
api.py — FastAPI app exposing alert data for the remote Fly.io trader.

Endpoints:
  GET  /api/alerts/tradeable             — alerts ready for execution (not yet traded)
  POST /api/trades                       — accept a trade execution report
  GET  /api/stats/trading                — trading stats for remote risk management
  GET  /api/trades/pending               — pending trades + current resolution status
  GET  /api/trades/recent-losses         — N most recently resolved-lost trades (for notifications)
  GET  /api/trades/resolved-recent       — won/lost trades since ?since=<ts> (CB backfill on restart)
  GET  /api/positions/open               — currently open (filled, unresolved) positions
  GET  /api/stats/vault                  — vault sweep history (count, total, last timestamp)
  PATCH /api/trades/{alert_id}/resolution — remote trader reports a resolution
  POST /api/skips/telemetry              — record a slippage-skip event with shadow counterfactual

Authentication: every request must include X-API-Key matching API_SECRET_KEY.
Fail-closed: if API_SECRET_KEY is empty, all requests are refused with 403.
"""

import logging
import time
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

import config
import database

log = logging.getLogger("api")

app = FastAPI(title="Polymarket Signal Bot API", docs_url=None, redoc_url=None)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


def _verify_api_key(api_key: str = Depends(_api_key_header)) -> None:
    if not config.API_SECRET_KEY:
        raise HTTPException(status_code=403, detail="API authentication not configured on server")
    if api_key != config.API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class TradeReport(BaseModel):
    alert_id: str
    market_id: str
    market_question: str
    clob_token_id: str
    bet_side: str
    bet_price_intended: float
    bet_price_filled: Optional[float] = None
    slippage: Optional[float] = None
    size_usdc: float
    order_id: Optional[str] = None
    status: str
    error_message: Optional[str] = None


class TradeResolutionUpdate(BaseModel):
    resolution_status: str              # won | lost | invalid
    pnl: float
    resolved_at: int
    resolution_source: Optional[str] = None  # prospective | backfill | reconcile | None


class BulkResolutionCorrection(BaseModel):
    corrections: list[dict]  # [{alert_id, resolution_status, pnl, resolved_at}]
    source: str = "reconcile"


class SkipTelemetryReport(BaseModel):
    alert_id: str
    market_id: str
    market_question: str
    bet_side: str
    score: int
    market_type: str
    price_intended: float
    price_current: float
    price_delta_abs: float
    price_delta_frac: float
    static_threshold: float
    gate_outcome: str          # would_have_traded | rejected_expansion_bound | rejected_price_ceiling
    shadow_entry_price: Optional[float] = None
    shadow_size_usdc: Optional[float] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/alerts/tradeable")
def get_tradeable_alerts(
    min_score: Optional[int] = None,
    since: Optional[int] = None,
    limit: int = 20,
    _key: None = Depends(_verify_api_key),
):
    """
    Return alert_outcomes that meet trading criteria and haven't been traded.
    Includes clob_token_id resolved from the markets table.
    """
    if min_score is None:
        min_score = config.TRADING_MIN_SCORE
    if since is None:
        since = int(time.time()) - 3600
    limit = max(1, min(100, limit))

    try:
        alerts = database.get_tradeable_alerts_for_api(
            min_score=min_score,
            since_timestamp=since,
            limit=limit,
        )
        return alerts
    except Exception as exc:
        log.error("[API] get_tradeable_alerts failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.post("/api/trades")
def post_trade(
    body: TradeReport,
    _key: None = Depends(_verify_api_key),
):
    """Accept a trade execution report from the remote trader."""
    try:
        database.insert_trade_execution(
            alert_id=body.alert_id,
            market_id=body.market_id,
            market_question=body.market_question,
            clob_token_id=body.clob_token_id,
            bet_side=body.bet_side,
            size_usdc=body.size_usdc,
            status=body.status,
            bet_price_intended=body.bet_price_intended,
            bet_price_filled=body.bet_price_filled,
            slippage=body.slippage,
            order_id=body.order_id,
            error_message=body.error_message,
        )
        log.info(
            "[API] Trade recorded: alert=%s status=%s size=$%.2f",
            body.alert_id[:12], body.status, body.size_usdc,
        )
        return {"ok": True}
    except Exception as exc:
        log.error("[API] post_trade failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc)}


@app.get("/api/stats/trading")
def get_trading_stats(_key: None = Depends(_verify_api_key)):
    """
    Return trading stats for the remote trader's risk management.
    Includes daily_loss, open_positions, and consecutive_losses which the
    remote trader cannot compute without direct DB access.
    """
    try:
        stats = database.get_trade_stats()
        daily_loss = database.get_daily_loss()
        open_positions = database.get_open_position_count()
        consecutive_losses = database.get_consecutive_losses()
        return {
            "total_trades":       stats.get("total", 0),
            "resolved":           stats.get("resolved", 0),
            "won":                stats.get("won", 0),
            "lost":               stats.get("lost", 0),
            "cumulative_pnl":     stats.get("total_pnl") or 0.0,
            "daily_loss":         daily_loss,
            "open_positions":     open_positions,
            "consecutive_losses": consecutive_losses,
        }
    except Exception as exc:
        log.error("[API] get_trading_stats failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/positions/open")
def get_open_positions(_key: None = Depends(_verify_api_key)):
    """Return filled, pending-resolution positions for the periodic summary."""
    try:
        return database.get_open_positions_for_api()
    except Exception as exc:
        log.error("[API] get_open_positions failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/stats/vault")
def get_vault_stats(_key: None = Depends(_verify_api_key)):
    """Return vault sweep history: count, total USDC swept, last sweep timestamp."""
    try:
        return database.get_vault_sweep_stats()
    except Exception as exc:
        log.error("[API] get_vault_stats failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/trades/pending")
def get_pending_trades(_key: None = Depends(_verify_api_key)):
    """
    Return filled trade_executions still pending resolution, joined with
    the current alert_outcomes.resolution_status. The remote trader polls
    this to detect resolutions and fire Telegram notifications.
    """
    try:
        return database.get_pending_trades_with_resolution()
    except Exception as exc:
        log.error("[API] get_pending_trades failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/trades/recent-losses")
def get_recent_losses(
    limit: int = 6,
    _key: None = Depends(_verify_api_key),
):
    """Return the N most recently resolved-lost trades with score, for streak notifications."""
    try:
        return database.get_recent_resolved_losses(limit=limit)
    except Exception as exc:
        log.error("[API] get_recent_losses failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/trades/resolved-recent")
def get_resolved_recent(
    since: Optional[int] = None,
    _key: None = Depends(_verify_api_key),
):
    """Return won/lost trade_executions since ?since=<ts> for CB history backfill on restart."""
    if since is None:
        since = int(time.time()) - 86400
    try:
        return database.get_resolved_executions_since(since_ts=since, prospective_only=True)
    except Exception as exc:
        log.error("[API] get_resolved_recent failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")


@app.patch("/api/trades/{alert_id}/resolution")
def update_trade_resolution(
    alert_id: str,
    body: TradeResolutionUpdate,
    _key: None = Depends(_verify_api_key),
):
    """
    Called by the remote trader after detecting a resolution via /api/trades/pending.
    Updates trade_executions so that /api/stats/trading reflects correct P&L.
    """
    valid_statuses = {"won", "lost", "invalid"}
    if body.resolution_status not in valid_statuses:
        raise HTTPException(
            status_code=422,
            detail=f"resolution_status must be one of {valid_statuses}",
        )
    try:
        database.update_trade_resolution_by_alert_id(
            alert_id=alert_id,
            resolution_status=body.resolution_status,
            pnl=body.pnl,
            resolved_at=body.resolved_at,
            resolution_source=body.resolution_source,
        )
        log.info(
            "[API] Resolution recorded: alert=%s status=%s pnl=$%+.2f source=%s",
            alert_id[:12], body.resolution_status, body.pnl, body.resolution_source or "none",
        )
        return {"ok": True}
    except Exception as exc:
        log.error("[API] update_trade_resolution failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc)}


@app.post("/api/trades/bulk-correction")
def bulk_correct_trades(
    body: BulkResolutionCorrection,
    _key: None = Depends(_verify_api_key),
):
    """
    Batch-correct resolution_status/pnl for multiple trades.
    Used by the DB reconciliation script to fix backfill artifacts.
    Each correction: {alert_id, resolution_status, pnl, resolved_at}.
    """
    valid_statuses = {"won", "lost", "invalid"}
    updated = 0
    errors = []
    now_ts = int(time.time())
    for c in body.corrections:
        aid = c.get("alert_id", "")
        status = c.get("resolution_status", "")
        if not aid or status not in valid_statuses:
            errors.append({"alert_id": aid, "error": "bad input"})
            continue
        try:
            database.update_trade_resolution_by_alert_id(
                alert_id=aid,
                resolution_status=status,
                pnl=c.get("pnl", 0.0),
                resolved_at=c.get("resolved_at", now_ts),
                resolution_source=body.source,
            )
            updated += 1
        except Exception as exc:
            errors.append({"alert_id": aid, "error": str(exc)})
    log.info("[API] Bulk correction: %d updated, %d errors", updated, len(errors))
    return {"updated": updated, "errors": errors}


@app.get("/api/trades/all-filled")
def get_all_filled_trades(_key: None = Depends(_verify_api_key)):
    """
    Return all filled trade_executions for DB reconciliation.
    Temporary diagnostic — remove after Task 2 is closed.
    """
    try:
        db = database.get_db()
        rows = db.execute("""
            SELECT alert_id, market_id, market_question, bet_side,
                   bet_price_filled, bet_price_intended, size_usdc,
                   resolution_status, pnl, resolved_at, created_at
            FROM trade_executions
            WHERE status = 'filled'
            ORDER BY created_at ASC
        """).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        log.error("[API] get_all_filled_trades failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/diag/pnl-window")
def get_pnl_window(
    from_ts: int,
    until_ts: int,
    _key: None = Depends(_verify_api_key),
):
    """
    Temporary diagnostic: resolved P&L for a specific created_at window.
    Used for DB-vs-chain accounting reconciliation.
    Remove after Task 2 is closed.
    """
    try:
        db = database.get_db()
        row = db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN resolution_status IN ('won','lost','invalid') THEN 1 ELSE 0 END) as resolved,
                SUM(CASE WHEN resolution_status='won' THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN resolution_status='lost' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN resolution_status='pending' THEN 1 ELSE 0 END) as still_pending,
                ROUND(SUM(IFNULL(pnl,0)),2) as db_pnl,
                ROUND(SUM(size_usdc),2) as total_deployed,
                ROUND(AVG(CASE WHEN bet_price_filled IS NOT NULL THEN bet_price_filled
                               ELSE bet_price_intended END),4) as avg_fill_price
            FROM trade_executions
            WHERE status = 'filled'
              AND created_at >= ?
              AND created_at < ?
        """, (from_ts, until_ts)).fetchone()
        by_resolution = db.execute("""
            SELECT resolution_status, COUNT(*) as n,
                   ROUND(SUM(IFNULL(pnl,0)),2) as pnl,
                   ROUND(SUM(size_usdc),2) as deployed,
                   ROUND(AVG(CASE WHEN bet_price_filled IS NOT NULL THEN bet_price_filled
                                  ELSE bet_price_intended END),4) as avg_price
            FROM trade_executions
            WHERE status = 'filled'
              AND created_at >= ?
              AND created_at < ?
            GROUP BY resolution_status
        """, (from_ts, until_ts)).fetchall()
        return {
            "window": {"from_ts": from_ts, "until_ts": until_ts},
            "summary": dict(row) if row else {},
            "by_resolution_status": [dict(r) for r in by_resolution],
        }
    except Exception as exc:
        log.error("[API] get_pnl_window failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/diag/categories")
def get_category_stats(_key: None = Depends(_verify_api_key)):
    """Temporary: category breakdown for analysis. Remove after mission."""
    try:
        db = database.get_db()
        rows = db.execute("""
            SELECT ao.market_category,
                   COUNT(*) as n_trades,
                   SUM(CASE WHEN te.resolution_status='won' THEN 1 ELSE 0 END) as won,
                   SUM(CASE WHEN te.resolution_status IN ('won','lost') THEN 1 ELSE 0 END) as resolved,
                   ROUND(SUM(IFNULL(te.pnl,0)),2) as total_pnl,
                   ROUND(SUM(te.size_usdc),2) as total_deployed
            FROM trade_executions te
            JOIN alert_outcomes ao ON te.alert_id = ao.alert_id
            WHERE te.status = 'filled'
            GROUP BY ao.market_category
            ORDER BY total_deployed DESC
        """).fetchall()
        signal_cats = db.execute("""
            SELECT market_category, COUNT(*) as n FROM alert_outcomes GROUP BY market_category ORDER BY n DESC
        """).fetchall()
        pnl_row = db.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN resolution_status IN ('won','lost','invalid') THEN 1 ELSE 0 END) as resolved,
                   SUM(CASE WHEN resolution_status='won' THEN 1 ELSE 0 END) as won,
                   SUM(CASE WHEN resolution_status='lost' THEN 1 ELSE 0 END) as lost,
                   ROUND(SUM(IFNULL(pnl,0)),2) as cumulative_pnl,
                   SUM(CASE WHEN resolution_status='pending' THEN 1 ELSE 0 END) as still_pending
            FROM trade_executions WHERE status='filled'
        """).fetchone()
        skips = db.execute("""
            SELECT gate_outcome,
                   COUNT(*) as n,
                   ROUND(AVG(price_delta_abs),3) as avg_delta,
                   MIN(recorded_at) as first_ts,
                   MAX(recorded_at) as last_ts
            FROM skip_telemetry
            GROUP BY gate_outcome
            ORDER BY n DESC
        """).fetchall()
        return {
            "trade_by_category": [dict(r) for r in rows],
            "signal_by_category": [dict(r) for r in signal_cats],
            "realized_pnl": dict(pnl_row) if pnl_row else {},
            "skip_telemetry": [dict(r) for r in skips],
        }
    except Exception as exc:
        log.error("[API] get_category_stats failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/skips/telemetry")
def post_skip_telemetry(
    body: SkipTelemetryReport,
    _key: None = Depends(_verify_api_key),
):
    """
    Called by the Fly trader when a slippage-skip occurs (flag on or off).
    Idempotent — duplicate POSTs for the same alert_id are silently ignored.
    """
    valid_outcomes = {"would_have_traded", "rejected_expansion_bound", "rejected_price_ceiling", "rejected_stale_signal", "rejected_soccer_favorite"}
    if body.gate_outcome not in valid_outcomes:
        raise HTTPException(
            status_code=422,
            detail=f"gate_outcome must be one of {valid_outcomes}",
        )
    try:
        database.insert_skip_telemetry(
            alert_id=body.alert_id,
            market_id=body.market_id,
            market_question=body.market_question,
            bet_side=body.bet_side,
            score=body.score,
            market_type=body.market_type,
            price_intended=body.price_intended,
            price_current=body.price_current,
            price_delta_abs=body.price_delta_abs,
            price_delta_frac=body.price_delta_frac,
            static_threshold=body.static_threshold,
            gate_outcome=body.gate_outcome,
            shadow_entry_price=body.shadow_entry_price,
            shadow_size_usdc=body.shadow_size_usdc,
        )
        log.info(
            "[API] Skip telemetry recorded: alert=%s gate=%s delta=%.3f",
            body.alert_id[:12], body.gate_outcome, body.price_delta_abs,
        )
        return {"ok": True}
    except Exception as exc:
        log.error("[API] post_skip_telemetry failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
