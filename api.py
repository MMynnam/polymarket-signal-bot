"""
api.py — FastAPI app exposing alert data for the remote Fly.io trader.

Endpoints:
  GET  /api/alerts/tradeable             — alerts ready for execution (not yet traded)
  POST /api/trades                       — accept a trade execution report
  GET  /api/stats/trading                — trading stats for remote risk management
  GET  /api/trades/pending               — pending trades + current resolution status
  GET  /api/positions/open               — currently open (filled, unresolved) positions
  PATCH /api/trades/{alert_id}/resolution — remote trader reports a resolution

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
    resolution_status: str   # won | lost | invalid
    pnl: float
    resolved_at: int


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
        )
        log.info(
            "[API] Resolution recorded: alert=%s status=%s pnl=$%+.2f",
            alert_id[:12], body.resolution_status, body.pnl,
        )
        return {"ok": True}
    except Exception as exc:
        log.error("[API] update_trade_resolution failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc)}
