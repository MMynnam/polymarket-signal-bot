"""
database.py — SQLite schema, connection management, and all persistence helpers.

Design principles:
  • Single writer pattern: all writes go through get_db() which returns a
    connection with WAL mode enabled (safe for concurrent reads from
    asyncio tasks sharing the same thread).
  • All state lives here. A Railway restart loses nothing.
  • Explicit schema migrations via _apply_migrations() — new columns can be
    added without dropping tables.
"""

import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Generator, Optional

import config

log = logging.getLogger("database")

# ---------------------------------------------------------------------------
# DDL — single source of truth for the schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
-- ------------------------------------------------------------------
-- markets: discovered via Gamma API
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS markets (
    condition_id    TEXT PRIMARY KEY,
    title           TEXT,
    clob_token_ids  TEXT,           -- JSON array of token IDs
    end_date        TEXT,           -- ISO-8601 string; NULL if unknown
    active          INTEGER DEFAULT 1,
    raw_json        TEXT,           -- Full Gamma response blob for debugging
    first_seen_at   REAL NOT NULL,  -- Unix timestamp
    updated_at      REAL NOT NULL
);

-- ------------------------------------------------------------------
-- alert_outcomes: closed-loop outcome tracking for every fired alert.
-- Populated immediately when an alert fires (resolution_status=pending).
-- The resolution_checker_loop updates rows once markets resolve.
-- alert_id is the trade_id from alert_history — TEXT natural key.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_outcomes (
    alert_id                TEXT PRIMARY KEY,   -- trade_id; joins alert_history.trade_id
    created_at              INTEGER NOT NULL,   -- unix timestamp when alert fired
    market_id               TEXT NOT NULL,
    market_question         TEXT NOT NULL,      -- denormalised title at alert time
    wallet_address          TEXT NOT NULL,
    score                   INTEGER NOT NULL,
    score_breakdown_json    TEXT NOT NULL,      -- JSON of full ScoreBreakdown.to_dict()
    bet_side                TEXT NOT NULL,      -- outcome the wallet bet on (e.g. "Yes")
    bet_price_at_alert      REAL NOT NULL,      -- implied probability 0.0–1.0
    bet_size_usd            REAL NOT NULL,
    resolution_status       TEXT NOT NULL DEFAULT 'pending',
                                                -- pending | resolved_won | resolved_lost
                                                -- | resolved_invalid
    resolved_at             INTEGER,            -- unix timestamp; NULL until resolved
    winning_outcome         TEXT,               -- NULL until resolved
    roi                     REAL                -- NULL until resolved
);
CREATE INDEX IF NOT EXISTS idx_alert_outcomes_status
    ON alert_outcomes(resolution_status);
CREATE INDEX IF NOT EXISTS idx_alert_outcomes_created
    ON alert_outcomes(created_at);
CREATE INDEX IF NOT EXISTS idx_alert_outcomes_score
    ON alert_outcomes(score);

-- ------------------------------------------------------------------
-- last_seen_trades: REST fallback deduplication — one row per market.
-- We record the highest trade ID we have processed so the next poll
-- only processes newer trades.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS last_seen_trades (
    market_id       TEXT PRIMARY KEY,
    last_trade_id   TEXT NOT NULL,
    updated_at      REAL NOT NULL
);

-- ------------------------------------------------------------------
-- wallet_profiles: cached analysis from Data API + Etherscan V2.
-- Keyed by lowercase Ethereum address.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallet_profiles (
    address         TEXT PRIMARY KEY,
    profile_json    TEXT NOT NULL,  -- Serialized WalletProfile dict
    fetched_at      REAL NOT NULL   -- Unix timestamp; compare vs WALLET_CACHE_TTL_SECONDS
);

-- ------------------------------------------------------------------
-- alert_history: every fired (or suppressed in dry-run) alert.
-- Useful for deduplication and auditing.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alert_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        TEXT NOT NULL,
    market_id       TEXT NOT NULL,
    wallet_address  TEXT NOT NULL,
    score           INTEGER NOT NULL,
    score_json      TEXT NOT NULL,  -- Full breakdown JSON
    alert_text      TEXT NOT NULL,
    sent            INTEGER NOT NULL DEFAULT 0,  -- 0=dry-run, 1=sent
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_history_trade
    ON alert_history(trade_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_wallet
    ON alert_history(wallet_address);

-- ------------------------------------------------------------------
-- flagged_clusters: wallets identified as sharing a funding source.
-- Used by the cluster-bonus scorer component.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flagged_clusters (
    address         TEXT NOT NULL,
    cluster_id      TEXT NOT NULL,  -- shared funding-source address (hex)
    flagged_at      REAL NOT NULL,
    PRIMARY KEY (address, cluster_id)
);
CREATE INDEX IF NOT EXISTS idx_flagged_clusters_cluster
    ON flagged_clusters(cluster_id);

-- ------------------------------------------------------------------
-- schema_version: simple migration tracking
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""

_CURRENT_SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_connection: Optional[sqlite3.Connection] = None


def init_db() -> None:
    """
    Open the SQLite database, apply the schema, and run any pending
    migrations. Call once at startup before any other database function.
    """
    global _connection

    log.info("Opening SQLite database at: %s", config.SQLITE_DB_PATH)
    _connection = sqlite3.connect(
        config.SQLITE_DB_PATH,
        check_same_thread=False,  # We use a mutex pattern; asyncio tasks share thread
        timeout=30,
    )
    _connection.row_factory = sqlite3.Row

    # WAL mode: readers never block writers and vice-versa.
    _connection.execute("PRAGMA journal_mode=WAL")
    _connection.execute("PRAGMA synchronous=NORMAL")  # Durable enough; faster than FULL
    _connection.execute("PRAGMA foreign_keys=ON")
    _connection.execute("PRAGMA cache_size=-8000")    # ~8 MB page cache

    _apply_schema()
    _apply_migrations()
    log.info("Database initialised (schema version %d)", _CURRENT_SCHEMA_VERSION)


def get_db() -> sqlite3.Connection:
    """Return the singleton connection. Raises if init_db() was not called."""
    if _connection is None:
        raise RuntimeError("Database not initialised — call init_db() first")
    return _connection


@contextmanager
def transaction() -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager for explicit transactions with automatic rollback on error.

    Usage:
        with transaction() as db:
            db.execute("INSERT ...")
    """
    db = get_db()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


def _apply_schema() -> None:
    db = get_db()
    db.executescript(_SCHEMA_SQL)
    db.commit()


def _apply_migrations() -> None:
    """
    Run incremental schema migrations. Each migration block is idempotent
    (uses IF NOT EXISTS / TRY pattern). Bump _CURRENT_SCHEMA_VERSION when
    adding a new migration.
    """
    db = get_db()
    row = db.execute("SELECT MAX(version) FROM schema_version").fetchone()
    current = row[0] or 0

    if current < 1:
        # Version 1 is the baseline — schema already applied above.
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(1)")
        db.commit()
        log.info("Applied schema migration → version 1")

    if current < 2:
        # Version 2 — alert_outcomes table (created via _SCHEMA_SQL above).
        # Nothing to ALTER; the table is new.
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(2)")
        db.commit()
        log.info("Applied schema migration → version 2")


# ---------------------------------------------------------------------------
# Market registry helpers
# ---------------------------------------------------------------------------

def upsert_market(
    condition_id: str,
    title: str,
    clob_token_ids: list[str],
    end_date: Optional[str],
    raw_json: dict,
    active: bool = True,
) -> None:
    """Insert or update a market from a Gamma API response."""
    now = time.time()
    with transaction() as db:
        db.execute(
            """
            INSERT INTO markets
                (condition_id, title, clob_token_ids, end_date, active, raw_json,
                 first_seen_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(condition_id) DO UPDATE SET
                title          = excluded.title,
                clob_token_ids = excluded.clob_token_ids,
                end_date       = excluded.end_date,
                active         = excluded.active,
                raw_json       = excluded.raw_json,
                updated_at     = excluded.updated_at
            """,
            (
                condition_id,
                title,
                json.dumps(clob_token_ids),
                end_date,
                1 if active else 0,
                json.dumps(raw_json),
                now,
                now,
            ),
        )


def get_market(condition_id: str) -> Optional[dict]:
    """Return a market dict or None."""
    row = get_db().execute(
        "SELECT * FROM markets WHERE condition_id = ?", (condition_id,)
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["clob_token_ids"] = json.loads(d["clob_token_ids"] or "[]")
    d["raw_json"] = json.loads(d["raw_json"] or "{}")
    return d


def get_all_active_markets() -> list[dict]:
    """Return all active markets."""
    rows = get_db().execute(
        "SELECT * FROM markets WHERE active = 1 ORDER BY updated_at DESC"
    ).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d["clob_token_ids"] = json.loads(d["clob_token_ids"] or "[]")
        d["raw_json"] = json.loads(d["raw_json"] or "{}")
        results.append(d)
    return results


def mark_market_inactive(condition_id: str) -> None:
    with transaction() as db:
        db.execute(
            "UPDATE markets SET active = 0, updated_at = ? WHERE condition_id = ?",
            (time.time(), condition_id),
        )


# ---------------------------------------------------------------------------
# Trade deduplication helpers
# ---------------------------------------------------------------------------

def get_last_seen_trade_id(market_id: str) -> Optional[str]:
    """Return the last processed trade ID for a market, or None."""
    row = get_db().execute(
        "SELECT last_trade_id FROM last_seen_trades WHERE market_id = ?",
        (market_id,),
    ).fetchone()
    return row[0] if row else None


def set_last_seen_trade_id(market_id: str, trade_id: str) -> None:
    with transaction() as db:
        db.execute(
            """
            INSERT INTO last_seen_trades (market_id, last_trade_id, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                last_trade_id = excluded.last_trade_id,
                updated_at    = excluded.updated_at
            """,
            (market_id, trade_id, time.time()),
        )


# ---------------------------------------------------------------------------
# Wallet profile cache helpers
# ---------------------------------------------------------------------------

def get_cached_wallet_profile(address: str) -> Optional[dict]:
    """
    Return a cached wallet profile if it exists and is within TTL.
    Returns None if missing or stale.
    """
    address = address.lower()
    row = get_db().execute(
        "SELECT profile_json, fetched_at FROM wallet_profiles WHERE address = ?",
        (address,),
    ).fetchone()
    if row is None:
        return None

    age_seconds = time.time() - row["fetched_at"]
    if age_seconds > config.WALLET_CACHE_TTL_SECONDS:
        log.debug(
            "Wallet cache STALE for %s (age=%.0fs, TTL=%ds)",
            address, age_seconds, config.WALLET_CACHE_TTL_SECONDS,
        )
        return None

    log.debug("Wallet cache HIT for %s (age=%.0fs)", address, age_seconds)
    return json.loads(row["profile_json"])


def save_wallet_profile(address: str, profile: dict) -> None:
    """Persist (or overwrite) a wallet profile in the cache."""
    address = address.lower()
    with transaction() as db:
        db.execute(
            """
            INSERT INTO wallet_profiles (address, profile_json, fetched_at)
            VALUES (?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                profile_json = excluded.profile_json,
                fetched_at   = excluded.fetched_at
            """,
            (address, json.dumps(profile), time.time()),
        )
    log.debug("Wallet profile cached for %s", address)


# ---------------------------------------------------------------------------
# Alert history helpers
# ---------------------------------------------------------------------------

def has_alert_been_sent_for_trade(trade_id: str) -> bool:
    """Return True if we have already alerted on this specific trade ID."""
    row = get_db().execute(
        "SELECT 1 FROM alert_history WHERE trade_id = ? LIMIT 1",
        (trade_id,),
    ).fetchone()
    return row is not None


def save_alert(
    trade_id: str,
    market_id: str,
    wallet_address: str,
    score: int,
    score_breakdown: dict,
    alert_text: str,
    sent: bool,
) -> int:
    """Persist an alert record. Returns the new row ID."""
    with transaction() as db:
        cursor = db.execute(
            """
            INSERT INTO alert_history
                (trade_id, market_id, wallet_address, score, score_json,
                 alert_text, sent, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                market_id,
                wallet_address.lower(),
                score,
                json.dumps(score_breakdown),
                alert_text,
                1 if sent else 0,
                time.time(),
            ),
        )
        return cursor.lastrowid


def get_recent_alerts(limit: int = 50) -> list[dict]:
    """Return the most recent alert records for monitoring."""
    rows = get_db().execute(
        "SELECT * FROM alert_history ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d["score_breakdown"] = json.loads(d["score_json"])
        results.append(d)
    return results


# ---------------------------------------------------------------------------
# Cluster / proxy wallet helpers
# ---------------------------------------------------------------------------

def flag_wallet_cluster(address: str, cluster_id: str) -> None:
    """
    Record that `address` was funded from `cluster_id`.
    cluster_id is typically the funding wallet's address.
    """
    address = address.lower()
    cluster_id = cluster_id.lower()
    with transaction() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO flagged_clusters (address, cluster_id, flagged_at)
            VALUES (?, ?, ?)
            """,
            (address, cluster_id, time.time()),
        )
    log.info("Cluster flagged: wallet=%s cluster=%s", address, cluster_id)


def get_cluster_id_for_wallet(address: str) -> Optional[str]:
    """
    Return the cluster_id if this wallet has been flagged, else None.
    Returns the most recently flagged cluster if there are multiple.
    """
    address = address.lower()
    row = get_db().execute(
        """
        SELECT cluster_id FROM flagged_clusters
        WHERE address = ?
        ORDER BY flagged_at DESC LIMIT 1
        """,
        (address,),
    ).fetchone()
    return row[0] if row else None


def get_cluster_members(cluster_id: str) -> list[str]:
    """Return all wallet addresses in a given cluster."""
    cluster_id = cluster_id.lower()
    rows = get_db().execute(
        "SELECT address FROM flagged_clusters WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchall()
    return [r[0] for r in rows]


def get_all_flagged_addresses() -> set[str]:
    """Return the set of all addresses that appear in any cluster."""
    rows = get_db().execute("SELECT DISTINCT address FROM flagged_clusters").fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Alert outcome helpers
# ---------------------------------------------------------------------------

def insert_alert_outcome(
    alert_id: str,
    market_id: str,
    market_question: str,
    wallet_address: str,
    score: int,
    score_breakdown_json: str,
    bet_side: str,
    bet_price_at_alert: float,
    bet_size_usd: float,
) -> None:
    """
    Insert a new pending outcome row when an alert fires.
    Uses INSERT OR IGNORE so duplicate calls (e.g. on retry) are safe.
    """
    now = int(time.time())
    with transaction() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO alert_outcomes
                (alert_id, created_at, market_id, market_question, wallet_address,
                 score, score_breakdown_json, bet_side, bet_price_at_alert,
                 bet_size_usd, resolution_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                alert_id,
                now,
                market_id,
                market_question,
                wallet_address.lower(),
                score,
                score_breakdown_json,
                bet_side,
                bet_price_at_alert,
                bet_size_usd,
            ),
        )


def get_pending_outcomes(limit: int = 500) -> list[dict]:
    """
    Return all alert_outcome rows with resolution_status='pending'.
    Called by the resolution-checker each cycle.
    """
    rows = get_db().execute(
        """
        SELECT * FROM alert_outcomes
        WHERE resolution_status = 'pending'
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def update_outcome_resolution(
    alert_id: str,
    status: str,
    winning_outcome: Optional[str],
    roi: Optional[float],
    resolved_at: int,
) -> None:
    """
    Update a pending outcome row once the market has resolved.
    `status` must be one of: resolved_won, resolved_lost, resolved_invalid.
    """
    with transaction() as db:
        db.execute(
            """
            UPDATE alert_outcomes
            SET resolution_status = ?,
                winning_outcome   = ?,
                roi               = ?,
                resolved_at       = ?
            WHERE alert_id = ?
            """,
            (status, winning_outcome, roi, resolved_at, alert_id),
        )


def get_outcome_stats(since_timestamp: Optional[int] = None) -> dict[str, Any]:
    """
    Return aggregate outcome statistics for the stats script.

    All SQL lives here; stats.py only does formatting and arithmetic.
    The returned dict contains pre-aggregated counts and a list of resolved
    rows (dicts with score, score_breakdown_json, bet_side, roi, resolution_status)
    for component-level correlation analysis in stats.py.
    """
    db = get_db()

    where_clause = "WHERE created_at >= ?" if since_timestamp else ""
    params: tuple = (since_timestamp,) if since_timestamp else ()

    total = db.execute(
        f"SELECT COUNT(*) FROM alert_outcomes {where_clause}", params
    ).fetchone()[0]

    def _count(extra_where: str, extra_params: tuple = ()) -> int:
        base = where_clause if where_clause else "WHERE " + extra_where.lstrip("AND ").lstrip("AND")
        if where_clause:
            clause = f"{where_clause} AND {extra_where.lstrip('AND').strip()}"
        else:
            clause = f"WHERE {extra_where.lstrip('AND').strip()}"
        return db.execute(
            f"SELECT COUNT(*) FROM alert_outcomes {clause}",
            params + extra_params,
        ).fetchone()[0]

    pending  = _count("resolution_status = 'pending'")
    wins     = _count("resolution_status = 'resolved_won'")
    losses   = _count("resolution_status = 'resolved_lost'")
    invalid  = _count("resolution_status = 'resolved_invalid'")
    resolved = wins + losses + invalid

    # Average and total ROI over won+lost alerts only (invalid get roi=0).
    roi_rows = db.execute(
        f"""
        SELECT roi FROM alert_outcomes
        {where_clause + ' AND' if where_clause else 'WHERE'}
        resolution_status IN ('resolved_won', 'resolved_lost', 'resolved_invalid')
        AND roi IS NOT NULL
        """,
        params,
    ).fetchall()
    roi_values = [r[0] for r in roi_rows if r[0] is not None]
    avg_roi   = sum(roi_values) / len(roi_values) if roi_values else None
    total_roi = sum(roi_values) if roi_values else None

    # Score-bucket aggregates.
    buckets = []
    for lo, hi in [(60, 69), (70, 79), (80, 89), (90, 110)]:
        label = f"{lo}-{hi}" if hi < 110 else f"{lo}+"
        if where_clause:
            bucket_clause = f"{where_clause} AND score BETWEEN ? AND ?"
        else:
            bucket_clause = "WHERE score BETWEEN ? AND ?"
        bucket_params = params + (lo, hi)
        b_total = db.execute(
            f"SELECT COUNT(*) FROM alert_outcomes {bucket_clause}",
            bucket_params,
        ).fetchone()[0]
        b_wins = db.execute(
            f"SELECT COUNT(*) FROM alert_outcomes {bucket_clause}"
            f" AND resolution_status = 'resolved_won'",
            bucket_params,
        ).fetchone()[0]
        b_roi_rows = db.execute(
            f"SELECT roi FROM alert_outcomes {bucket_clause}"
            f" AND resolution_status IN ('resolved_won','resolved_lost','resolved_invalid')"
            f" AND roi IS NOT NULL",
            bucket_params,
        ).fetchall()
        b_roi = [r[0] for r in b_roi_rows if r[0] is not None]
        buckets.append({
            "label":   label,
            "count":   b_total,
            "wins":    b_wins,
            "avg_roi": sum(b_roi) / len(b_roi) if b_roi else None,
        })

    # Resolved rows for component-level correlation analysis.
    if where_clause:
        resolved_clause = (
            f"{where_clause} AND resolution_status"
            f" IN ('resolved_won','resolved_lost','resolved_invalid')"
        )
    else:
        resolved_clause = (
            "WHERE resolution_status"
            " IN ('resolved_won','resolved_lost','resolved_invalid')"
        )
    resolved_rows = db.execute(
        f"""
        SELECT score, score_breakdown_json, bet_side, roi, resolution_status
        FROM alert_outcomes {resolved_clause}
        """,
        params,
    ).fetchall()

    return {
        "total":        total,
        "resolved":     resolved,
        "pending":      pending,
        "invalid":      invalid,
        "wins":         wins,
        "losses":       losses,
        "avg_roi":      avg_roi,
        "total_roi":    total_roi,
        "score_buckets": buckets,
        "resolved_rows": [dict(r) for r in resolved_rows],
    }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def get_stats() -> dict[str, Any]:
    """Return a quick summary of database contents for health-check logging."""
    db = get_db()
    return {
        "active_markets": db.execute(
            "SELECT COUNT(*) FROM markets WHERE active=1"
        ).fetchone()[0],
        "total_markets": db.execute("SELECT COUNT(*) FROM markets").fetchone()[0],
        "cached_wallets": db.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0],
        "alerts_sent": db.execute(
            "SELECT COUNT(*) FROM alert_history WHERE sent=1"
        ).fetchone()[0],
        "alerts_dry_run": db.execute(
            "SELECT COUNT(*) FROM alert_history WHERE sent=0"
        ).fetchone()[0],
        "flagged_cluster_wallets": db.execute(
            "SELECT COUNT(DISTINCT address) FROM flagged_clusters"
        ).fetchone()[0],
    }
