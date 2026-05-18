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
    roi                     REAL,               -- NULL until resolved
    resolution_latency_hours REAL               -- (resolved_at - created_at) / 3600; NULL until resolved
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
-- ------------------------------------------------------------------
-- trade_executions: every trade attempt made by the automated trader.
-- alert_id is a UNIQUE foreign key to alert_outcomes.alert_id.
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trade_executions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id            TEXT NOT NULL,
    market_id           TEXT NOT NULL,
    market_question     TEXT,
    clob_token_id       TEXT NOT NULL,
    bet_side            TEXT NOT NULL,
    bet_price_intended  REAL,
    bet_price_filled    REAL,
    slippage            REAL,
    size_usdc           REAL NOT NULL,
    order_type          TEXT DEFAULT 'FOK',
    order_id            TEXT,
    status              TEXT NOT NULL,   -- filled | partial | fill-unconfirmed | rejected | failed | error
    error_message       TEXT,
    gas_cost_matic      REAL,
    created_at          INTEGER NOT NULL,
    resolved_at         INTEGER,
    resolution_status   TEXT DEFAULT 'pending',  -- pending | won | lost | invalid
    pnl                 REAL,
    UNIQUE(alert_id)
);
CREATE INDEX IF NOT EXISTS idx_te_status     ON trade_executions(status);
CREATE INDEX IF NOT EXISTS idx_te_created    ON trade_executions(created_at);
CREATE INDEX IF NOT EXISTS idx_te_resolution ON trade_executions(resolution_status);

-- ------------------------------------------------------------------
-- vault_sweeps: accounting log for profit sweep transfers
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vault_sweeps (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    amount_usdc    REAL NOT NULL,
    balance_before REAL NOT NULL,
    balance_after  REAL NOT NULL,
    vault_address  TEXT NOT NULL,
    tx_hash        TEXT NOT NULL,
    swept_at       INTEGER NOT NULL
);

-- ------------------------------------------------------------------
-- skip_telemetry: every slippage-skip event with shadow counterfactual.
-- Keyed by alert_id (UNIQUE) — one row per alert that was skipped for
-- slippage. Populated by the Fly trader via POST /api/skips/telemetry.
-- shadow_resolution_status mirrors alert_outcomes.resolution_status and
-- is updated by resolve_shadow_position() when the market resolves.
-- gate_outcome ∈ {would_have_traded, rejected_expansion_bound,
--                 rejected_price_ceiling}
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skip_telemetry (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id                 TEXT NOT NULL,
    recorded_at              INTEGER NOT NULL,
    market_id                TEXT NOT NULL,
    market_question          TEXT NOT NULL DEFAULT '',
    bet_side                 TEXT NOT NULL DEFAULT '',
    score                    INTEGER NOT NULL DEFAULT 0,
    market_type              TEXT DEFAULT '',
    price_intended           REAL NOT NULL,
    price_current            REAL NOT NULL,
    price_delta_abs          REAL NOT NULL,
    price_delta_frac         REAL NOT NULL,
    static_threshold         REAL NOT NULL,
    gate_outcome             TEXT NOT NULL,
    shadow_entry_price       REAL,
    shadow_size_usdc         REAL,
    shadow_resolution_status TEXT NOT NULL DEFAULT 'pending',
    shadow_roi               REAL,
    shadow_resolved_at       INTEGER,
    UNIQUE(alert_id)
);
CREATE INDEX IF NOT EXISTS idx_skip_telemetry_recorded
    ON skip_telemetry(recorded_at);
CREATE INDEX IF NOT EXISTS idx_skip_telemetry_gate
    ON skip_telemetry(gate_outcome);
CREATE INDEX IF NOT EXISTS idx_skip_telemetry_shadow_status
    ON skip_telemetry(shadow_resolution_status);

-- ------------------------------------------------------------------
-- schema_version: simple migration tracking
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
"""

_CURRENT_SCHEMA_VERSION = 9

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

_connection: Optional[sqlite3.Connection] = None


def init_db(db_path: Optional[str] = None) -> None:
    """
    Open the SQLite database, apply the schema, and run any pending
    migrations. Call once at startup before any other database function.
    db_path overrides config.SQLITE_DB_PATH when provided.
    """
    global _connection

    path = db_path or config.SQLITE_DB_PATH
    log.info("Opening SQLite database at: %s", path)
    _connection = sqlite3.connect(
        path,
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

    if current < 3:
        # Version 3 — resolution_latency_hours column on alert_outcomes.
        try:
            db.execute(
                "ALTER TABLE alert_outcomes ADD COLUMN resolution_latency_hours REAL"
            )
            log.info("Migration 3: added resolution_latency_hours column")
        except Exception:
            pass  # Column already exists (idempotent re-run)
        # Backfill rows that are already resolved.
        db.execute(
            """
            UPDATE alert_outcomes
            SET resolution_latency_hours = (resolved_at - created_at) / 3600.0
            WHERE resolved_at IS NOT NULL
              AND resolution_latency_hours IS NULL
            """
        )
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(3)")
        db.commit()
        log.info("Applied schema migration → version 3")

    if current < 4:
        # Version 4 — analytical dimensions: category, price band, time-of-day,
        # contrarian flag, duration, and size anomaly multiple.
        new_cols = [
            ("market_category",        "TEXT"),
            ("bet_price_band",         "TEXT"),
            ("hours_to_close_at_alert","REAL"),
            ("trade_hour_utc",         "INTEGER"),
            ("is_contrarian",          "INTEGER DEFAULT 0"),
            ("size_anomaly_multiple",  "REAL"),
        ]
        for col_name, col_type in new_cols:
            try:
                db.execute(
                    f"ALTER TABLE alert_outcomes ADD COLUMN {col_name} {col_type}"
                )
                log.info("Migration 4: added column %s", col_name)
            except Exception:
                pass  # Column already exists (idempotent re-run)
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(4)")
        db.commit()
        log.info("Applied schema migration → version 4")

    if current < 5:
        # Version 5 — trade_executions table (created via _SCHEMA_SQL above;
        # indexes already applied). Just bump the version record.
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(5)")
        db.commit()
        log.info("Applied schema migration → version 5")

    if current < 6:
        # Version 6 — vault_sweeps table (created via _SCHEMA_SQL above).
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(6)")
        db.commit()
        log.info("Applied schema migration → version 6")

    if current < 7:
        # Version 7 — retry_count column on trade_executions for the re-queue mechanism.
        try:
            db.execute(
                "ALTER TABLE trade_executions ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"
            )
            log.info("Migration 7: added retry_count column to trade_executions")
        except Exception:
            pass  # Column already exists (idempotent re-run)
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(7)")
        db.commit()
        log.info("Applied schema migration → version 7")

    if current < 8:
        # Version 8 — invalidate wallet_profiles cache built against the deprecated
        # /closed-positions endpoint (returned empty list). Setting fetched_at=0 makes
        # every cached profile appear older than WALLET_CACHE_TTL_SECONDS so it gets
        # re-fetched with the correct /v1/closed-positions endpoint on next access.
        db.execute("UPDATE wallet_profiles SET fetched_at = 0")
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(8)")
        db.commit()
        log.info("Migration 8: invalidated %d stale wallet_profiles cache entries",
                 db.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0])

    if current < 9:
        # Version 9 — skip_telemetry table (created via _SCHEMA_SQL above).
        # No ALTER needed; the table and its indexes are new.
        db.execute("INSERT OR IGNORE INTO schema_version(version) VALUES(9)")
        db.commit()
        log.info("Applied schema migration → version 9 (skip_telemetry)")


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


_WALLET_PROFILE_MAX_ROWS = 2_000

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
        count = db.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]
        if count > _WALLET_PROFILE_MAX_ROWS:
            db.execute(
                """
                DELETE FROM wallet_profiles WHERE address IN (
                    SELECT address FROM wallet_profiles
                    ORDER BY fetched_at ASC
                    LIMIT ?
                )
                """,
                (count - _WALLET_PROFILE_MAX_ROWS,),
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
    market_category: Optional[str] = None,
    bet_price_band: Optional[str] = None,
    hours_to_close_at_alert: Optional[float] = None,
    trade_hour_utc: Optional[int] = None,
    is_contrarian: int = 0,
    size_anomaly_multiple: Optional[float] = None,
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
                 bet_size_usd, resolution_status,
                 market_category, bet_price_band, hours_to_close_at_alert,
                 trade_hour_utc, is_contrarian, size_anomaly_multiple)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
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
                market_category,
                bet_price_band,
                hours_to_close_at_alert,
                trade_hour_utc,
                is_contrarian,
                size_anomaly_multiple,
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
    resolution_latency_hours: Optional[float] = None,
) -> None:
    """
    Update a pending outcome row once the market has resolved.
    `status` must be one of: resolved_won, resolved_lost, resolved_invalid.
    Also resolves any matching skip_telemetry shadow row (non-fatal).
    """
    with transaction() as db:
        db.execute(
            """
            UPDATE alert_outcomes
            SET resolution_status        = ?,
                winning_outcome          = ?,
                roi                      = ?,
                resolved_at              = ?,
                resolution_latency_hours = ?
            WHERE alert_id = ?
            """,
            (status, winning_outcome, roi, resolved_at,
             resolution_latency_hours, alert_id),
        )
    try:
        resolve_shadow_position(alert_id, winning_outcome, status, resolved_at)
    except Exception as exc:
        log.debug("[shadow] resolve_shadow_position failed for %s (non-fatal): %s", alert_id, exc)


def insert_skip_telemetry(
    alert_id: str,
    market_id: str,
    market_question: str,
    bet_side: str,
    score: int,
    market_type: str,
    price_intended: float,
    price_current: float,
    price_delta_abs: float,
    price_delta_frac: float,
    static_threshold: float,
    gate_outcome: str,
    shadow_entry_price: Optional[float] = None,
    shadow_size_usdc: Optional[float] = None,
) -> None:
    """
    Record a slippage-skip event. INSERT OR IGNORE on alert_id — idempotent
    if the trader retries the telemetry POST.
    """
    now = int(time.time())
    with transaction() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO skip_telemetry (
                alert_id, recorded_at, market_id, market_question,
                bet_side, score, market_type,
                price_intended, price_current, price_delta_abs, price_delta_frac,
                static_threshold, gate_outcome,
                shadow_entry_price, shadow_size_usdc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_id, now, market_id, market_question,
                bet_side, score, market_type,
                price_intended, price_current, price_delta_abs, price_delta_frac,
                static_threshold, gate_outcome,
                shadow_entry_price, shadow_size_usdc,
            ),
        )


def resolve_shadow_position(
    alert_id: str,
    winning_outcome: Optional[str],
    base_status: str,
    resolved_at: int,
) -> None:
    """
    Compute and persist the shadow ROI for a skip_telemetry row.

    Called from update_outcome_resolution() after the real alert_outcomes
    row is updated. No-ops silently if no skip_telemetry row exists for
    this alert_id (the common case — most alerts are not slippage-skips).

    Shadow ROI formula (mirrors alert_outcomes convention):
      resolved_won  → shadow_roi = (1 - shadow_entry_price) / shadow_entry_price
      resolved_lost → shadow_roi = -1.0
      resolved_invalid → shadow_roi = 0.0
    """
    db = get_db()
    row = db.execute(
        "SELECT shadow_entry_price FROM skip_telemetry WHERE alert_id = ?",
        (alert_id,),
    ).fetchone()
    if row is None:
        return  # No skip_telemetry row for this alert — normal case

    entry = row[0]  # may be None if shadow_entry_price was not recorded

    if base_status == "resolved_won" and entry is not None and entry > 0:
        shadow_roi: Optional[float] = (1.0 - entry) / entry
    elif base_status == "resolved_lost":
        shadow_roi = -1.0
    elif base_status == "resolved_invalid":
        shadow_roi = 0.0
    else:
        shadow_roi = None

    with transaction() as db2:
        db2.execute(
            """
            UPDATE skip_telemetry
            SET shadow_resolution_status = ?,
                shadow_roi               = ?,
                shadow_resolved_at       = ?
            WHERE alert_id = ?
            """,
            (base_status, shadow_roi, resolved_at, alert_id),
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
    for lo, hi in [(75, 79), (80, 89), (90, 99), (100, 130)]:
        label = f"{lo}-{hi}" if hi < 130 else f"{lo}+"
        if where_clause:
            bucket_clause = f"{where_clause} AND score BETWEEN ? AND ?"
        else:
            bucket_clause = "WHERE score BETWEEN ? AND ?"
        bucket_params = params + (lo, hi)
        b_total = db.execute(
            f"SELECT COUNT(*) FROM alert_outcomes {bucket_clause}",
            bucket_params,
        ).fetchone()[0]
        b_resolved = db.execute(
            f"SELECT COUNT(*) FROM alert_outcomes {bucket_clause}"
            f" AND resolution_status IN ('resolved_won','resolved_lost','resolved_invalid')",
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
            "label":    label,
            "count":    b_total,
            "resolved": b_resolved,
            "wins":     b_wins,
            "avg_roi":  sum(b_roi) / len(b_roi) if b_roi else None,
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
        SELECT score, score_breakdown_json, bet_side, roi, resolution_status,
               market_category, bet_price_band, trade_hour_utc, is_contrarian
        FROM alert_outcomes {resolved_clause}
        """,
        params,
    ).fetchall()

    # Resolution latency rows — for speed-bucket breakdown in stats.py.
    if where_clause:
        latency_clause = (
            f"{where_clause} AND resolution_latency_hours IS NOT NULL"
            f" AND resolution_status IN ('resolved_won','resolved_lost','resolved_invalid')"
        )
    else:
        latency_clause = (
            "WHERE resolution_latency_hours IS NOT NULL"
            " AND resolution_status IN ('resolved_won','resolved_lost','resolved_invalid')"
        )
    latency_rows = db.execute(
        f"""
        SELECT resolution_latency_hours, resolution_status, roi
        FROM alert_outcomes {latency_clause}
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
        "latency_rows":  [dict(r) for r in latency_rows],
    }


# ---------------------------------------------------------------------------
# Trade execution helpers
# ---------------------------------------------------------------------------

def insert_trade_execution(
    alert_id: str,
    market_id: str,
    market_question: str,
    clob_token_id: str,
    bet_side: str,
    size_usdc: float,
    status: str,
    bet_price_intended: Optional[float] = None,
    bet_price_filled: Optional[float] = None,
    slippage: Optional[float] = None,
    order_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> int:
    """Log a trade attempt. Returns the new row id.

    INSERT OR IGNORE for most cases (idempotent on duplicate alert_id).
    Exception: if the existing row has status='error' and retry_count=0 (a
    re-queued alert getting its one allowed retry), UPDATE that row and
    increment retry_count so it can't be re-queued again.
    """
    now = int(time.time())
    with transaction() as db:
        cursor = db.execute(
            """
            INSERT OR IGNORE INTO trade_executions
                (alert_id, market_id, market_question, clob_token_id, bet_side,
                 bet_price_intended, bet_price_filled, slippage, size_usdc,
                 order_type, order_id, status, error_message, created_at, retry_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'FOK', ?, ?, ?, ?, 0)
            """,
            (
                alert_id, market_id, market_question, clob_token_id, bet_side,
                bet_price_intended, bet_price_filled, slippage, size_usdc,
                order_id, status, error_message, now,
            ),
        )
        if cursor.rowcount > 0:
            return cursor.lastrowid
        # Row already exists — update only if it is the re-queue window (error, retry_count=0).
        db.execute(
            """
            UPDATE trade_executions
               SET status          = ?,
                   error_message   = ?,
                   order_id        = ?,
                   bet_price_filled = ?,
                   slippage        = ?,
                   created_at      = ?,
                   retry_count     = retry_count + 1
             WHERE alert_id   = ?
               AND status     = 'error'
               AND retry_count = 0
            """,
            (status, error_message, order_id, bet_price_filled, slippage, now, alert_id),
        )
        return 0


def get_pending_trade_executions() -> list[dict]:
    """Return trade_executions awaiting resolution (filled or partial, pending resolution)."""
    rows = get_db().execute(
        """
        SELECT * FROM trade_executions
        WHERE status IN ('filled', 'partial') AND resolution_status = 'pending'
        ORDER BY created_at ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def update_trade_resolution(
    row_id: int,
    resolution_status: str,
    pnl: Optional[float],
    resolved_at: int,
) -> None:
    """Update a filled trade execution with its win/loss outcome and P&L."""
    with transaction() as db:
        db.execute(
            """
            UPDATE trade_executions
            SET resolution_status = ?, pnl = ?, resolved_at = ?
            WHERE id = ?
            """,
            (resolution_status, pnl, resolved_at, row_id),
        )


def get_daily_loss() -> float:
    """Sum of losses (as positive USDC amount) for trades resolved since UTC midnight today."""
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    today_start = int(
        now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    )
    row = get_db().execute(
        """
        SELECT COALESCE(SUM(ABS(pnl)), 0.0) FROM trade_executions
        WHERE resolution_status = 'lost' AND resolved_at >= ?
        """,
        (today_start,),
    ).fetchone()
    return float(row[0]) if row else 0.0


def get_open_position_count() -> int:
    """Count of filled (or partially filled) trades still pending resolution."""
    row = get_db().execute(
        "SELECT COUNT(*) FROM trade_executions WHERE status IN ('filled', 'partial') AND resolution_status = 'pending'"
    ).fetchone()
    return row[0] if row else 0


def get_consecutive_losses() -> int:
    """Count the number of consecutive 'lost' resolutions at the head of the resolved history."""
    rows = get_db().execute(
        """
        SELECT resolution_status FROM trade_executions
        WHERE resolution_status IN ('won', 'lost')
        ORDER BY resolved_at DESC
        """
    ).fetchall()
    count = 0
    for row in rows:
        if row[0] == "lost":
            count += 1
        else:
            break
    return count


def get_recent_resolved_losses(limit: int = 6) -> list[dict]:
    """Return the N most recently resolved-lost trade executions with alert score."""
    rows = get_db().execute(
        """
        SELECT te.market_question, te.bet_side, te.bet_price_intended, ao.score
        FROM trade_executions te
        LEFT JOIN alert_outcomes ao ON te.alert_id = ao.alert_id
        WHERE te.resolution_status = 'lost'
        ORDER BY te.resolved_at DESC
        LIMIT ?
        """,
        (max(1, min(20, limit)),),
    ).fetchall()
    return [dict(row) for row in rows]


def get_resolved_executions_since(since_ts: int) -> list[dict]:
    """Return won/lost trade_executions since since_ts, for CB history backfill on restart."""
    rows = get_db().execute(
        """
        SELECT alert_id, pnl, resolved_at
        FROM trade_executions
        WHERE resolution_status IN ('won', 'lost')
          AND resolved_at IS NOT NULL
          AND resolved_at >= ?
        ORDER BY resolved_at ASC
        """,
        (since_ts,),
    ).fetchall()
    return [dict(row) for row in rows]


def is_market_already_traded(market_id: str) -> bool:
    """Return True if any trade_execution exists for this market_id (regardless of status)."""
    row = get_db().execute(
        "SELECT 1 FROM trade_executions WHERE market_id = ? LIMIT 1",
        (market_id,),
    ).fetchone()
    return row is not None


def get_tradeable_alerts(since_timestamp: int, min_score: int) -> list[dict]:
    """
    Return alert_outcomes that:
      - were created after since_timestamp
      - meet or exceed min_score
      - are still pending resolution (market is live)
      - have no corresponding trade_execution yet
    """
    rows = get_db().execute(
        """
        SELECT ao.alert_id, ao.market_id, ao.market_question, ao.bet_side,
               ao.bet_price_at_alert, ao.bet_size_usd, ao.score, ao.created_at
        FROM alert_outcomes ao
        LEFT JOIN trade_executions te ON ao.alert_id = te.alert_id
        WHERE ao.created_at > ?
          AND ao.score >= ?
          AND ao.resolution_status = 'pending'
          AND te.alert_id IS NULL
        ORDER BY ao.created_at ASC
        """,
        (since_timestamp, min_score),
    ).fetchall()
    return [dict(row) for row in rows]


def get_trade_stats(since_timestamp: Optional[int] = None) -> dict[str, Any]:
    """Aggregate stats for the trading bot performance section in stats.py."""
    db = get_db()

    w = "WHERE created_at >= ?" if since_timestamp else ""
    p: tuple = (since_timestamp,) if since_timestamp else ()

    def _and(clause: str) -> tuple[str, tuple]:
        if w:
            return f"{w} AND {clause}", p
        return f"WHERE {clause}", p

    total = db.execute(f"SELECT COUNT(*) FROM trade_executions {w}", p).fetchone()[0]
    if total == 0:
        return {"total": 0}

    res_clause, res_p = _and("resolution_status IN ('won','lost','invalid')")
    resolved = db.execute(f"SELECT COUNT(*) FROM trade_executions {res_clause}", res_p).fetchone()[0]
    won_clause, won_p = _and("resolution_status = 'won'")
    won = db.execute(f"SELECT COUNT(*) FROM trade_executions {won_clause}", won_p).fetchone()[0]
    lost_clause, lost_p = _and("resolution_status = 'lost'")
    lost = db.execute(f"SELECT COUNT(*) FROM trade_executions {lost_clause}", lost_p).fetchone()[0]

    pnl_clause, pnl_p = _and("pnl IS NOT NULL")
    pnl_row = db.execute(
        f"SELECT SUM(pnl), AVG(pnl) FROM trade_executions {pnl_clause}", pnl_p
    ).fetchone()
    total_pnl = pnl_row[0]
    avg_pnl = pnl_row[1]

    slip_clause, slip_p = _and("slippage IS NOT NULL")
    slip_row = db.execute(
        f"SELECT AVG(slippage), MAX(slippage) FROM trade_executions {slip_clause}", slip_p
    ).fetchone()
    avg_slippage = slip_row[0]
    max_slippage = slip_row[1]

    size_row = db.execute(
        f"SELECT AVG(size_usdc) FROM trade_executions {w}", p
    ).fetchone()
    avg_size_usdc = size_row[0]

    return {
        "total":         total,
        "resolved":      resolved,
        "won":           won,
        "lost":          lost,
        "total_pnl":     total_pnl,
        "avg_pnl":       avg_pnl,
        "avg_slippage":  avg_slippage,
        "max_slippage":  max_slippage,
        "avg_size_usdc": avg_size_usdc,
    }


# ---------------------------------------------------------------------------
# Vault sweep helpers
# ---------------------------------------------------------------------------

def log_vault_sweep(
    amount_usdc: float,
    balance_before: float,
    balance_after: float,
    vault_address: str,
    tx_hash: str,
) -> None:
    """Record a completed vault sweep in the accounting log."""
    with transaction() as db:
        db.execute(
            """
            INSERT INTO vault_sweeps
                (amount_usdc, balance_before, balance_after, vault_address, tx_hash, swept_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (amount_usdc, balance_before, balance_after, vault_address, tx_hash, int(time.time())),
        )


def get_vault_sweep_stats() -> dict[str, Any]:
    """Return total sweep count, total USDC swept, and timestamp of most recent sweep."""
    row = get_db().execute(
        "SELECT COUNT(*), COALESCE(SUM(amount_usdc), 0.0), MAX(swept_at) FROM vault_sweeps"
    ).fetchone()
    return {
        "sweep_count":   row[0] if row else 0,
        "total_swept":   row[1] if row else 0.0,
        "last_swept_at": row[2] if row else None,
    }


# ---------------------------------------------------------------------------
# API helpers — used by api.py for the remote trader endpoints
# ---------------------------------------------------------------------------

def get_tradeable_alerts_for_api(
    min_score: int,
    since_timestamp: int,
    limit: int = 20,
) -> list[dict]:
    """
    Return alert_outcomes ready for the remote trader, with clob_token_id resolved.
    Excludes alerts that already have a trade_execution row.
    Joins against markets to resolve the correct CLOB token ID for bet_side.
    """
    rows = get_db().execute(
        """
        SELECT ao.alert_id, ao.market_id, ao.market_question, ao.wallet_address,
               ao.score, ao.score_breakdown_json, ao.bet_side, ao.bet_price_at_alert,
               ao.bet_size_usd, ao.created_at, ao.market_category,
               ao.hours_to_close_at_alert, ao.is_contrarian,
               m.clob_token_ids, m.raw_json,
               te.status     AS te_status,
               te.error_message AS te_error_msg,
               te.created_at AS te_created_at
        FROM alert_outcomes ao
        LEFT JOIN trade_executions te ON ao.alert_id = te.alert_id
        LEFT JOIN markets m ON ao.market_id = m.condition_id
        WHERE ao.created_at >= ?
          AND ao.score >= ?
          AND ao.resolution_status = 'pending'
          AND (
              te.alert_id IS NULL
              OR (
                  te.status     = 'error'
                  AND te.retry_count = 0
                  AND te.created_at > (CAST(strftime('%s', 'now') AS INTEGER) - 14400)
              )
          )
        ORDER BY ao.score DESC
        LIMIT ?
        """,
        (since_timestamp, min_score, limit),
    ).fetchall()

    now_ts = int(time.time())
    results = []
    for row in rows:
        d = dict(row)
        clob_token_ids_json = d.pop("clob_token_ids", None)
        raw_json_str = d.pop("raw_json", None)

        # Log re-queue events (errored alerts being given a second chance).
        te_status    = d.pop("te_status",    None)
        te_error_msg = d.pop("te_error_msg", None)
        te_created_at = d.pop("te_created_at", None)
        if te_status == "error":
            age_min = (now_ts - te_created_at) / 60 if te_created_at else 0
            log.info(
                "[DB] Re-queuing errored alert %s — original error: %s (%.0f min ago)",
                d["alert_id"][:12], te_error_msg, age_min,
            )

        clob_token_id = None
        neg_risk: Optional[bool] = None
        market_slug = None

        raw: dict = {}
        if raw_json_str:
            try:
                raw = json.loads(raw_json_str)
            except Exception:
                pass

        if clob_token_ids_json:
            try:
                token_ids = json.loads(clob_token_ids_json)
                outcomes: list = raw.get("outcomes", [])
                target = d["bet_side"].strip().lower()
                for i, outcome in enumerate(outcomes):
                    if outcome.strip().lower() == target and i < len(token_ids):
                        clob_token_id = token_ids[i]
                        break
                if clob_token_id is None and token_ids:
                    clob_token_id = token_ids[0]
            except Exception:
                pass

        if raw:
            neg_risk_val = raw.get("negRisk")
            if neg_risk_val is not None:
                neg_risk = bool(neg_risk_val)
            market_slug = raw.get("_event_slug") or raw.get("slug") or None

        d["clob_token_id"] = clob_token_id
        d["neg_risk"] = neg_risk
        d["market_slug"] = market_slug

        results.append(d)

    return results


def get_open_positions_for_api() -> list[dict]:
    """Return filled, pending-resolution trades joined with score from alert_outcomes."""
    rows = get_db().execute(
        """
        SELECT te.alert_id, te.market_id, te.market_question,
               te.bet_side, te.bet_price_filled, te.bet_price_intended,
               te.size_usdc, te.created_at,
               ao.score
        FROM trade_executions te
        LEFT JOIN alert_outcomes ao ON te.alert_id = ao.alert_id
        WHERE te.status IN ('filled', 'partial') AND te.resolution_status = 'pending'
        ORDER BY te.created_at ASC
        """,
    ).fetchall()
    return [dict(row) for row in rows]


def get_pending_trades_with_resolution() -> list[dict]:
    """
    Return filled trade_executions still pending resolution, joined with the
    current alert_outcomes.resolution_status. Used by the remote trader to
    detect resolutions and fire Telegram notifications.
    """
    rows = get_db().execute(
        """
        SELECT te.id, te.alert_id, te.market_id, te.market_question,
               te.bet_side, te.bet_price_intended, te.bet_price_filled,
               te.size_usdc, te.order_id, te.status, te.created_at,
               te.resolution_status, te.pnl, te.resolved_at,
               ao.resolution_status AS alert_resolution_status,
               ao.winning_outcome
        FROM trade_executions te
        LEFT JOIN alert_outcomes ao ON te.alert_id = ao.alert_id
        WHERE te.status IN ('filled', 'partial') AND te.resolution_status = 'pending'
        ORDER BY te.created_at ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def update_trade_resolution_by_alert_id(
    alert_id: str,
    resolution_status: str,
    pnl: Optional[float],
    resolved_at: int,
) -> None:
    """Update trade_executions resolution from the remote trader's report."""
    with transaction() as db:
        db.execute(
            """
            UPDATE trade_executions
            SET resolution_status = ?, pnl = ?, resolved_at = ?
            WHERE alert_id = ?
            """,
            (resolution_status, pnl, resolved_at, alert_id),
        )


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
