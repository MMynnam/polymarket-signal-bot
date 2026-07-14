"""Tests for the GRADUATION SPRINT (2026-07-13) — regime-aware Stage-1 gates (only
current-regime evidence counts toward graduation) and the self-limiting sprint flag.
Uses a real temp SQLite DB; no network, no anthropic SDK."""
import importlib.util
import os
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


config = _load("config", os.path.join(ROOT, "config.py"))
database = _load("database", os.path.join(ROOT, "database.py"))
brain = _load("brain_sprint_test", os.path.join(ROOT, "brain.py"))

T0 = 1783814400  # the pre-registered regime boundary (2026-07-12T00:00Z)


def _setup_db():
    database.init_db(os.path.join(tempfile.mkdtemp(), "sprint.db"))
    config.BRAIN_REGIME_START_TS = T0
    db = database.get_db()

    def pick(alert_id, created_at, res, pnl, size=5.0):
        db.execute(
            "INSERT INTO trade_executions (alert_id, market_id, market_question, clob_token_id,"
            " bet_side, size_usdc, status, created_at, resolution_status, pnl) "
            "VALUES (?, 'm', 'q', 't', 'Yes', ?, 'filled', ?, ?, ?)",
            (alert_id, size, created_at, res, pnl))

    def forecast(created_at, bb, bm):
        db.execute(
            "INSERT INTO brain_forecasts (created_at, source, market_id, question, target_label,"
            " market_price, brain_prob_raw, brain_prob, confidence, edge, verdict,"
            " resolved_outcome, brier_brain, brier_market) "
            "VALUES (?, 'scanner', 'm', 'q', 'Yes', 0.5, 0.6, 0.6, 0.7, 0.1, 'UNDERPRICED',"
            " 1, ?, ?)",
            (created_at, bb, bm))

    # OLD regime: 2 graded picks (big losses) + 1 graded forecast where brain loses badly.
    pick("brain_old1", T0 - 86400, "lost", -5.0)
    pick("brain_old2", T0 - 5000, "lost", -5.0)
    forecast(T0 - 86400, 0.9, 0.1)
    # NEW regime: 3 graded winners + 2 graded forecasts where brain beats market.
    pick("brain_new1", T0 + 100, "won", 2.0)
    pick("brain_new2", T0 + 200, "won", 2.5)
    pick("brain_new3", T0 + 300, "won", 1.5)
    forecast(T0 + 100, 0.05, 0.20)
    forecast(T0 + 200, 0.04, 0.25)
    db.commit()


_setup_db()


def test_gates_count_only_new_regime():
    g = brain.stage1_gates()
    assert g["n_graded"] == 3, g          # old-regime picks excluded
    assert g["brier_n"] == 2, g           # old-regime forecast excluded
    assert g["roi_mean"] > 0, g           # new-regime winners only; old losses don't dilute
    assert g["regime_start"] == T0
    print("  [ok] gates count only current-regime evidence")


def test_sprint_flag_self_limits():
    config.BRAIN_SPRINT_ENABLED = True
    config.BRAIN_SPRINT_TARGET = 40
    assert brain.stage1_gates()["sprint"] is True     # 3 < 40 → sprinting
    config.BRAIN_SPRINT_TARGET = 3
    assert brain.stage1_gates()["sprint"] is False    # target reached → auto-revert
    config.BRAIN_SPRINT_ENABLED = False
    config.BRAIN_SPRINT_TARGET = 40
    assert brain.stage1_gates()["sprint"] is False    # kill switch works
    config.BRAIN_SPRINT_ENABLED = True
    print("  [ok] sprint flag self-limits at target and honors the kill switch")


def test_refresh_sprint_state_sets_module_flag():
    config.BRAIN_SPRINT_TARGET = 40
    assert brain._refresh_sprint_state() is True and brain._sprint_now is True
    config.BRAIN_SPRINT_TARGET = 3
    assert brain._refresh_sprint_state() is False and brain._sprint_now is False
    config.BRAIN_SPRINT_TARGET = 40
    print("  [ok] _refresh_sprint_state drives the module flag")


def test_old_regime_dilution_is_the_bug_being_fixed():
    # Sanity: pooled lifetime would show 5 graded with mean dragged down by the -100% olds;
    # the regime gate sees +ROI. This is the exact mechanism that made Stage 1 unreachable.
    db = database.get_db()
    rows = db.execute("SELECT pnl, size_usdc FROM trade_executions "
                      "WHERE resolution_status IN ('won','lost')").fetchall()
    pooled = [r["pnl"] / r["size_usdc"] for r in rows]
    assert len(pooled) == 5 and sum(pooled) / 5 < 0      # lifetime book is net-negative
    assert brain.stage1_gates()["roi_mean"] > 0          # current regime is positive
    print("  [ok] regime gate un-buries the current brain from the old book")


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    print(f"sprint/regime tests ({len(fns)}):")
    for fn in fns:
        fn()
    print(f"ALL {len(fns)} PASSED")


if __name__ == "__main__":
    _run_all()
