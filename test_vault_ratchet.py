"""Tests for the continuous win-ratchet (2026-06-13).

Each win banks SWEEP_WIN_PCT of its profit into a vault tab; losses don't. The on-chain
settle fires only when the tab clears the min batch AND there's free cash above the
operating floor. Pure-stdlib, no network, no on-chain calls."""
import importlib.util, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tr = _load("tr_ratchet", os.path.join(ROOT, "fly-trader", "trader_remote.py"))


def test_slice_is_pct_of_profit():
    assert tr._ratchet_slice(3.0, 0.33) == 0.99
    assert tr._ratchet_slice(6.06, 0.33) == round(6.06 * 0.33, 4)
    # losses / non-positive profit bank nothing
    assert tr._ratchet_slice(-2.0, 0.33) == 0.0
    assert tr._ratchet_slice(0.0, 0.33) == 0.0
    assert tr._ratchet_slice(None, 0.33) == 0.0
    print("  [ok] slice = pct of profit, zero on losses")


def test_accrual_accumulates_and_debits():
    tr._vault_tab = 0.0
    s1 = tr._accrue_vault_tab(3.0)   # +0.99
    s2 = tr._accrue_vault_tab(6.0)   # +1.98
    assert s1 == 0.99 and s2 == 1.98
    assert abs(tr._vault_tab - 2.97) < 1e-9
    assert tr._accrue_vault_tab(-5.0) == 0.0  # loss: no change
    assert abs(tr._vault_tab - 2.97) < 1e-9
    tr._debit_vault_tab(2.0)
    assert abs(tr._vault_tab - 0.97) < 1e-9
    tr._debit_vault_tab(99.0)         # clamps at 0, never negative
    assert tr._vault_tab == 0.0
    tr._vault_tab = 0.0
    print("  [ok] tab accumulates on wins, ignores losses, debits clamp at 0")


def test_settle_amount_gating():
    floor, min_settle = 40.0, 8.0
    # tab below the min batch -> no settle
    assert tr._ratchet_settle_amount(5.0, 100.0, floor, min_settle) == 0.0
    # tab ready but no free headroom above floor -> no settle
    assert tr._ratchet_settle_amount(20.0, 45.0, floor, min_settle) == 0.0  # headroom 5 < 8
    # tab ready and plenty of headroom -> sweep the whole tab
    assert tr._ratchet_settle_amount(20.0, 100.0, floor, min_settle) == 20.0
    # headroom caps the sweep below the tab
    assert tr._ratchet_settle_amount(50.0, 60.0, floor, min_settle) == 20.0  # 60-40
    # None balance (RPC failed) -> no settle
    assert tr._ratchet_settle_amount(20.0, None, floor, min_settle) == 0.0
    print("  [ok] settle fires only on min-batch + free-cash-above-floor")


def test_settle_card_shows_lifeline():
    win = dict(market_question="Lakers vs Celtics", bet_side="Lakers",
               bet_price_filled=0.50, winning_outcome="Lakers", alert_id="z1")
    text, _ = tr._build_settle_text(win, "won", 2.00, 1, "", threaded=True,
                                    balance=42.0, vault_slice=0.66, vault_total=5.28)
    assert "+$0.66" in text and "lifeline $5.28" in text
    assert "vault" in text.lower()
    # bank line is replaced by the lifeline line when a slice is banked
    assert "bank $42" not in text
    # no slice -> falls back to the plain bank line
    text2, _ = tr._build_settle_text(win, "won", 2.00, 1, "", threaded=True,
                                     balance=42.0, vault_slice=0.0, vault_total=5.28)
    assert "bank $42.00" in text2 and "lifeline" not in text2
    print("  [ok] settle card shows the banked slice + lifeline, falls back cleanly")


if __name__ == "__main__":
    test_slice_is_pct_of_profit()
    test_accrual_accumulates_and_debits()
    test_settle_amount_gating()
    test_settle_card_shows_lifeline()
    print("all vault-ratchet tests passed")
