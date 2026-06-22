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


def test_pct_is_50():
    # The operator set the ratchet to bank HALF of each win's profit.
    assert tr.SWEEP_WIN_PCT == 0.50
    assert tr._ratchet_slice(2.00) == 1.00   # default pct = 0.50
    print("  [ok] ratchet banks 50% of win profit")


def test_accrual_accumulates_and_debits():
    tr._vault_tab = 0.0
    s1 = tr._accrue_vault_tab(3.0)   # +1.50 (50%)
    s2 = tr._accrue_vault_tab(6.0)   # +3.00
    assert s1 == 1.50 and s2 == 3.00
    assert abs(tr._vault_tab - 4.50) < 1e-9
    assert tr._accrue_vault_tab(-5.0) == 0.0  # loss: no change
    assert abs(tr._vault_tab - 4.50) < 1e-9
    tr._debit_vault_tab(2.0)
    assert abs(tr._vault_tab - 2.50) < 1e-9
    tr._debit_vault_tab(99.0)         # clamps at 0, never negative
    assert tr._vault_tab == 0.0
    tr._vault_tab = 0.0
    print("  [ok] tab accumulates 50% on wins, ignores losses, debits clamp at 0")


def test_settle_amount_gating_and_cap():
    floor, min_settle, mx = 6.0, 3.0, 20.0
    # tab below the min batch -> no settle
    assert tr._ratchet_settle_amount(2.0, 100.0, floor, min_settle, mx) == 0.0
    # tab ready but no free headroom above floor -> no settle
    assert tr._ratchet_settle_amount(10.0, 8.0, floor, min_settle, mx) == 0.0  # headroom 2 < 3
    # tab ready, headroom available -> sweep the tab
    assert tr._ratchet_settle_amount(10.0, 100.0, floor, min_settle, mx) == 10.0
    # headroom caps the sweep below the tab
    assert tr._ratchet_settle_amount(50.0, 20.0, floor, min_settle, mx) == 14.0  # 20-6
    # the MAX_BATCH cap bounds it even with plenty of cash + a huge tab
    assert tr._ratchet_settle_amount(500.0, 1000.0, floor, min_settle, mx) == 20.0
    # None balance (RPC failed) -> no settle
    assert tr._ratchet_settle_amount(10.0, None, floor, min_settle, mx) == 0.0
    print("  [ok] settle fires on min-batch + headroom; capped at max-batch")


def test_reserve_holds_pending_sweep():
    # The reserve is the floor normally; while a sweep is pending it ALSO holds the
    # in-flight amount, so the bot won't re-bet the cash before the timelock withdraw.
    tr._sweep_state = "idle"; tr._sweep_intended_amount = 0.0
    assert tr._sweep_reserve_usdc() == tr.VAULT_RATCHET_FLOOR_USDC
    tr._sweep_state = "pause_pending"; tr._sweep_intended_amount = 12.0
    assert tr._sweep_reserve_usdc() == tr.VAULT_RATCHET_FLOOR_USDC + 12.0
    tr._sweep_state = "pause_ready"
    assert tr._sweep_reserve_usdc() == tr.VAULT_RATCHET_FLOOR_USDC + 12.0
    tr._sweep_state = "idle"; tr._sweep_intended_amount = 0.0
    print("  [ok] reserve = floor, plus in-flight sweep while pending")


def test_settle_card_honest_vault_line():
    win = dict(market_question="Lakers vs Celtics", bet_side="Lakers",
               bet_price_filled=0.50, winning_outcome="Lakers", alert_id="z1")
    # vault_secured is the REAL on-chain balance; the slice is "set aside" (earmarked).
    text, _ = tr._build_settle_text(win, "won", 2.00, 1, "", threaded=True,
                                    balance=42.0, vault_slice=1.00, vault_secured=5.28)
    assert "+$1.00" in text and "set aside" in text
    assert "vault holds <b>$5.28</b>" in text
    # honest: must NOT claim the slice is already locked/secured/in the vault
    assert "lifeline" not in text
    assert "locked away" not in text
    assert "can't be re-risked" not in text
    # unknown real vault balance (-1) -> show the slice, omit the secured figure
    text2, _ = tr._build_settle_text(win, "won", 2.00, 1, "", threaded=True,
                                     balance=42.0, vault_slice=1.00, vault_secured=-1.0)
    assert "set aside" in text2 and "vault holds" not in text2
    # no slice -> plain bank line
    text3, _ = tr._build_settle_text(win, "won", 2.00, 1, "", threaded=True,
                                     balance=42.0, vault_slice=0.0, vault_secured=5.28)
    assert "bank $42.00" in text3 and "set aside" not in text3
    print("  [ok] settle card: honest earmark + real secured total, no false claims")


if __name__ == "__main__":
    test_slice_is_pct_of_profit()
    test_pct_is_50()
    test_accrual_accumulates_and_debits()
    test_settle_amount_gating_and_cap()
    test_reserve_holds_pending_sweep()
    test_settle_card_honest_vault_line()
    print("all vault-ratchet tests passed")
