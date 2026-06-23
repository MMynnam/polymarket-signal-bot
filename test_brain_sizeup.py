"""Tests for the brain conviction size-up (2026-06-23) — the trader sizes a bet up
toward the cap only when the brain has independently confirmed that exact market+side
with high conviction, clamped so it never makes a fundable trade unaffordable. Pure
decision logic + betslip rendering; no network, no on-chain calls."""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tr = _load("tr_sizeup", os.path.join(ROOT, "fly-trader", "trader_remote.py"))


def _arm(confirmations=None, today=0):
    tr.BRAIN_SIZEUP_ENABLED = True
    tr.BRAIN_SIZEUP_MAX_USDC = 15.0
    tr.BRAIN_SIZEUP_MIN_CONFIDENCE = 0.75
    tr.BRAIN_SIZEUP_MIN_EDGE = 0.12
    tr.BRAIN_SIZEUP_MAX_PER_DAY = 5
    tr._brain_sizeups_today = today
    tr._brain_confirmations = confirmations or {}


_GOOD = {"confidence": 0.82, "edge": 0.20, "take": "Market's mispricing a healthy favorite."}


def test_disabled_never_sizes_up():
    _arm({("m1", "Yes"): _GOOD})
    tr.BRAIN_SIZEUP_ENABLED = False
    size, info = tr._apply_brain_sizeup(2.0, "m1", "Yes", 100.0)
    assert size == 2.0 and info is None
    print("  [ok] disabled flag → never sizes up")


def test_no_confirmation_keeps_base():
    _arm({("m1", "Yes"): _GOOD})
    size, info = tr._apply_brain_sizeup(2.0, "m2", "Yes", 100.0)   # different market
    assert size == 2.0 and info is None
    size, info = tr._apply_brain_sizeup(2.0, "m1", "No", 100.0)    # different side
    assert size == 2.0 and info is None
    print("  [ok] no matching confirmation → base size")


def test_low_conviction_keeps_base():
    _arm({("m1", "Yes"): {"confidence": 0.60, "edge": 0.20, "take": "meh"}})
    assert tr._apply_brain_sizeup(2.0, "m1", "Yes", 100.0) == (2.0, None)
    _arm({("m1", "Yes"): {"confidence": 0.90, "edge": 0.05, "take": "thin edge"}})
    assert tr._apply_brain_sizeup(2.0, "m1", "Yes", 100.0) == (2.0, None)
    print("  [ok] low confidence OR thin edge → base size")


def test_high_conviction_sizes_to_cap():
    _arm({("m1", "Yes"): _GOOD})
    size, info = tr._apply_brain_sizeup(2.0, "m1", "Yes", 100.0)
    assert size == 15.0 and info is _GOOD     # bumped to the cap, plenty of cash
    print("  [ok] high conviction + cash → sized to the $15 cap, carries the take")


def test_clamped_to_available_cash():
    _arm({("m1", "Yes"): _GOOD})
    # only $9 free above the reserve → size up to $9, never the $15 cap, never a skip
    size, info = tr._apply_brain_sizeup(2.0, "m1", "Yes", 9.0)
    assert size == 9.0 and info is _GOOD
    # avail below base → no size-up (don't shrink a fundable base trade)
    size, info = tr._apply_brain_sizeup(2.0, "m1", "Yes", 1.5)
    assert size == 2.0 and info is None
    print("  [ok] clamped to free cash above reserve; never shrinks the base trade")


def test_daily_cap_blocks_further_sizeups():
    _arm({("m1", "Yes"): _GOOD}, today=5)   # cap already spent
    assert tr._apply_brain_sizeup(2.0, "m1", "Yes", 100.0) == (2.0, None)
    print("  [ok] per-day cap reached → no more size-ups")


def test_unknown_balance_allows_cap():
    _arm({("m1", "Yes"): _GOOD})
    size, info = tr._apply_brain_sizeup(2.0, "m1", "Yes", -1.0)   # balance unknown
    assert size == 15.0 and info is _GOOD
    print("  [ok] unknown balance (-1) → cap allowed (reserve gate still runs after)")


def test_betslip_renders_brain_take_and_escapes():
    plain = tr._build_slip_text("Knicks make playoffs?", "Yes", 0.40, 2.0, bet_no=7)
    assert "brain conviction" not in plain          # no brain line on a normal bet
    sized = tr._build_slip_text("Knicks <playoffs> & more", "Yes", 0.40, 15.0, bet_no=8,
                                brain_take="Healthy roster, soft schedule — 40c is a gift.")
    assert "brain conviction" in sized and "sized up" in sized
    assert "gift." in sized
    assert "&lt;playoffs&gt;" in sized              # HTML-escaped question
    assert "$15.00" in sized                        # the bigger stake
    print("  [ok] betslip shows brain rationale only when sized up; HTML-escaped")


if __name__ == "__main__":
    test_disabled_never_sizes_up()
    test_no_confirmation_keeps_base()
    test_low_conviction_keeps_base()
    test_high_conviction_sizes_to_cap()
    test_clamped_to_available_cash()
    test_daily_cap_blocks_further_sizeups()
    test_unknown_balance_allows_cap()
    test_betslip_renders_brain_take_and_escapes()
    print("all brain-sizeup tests passed")
