"""Tests for the brain conviction size-up (2026-06-23) — the trader sizes a bet up toward
the cap only when the brain returns a high-conviction CONFIRM (from the REAL-TIME vet at trade
time, or the cached confirm fallback), clamped so it never makes a fundable trade unaffordable.
Pure decision logic + betslip rendering; no network, no on-chain calls."""
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


def _arm(today=0):
    tr.BRAIN_SIZEUP_ENABLED = True
    tr.BRAIN_SIZEUP_MAX_USDC = 15.0
    tr.BRAIN_SIZEUP_MIN_CONFIDENCE = 0.75
    tr.BRAIN_SIZEUP_MIN_EDGE = 0.12
    tr.BRAIN_SIZEUP_MAX_PER_DAY = 5
    tr._brain_sizeups_today = today


# A high-conviction CONFIRM verdict, as returned by the real-time vet (or the cache fallback).
_GOOD = {"verdict": "CONFIRM", "confidence": 0.82, "edge": 0.20,
         "take": "Market's mispricing a healthy favorite."}


def test_disabled_never_sizes_up():
    _arm()
    tr.BRAIN_SIZEUP_ENABLED = False
    size, info = tr._apply_brain_sizeup(2.0, _GOOD, 100.0)
    assert size == 2.0 and info is None
    print("  [ok] disabled flag → never sizes up")


def test_no_verdict_keeps_base():
    _arm()
    assert tr._apply_brain_sizeup(2.0, None, 100.0) == (2.0, None)   # brain returned nothing
    print("  [ok] no verdict (real-time vet failed / no confirm) → base size")


def test_non_confirm_keeps_base():
    _arm()
    veto = {"verdict": "VETO", "confidence": 0.9, "edge": -0.2, "take": "overpriced"}
    neutral = {"verdict": "NEUTRAL", "confidence": 0.9, "edge": 0.02, "take": "fair"}
    assert tr._apply_brain_sizeup(2.0, veto, 100.0) == (2.0, None)
    assert tr._apply_brain_sizeup(2.0, neutral, 100.0) == (2.0, None)
    print("  [ok] VETO / NEUTRAL verdict → base size (only CONFIRM sizes up)")


def test_low_conviction_keeps_base():
    _arm()
    assert tr._apply_brain_sizeup(2.0, {"verdict": "CONFIRM", "confidence": 0.60, "edge": 0.20}, 100.0) == (2.0, None)
    assert tr._apply_brain_sizeup(2.0, {"verdict": "CONFIRM", "confidence": 0.90, "edge": 0.05}, 100.0) == (2.0, None)
    print("  [ok] CONFIRM but low confidence OR thin edge → base size")


def test_high_conviction_sizes_to_cap():
    _arm()
    size, info = tr._apply_brain_sizeup(2.0, _GOOD, 100.0)
    assert size == 15.0 and info is _GOOD     # bumped to the cap, plenty of cash
    print("  [ok] high-conviction CONFIRM + cash → sized to the $15 cap, carries the take")


def test_clamped_to_available_cash():
    _arm()
    size, info = tr._apply_brain_sizeup(2.0, _GOOD, 9.0)   # only $9 free above reserve
    assert size == 9.0 and info is _GOOD
    size, info = tr._apply_brain_sizeup(2.0, _GOOD, 1.5)   # avail below base → no size-up
    assert size == 2.0 and info is None
    print("  [ok] clamped to free cash above reserve; never shrinks the base trade")


def test_daily_cap_blocks_further_sizeups():
    _arm(today=5)   # cap already spent
    assert tr._apply_brain_sizeup(2.0, _GOOD, 100.0) == (2.0, None)
    print("  [ok] per-day cap reached → no more size-ups")


def test_unknown_balance_allows_cap():
    _arm()
    size, info = tr._apply_brain_sizeup(2.0, _GOOD, -1.0)   # balance unknown
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
    test_no_verdict_keeps_base()
    test_non_confirm_keeps_base()
    test_low_conviction_keeps_base()
    test_high_conviction_sizes_to_cap()
    test_clamped_to_available_cash()
    test_daily_cap_blocks_further_sizeups()
    test_unknown_balance_allows_cap()
    test_betslip_renders_brain_take_and_escapes()
    print("all brain-sizeup tests passed")
