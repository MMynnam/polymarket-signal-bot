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


def _arm_veto(enabled=True):
    tr.BRAIN_VETO_SKIP_ENABLED = enabled
    tr.BRAIN_VETO_MIN_CONFIDENCE = 0.50
    tr.BRAIN_VETO_MIN_EDGE = 0.10


def test_veto_skip_decision():
    _arm_veto(True)
    # strong veto: VETO, confident, side overpriced by >= 10pp → skip
    strong = {"verdict": "VETO", "confidence": 0.60, "edge": -0.15}
    assert tr._brain_skip_veto(strong) is True
    # not a veto → never skips
    assert tr._brain_skip_veto({"verdict": "CONFIRM", "confidence": 0.9, "edge": 0.3}) is False
    assert tr._brain_skip_veto({"verdict": "NEUTRAL", "confidence": 0.9, "edge": -0.2}) is False
    # weak veto: low confidence → don't skip (like the real #1059 at conf 0.45)
    assert tr._brain_skip_veto({"verdict": "VETO", "confidence": 0.45, "edge": -0.15}) is False
    # thin edge → don't skip
    assert tr._brain_skip_veto({"verdict": "VETO", "confidence": 0.8, "edge": -0.05}) is False
    # None / disabled
    assert tr._brain_skip_veto(None) is False
    _arm_veto(False)
    assert tr._brain_skip_veto(strong) is False
    _arm_veto(True)
    print("  [ok] veto-skip: only a confident, strong-edge VETO skips; CONFIRM/NEUTRAL/weak don't")


def test_brain_pick_stake_scales_with_edge():
    tr.BRAIN_PICK_SIZE_USDC = 2.0
    tr.BRAIN_PICK_MAX_SIZE_USDC = 5.0
    tr.BRAIN_PICK_EDGE_SLOPE = 15.0
    assert tr._brain_pick_stake(0.08) == 2.0          # min edge → base
    assert tr._brain_pick_stake(0.18) == 3.5          # 2 + 15×0.10
    assert tr._brain_pick_stake(0.28) == 5.0          # hits the cap
    assert tr._brain_pick_stake(0.60) == 5.0          # cap binds
    assert tr._brain_pick_stake(0.00) == 2.0          # below min edge → base, never less
    assert tr._brain_pick_stake(None) == 2.0          # malformed → base
    print("  [ok] pick stake: base at min edge, scales with conviction, hard $5 cap")


def test_betslip_renders_brain_verdict_on_every_vetted_slip():
    # Un-vetted bet → no brain line at all.
    plain = tr._build_slip_text("Knicks make playoffs?", "Yes", 0.40, 2.0, bet_no=7)
    assert "🧠" not in plain
    # Sized-up CONFIRM → conviction line + take, bigger stake, HTML-escaped.
    sized = tr._build_slip_text("Knicks <playoffs> & more", "Yes", 0.40, 15.0, bet_no=8,
                                brain_verdict={"verdict": "CONFIRM", "take": "40c is a gift.",
                                               "brain_prob": 0.66, "market_price": 0.40},
                                brain_sized_up=True)
    assert "CONVICTION" in sized and "sized up" in sized and "gift." in sized
    assert "&lt;playoffs&gt;" in sized and "$15.00" in sized
    # VETO → the brain's disagreement is shown, bet still rides at base.
    veto = tr._build_slip_text("England vs Ghana O/U 3.5", "Over", 0.39, 1.95, bet_no=9,
                               brain_verdict={"verdict": "VETO", "take": "leans Under.",
                                              "brain_prob": 0.28, "market_price": 0.39})
    assert "leans the other way" in veto and "riding the signal anyway" in veto
    assert "28% vs market 39%" in veto and "leans Under." in veto
    # NEUTRAL → coin-flip line.
    neutral = tr._build_slip_text("Will Mexico win?", "Yes", 0.52, 1.5, bet_no=10,
                                  brain_verdict={"verdict": "NEUTRAL", "take": "no clear edge.",
                                                 "brain_prob": 0.53, "market_price": 0.52})
    assert "coin-flip" in neutral and "no edge to add" in neutral
    print("  [ok] betslip shows the brain's verdict on EVERY vetted slip (agree/veto/neutral)")


if __name__ == "__main__":
    test_disabled_never_sizes_up()
    test_no_verdict_keeps_base()
    test_non_confirm_keeps_base()
    test_low_conviction_keeps_base()
    test_high_conviction_sizes_to_cap()
    test_clamped_to_available_cash()
    test_daily_cap_blocks_further_sizeups()
    test_unknown_balance_allows_cap()
    test_veto_skip_decision()
    test_brain_pick_stake_scales_with_edge()
    test_betslip_renders_brain_verdict_on_every_vetted_slip()
    print("all brain-sizeup tests passed")
