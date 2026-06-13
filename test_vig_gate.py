"""Tests for the opposite-side vig gate (2026-06-13 edge audit).

Once we hold one side of a market, a second side is only worth taking if the two entry
prices lock in a profit (sum <= cap); otherwise it copies the insiders' overround as a
mechanical guaranteed loss (verified WR=50% by identity, -3.5..-4.9pp drag, ~45-54% of
the tradeable stream). Pure-stdlib, no network, no DB."""
import importlib.util, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


tr = _load("tr_vig", os.path.join(ROOT, "fly-trader", "trader_remote.py"))
vig = tr._opposite_side_vig
CAP = tr.TRADING_OPPOSITE_SIDE_MAX_SUM  # 0.98 default


def test_blocks_locked_loss():
    # hold "Yes" @ 0.55; new "No" @ 0.55 -> sum 1.10 > 0.98 -> locked loss -> skip
    skip, s = vig(0.55, {"Yes": 0.55}, "No", CAP)
    assert skip and abs(s - 1.10) < 1e-9
    # the dominant real case: both legs near the money
    assert vig(0.52, {"Over": 0.52}, "Under", CAP)[0]
    print("  [ok] blocks the guaranteed-loss pair")


def test_allows_cheap_hedge_or_arb():
    # hold "Yes" @ 0.40; new "No" @ 0.40 -> sum 0.80 <= 0.98 -> real arb -> allow
    skip, s = vig(0.40, {"Yes": 0.40}, "No", CAP)
    assert not skip and abs(s - 0.80) < 1e-9
    # exactly at the cap is allowed (locks in ~2%)
    assert not vig(0.49, {"Yes": 0.49}, "No", CAP)[0]
    print("  [ok] allows a genuine cheap hedge / arb")


def test_same_side_and_no_holdings_never_skip():
    # same side is handled by the dedup SET, not this gate
    assert not vig(0.90, {"Yes": 0.55}, "Yes", CAP)[0]
    # nothing held in this market
    assert not vig(0.90, {}, "No", CAP)[0]
    assert not vig(0.90, None, "No", CAP)[0]
    # missing new price -> never skip (don't act on unknown)
    assert not vig(None, {"Yes": 0.55}, "No", CAP)[0]
    print("  [ok] same-side / empty / unknown-price never trip the gate")


def test_multi_outcome_uses_tightest_pair():
    # 3-outcome market, hold A@0.30 and B@0.50; new C@0.55 -> worst pair 0.55+0.50=1.05 > cap
    skip, s = vig(0.55, {"A": 0.30, "B": 0.50}, "C", CAP)
    assert skip and abs(s - 1.05) < 1e-9
    # but a cheap third leg against only cheap holdings is allowed
    assert not vig(0.20, {"A": 0.30, "B": 0.40}, "C", CAP)[0]
    print("  [ok] multi-outcome uses the tightest (most -EV) pair")


def test_cap_is_configurable():
    # a permissive cap (>=1.0) restores the old always-take-second-side behavior
    assert not vig(0.55, {"Yes": 0.55}, "No", 1.10)[0]
    print("  [ok] cap is configurable (>=1.0 disables the gate)")


if __name__ == "__main__":
    test_blocks_locked_loss()
    test_allows_cheap_hedge_or_arb()
    test_same_side_and_no_holdings_never_skip()
    test_multi_outcome_uses_tightest_pair()
    test_cap_is_configurable()
    print("all vig-gate tests passed")
