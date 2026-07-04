"""Tests for the brain's pure forecasting logic — calibration, edge, Kelly, Brier,
and the hard daily spend cap. No network, no anthropic SDK, no DB. These are the
load-bearing numbers; the Claude pipeline around them is forward-validated live."""
import importlib.util
import math
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


brain = _load("brain_under_test", os.path.join(ROOT, "brain.py"))


def test_platt_extremizes_away_from_half():
    # coef>1 pushes probabilities toward 0/1 (de-hedges the LLM); 0.5 is a fixed point.
    assert abs(brain.platt_calibrate(0.5, 1.73) - 0.5) < 1e-9
    assert brain.platt_calibrate(0.70, 1.73) > 0.70   # above .5 → more extreme up
    assert brain.platt_calibrate(0.30, 1.73) < 0.30   # below .5 → more extreme down
    # symmetry: p and 1-p stay mirror images
    assert abs(brain.platt_calibrate(0.30, 1.73) + brain.platt_calibrate(0.70, 1.73) - 1.0) < 1e-9
    # coef=1 is the identity
    assert abs(brain.platt_calibrate(0.42, 1.0) - 0.42) < 1e-9
    # never returns exactly 0/1 (clamped)
    assert 0.0 < brain.platt_calibrate(0.999999, 1.73) < 1.0
    print("  [ok] platt extremizes away from 0.5, symmetric, identity at coef=1")


def test_edge_sign_and_value():
    assert abs(brain.edge(0.70, 0.55) - 0.15) < 1e-9   # brain thinks outcome underpriced
    assert brain.edge(0.40, 0.55) < 0                  # overpriced
    print("  [ok] edge = brain_prob - market_price (signed)")


def test_kelly_zero_when_no_edge_and_capped():
    cap = 0.25
    assert brain.kelly_fraction(0.55, 0.55, cap) == 0.0          # no edge → no stake
    # YES side: f = (q-p)/(1-p)
    assert abs(brain.kelly_fraction(0.70, 0.50, 1.0) - (0.20 / 0.50)) < 1e-9
    # complement side: f = (p-q)/p
    assert abs(brain.kelly_fraction(0.30, 0.50, 1.0) - (0.20 / 0.50)) < 1e-9
    # cap binds
    assert brain.kelly_fraction(0.95, 0.30, cap) == cap
    # never negative
    assert brain.kelly_fraction(0.50, 0.50, cap) >= 0.0
    print("  [ok] kelly: zero at no-edge, symmetric sides, capped, non-negative")


def test_decide_verdicts():
    # veto source: confirm when brain > price by the threshold, veto when below
    d = brain.decide(0.75, 0.55, "veto", edge_threshold=0.10, min_confidence=0.5, confidence=0.8)
    assert d["verdict"] == "CONFIRM" and d["act"] is True
    d = brain.decide(0.40, 0.60, "veto", edge_threshold=0.10, min_confidence=0.5, confidence=0.8)
    assert d["verdict"] == "VETO" and d["act"] is True
    d = brain.decide(0.57, 0.55, "veto", edge_threshold=0.10, min_confidence=0.5, confidence=0.8)
    assert d["verdict"] == "NEUTRAL" and d["act"] is False   # edge below threshold
    # scanner source labels
    assert brain.decide(0.75, 0.55, "scanner", 0.10, 0.5, 0.9)["verdict"] == "UNDERPRICED"
    assert brain.decide(0.40, 0.60, "scanner", 0.10, 0.5, 0.9)["verdict"] == "OVERPRICED"
    # low confidence blocks the act flag even with a big edge
    d = brain.decide(0.80, 0.55, "scanner", 0.10, 0.6, confidence=0.4)
    assert d["act"] is False
    print("  [ok] decide: CONFIRM/VETO/NEUTRAL + scanner labels; confidence gates act")


def test_brier_and_aggregate():
    assert brain.brier(1.0, 1) == 0.0
    assert brain.brier(0.0, 1) == 1.0
    assert abs(brain.brier(0.7, 1) - 0.09) < 1e-9
    # brain confidently right, market at the coin flip → brain wins the comparison
    rows = [(0.9, 0.5, 1), (0.8, 0.5, 1), (0.1, 0.5, 0)]
    agg = brain.aggregate_brier(rows)
    assert agg["n"] == 3
    assert agg["brain"] < agg["market"]
    # unresolved (None) rows are ignored
    assert brain.aggregate_brier([(0.9, 0.5, None)])["n"] == 0
    print("  [ok] brier + aggregate: brain<market when brain is better calibrated")


class _Usage:
    def __init__(self, i, o, cc=0, cr=0):
        self.input_tokens = i
        self.output_tokens = o
        self.cache_creation_input_tokens = cc
        self.cache_read_input_tokens = cr


def test_spend_tracker_cap_and_cost():
    st = brain.SpendTracker(1.00)
    assert st.remaining() == 1.00 and st.can_afford(0.5)
    # Sonnet: $3/1M in, $15/1M out → 100k in + 20k out = 0.30 + 0.30 = $0.60
    cost = st.record_usage("claude-sonnet-4-6", _Usage(100_000, 20_000))
    assert abs(cost - 0.60) < 1e-6
    assert abs(st.spent_today() - 0.60) < 1e-6
    # web search adds $0.01 each
    st.record_web_searches(3)
    assert abs(st.spent_today() - 0.63) < 1e-6
    # now over-budget for a $0.50 estimate
    assert not st.can_afford(0.50)
    assert st.remaining() < 0.40
    # Haiku is cheaper: $1/1M in, $5/1M out
    st2 = brain.SpendTracker(10.0)
    c = st2.record_usage("claude-haiku-4-5", _Usage(10_000, 1_000))
    assert abs(c - (10_000 * 1.0 + 1_000 * 5.0) / 1e6) < 1e-9
    print("  [ok] spend tracker: correct per-model cost, web-search cost, cap enforcement")


def _mkt(outcomes, prices, tokens, tradeable=True):
    return {"tradeable": tradeable, "outcomes": outcomes, "prices": prices, "clob_token_ids": tokens}


def test_brain_pick_side_buys_the_underpriced_side_with_correct_token():
    YES, NO = "Yes", "No"
    T0, T1 = "tokenYES", "tokenNO"
    # outcomes[0] underpriced: brain 70% vs price 50% → buy Yes (token0)
    p = brain._brain_pick_side(_mkt([YES, NO], [0.50, 0.50], [T0, T1]), 0.70)
    assert p["buy_side"] == YES and p["buy_token"] == T0 and abs(p["buy_price"] - 0.50) < 1e-9
    assert abs(p["edge"] - 0.20) < 1e-9 and abs(p["brain_prob_side"] - 0.70) < 1e-9
    # outcomes[1] underpriced: brain 30% on Yes → buy No (token1), edge on No side
    p = brain._brain_pick_side(_mkt([YES, NO], [0.50, 0.50], [T0, T1]), 0.30)
    assert p["buy_side"] == NO and p["buy_token"] == T1
    assert abs(p["edge"] - 0.20) < 1e-9 and abs(p["brain_prob_side"] - 0.70) < 1e-9
    # asymmetric prices: brain 40% on Yes, prices [0.55,0.45] → No is underpriced (0.60 vs 0.45)
    p = brain._brain_pick_side(_mkt([YES, NO], [0.55, 0.45], [T0, T1]), 0.40)
    assert p["buy_side"] == NO and p["buy_token"] == T1 and abs(p["buy_price"] - 0.45) < 1e-9
    assert abs(p["edge"] - 0.15) < 1e-9
    # token order must follow the chosen side, never default to token[0] (the historic bug)
    p = brain._brain_pick_side(_mkt(["Up", "Down"], [0.20, 0.80], ["tA", "tB"]), 0.10)
    assert p["buy_side"] == "Down" and p["buy_token"] == "tB"   # Down underpriced (0.90 vs 0.80)
    # not tradeable (no token ids) → None
    assert brain._brain_pick_side(_mkt([YES, NO], [0.5, 0.5], [T0, T1], tradeable=False), 0.7) is None
    print("  [ok] brain pick: buys the underpriced side with the index-correct token (no side bug)")


def test_billing_breaker_and_output_config():
    import time as _t
    # billing error arms the cooldown; unrelated errors don't
    brain._billing_pause_until = 0.0
    assert brain._note_billing_error(Exception("Your credit balance is too low to access the Anthropic API")) is True
    assert brain._billing_paused() is True
    brain._billing_pause_until = 0.0
    assert brain._note_billing_error(Exception("connection reset by peer")) is False
    assert brain._billing_paused() is False
    # cooldown expires
    brain._billing_pause_until = _t.time() - 1
    assert brain._billing_paused() is False
    # output_config: effort only on non-Haiku models; format always when schema given
    c = brain._output_config({"type": "object"}, "claude-haiku-4-5")
    assert "format" in c and "effort" not in c        # Haiku rejects the effort param
    c = brain._output_config({"type": "object"}, "claude-sonnet-4-6")
    assert "format" in c and "effort" in c
    print("  [ok] billing breaker arms only on credit errors; Haiku calls omit effort")


def test_clamp_guards_bad_model_output():
    assert brain._clamp(1.4) == 1.0
    assert brain._clamp(-0.2) == 0.0
    assert brain._clamp("oops") == 0.0
    assert brain._clamp(None) == 0.0
    print("  [ok] _clamp guards malformed probabilities from the model")


if __name__ == "__main__":
    test_platt_extremizes_away_from_half()
    test_edge_sign_and_value()
    test_kelly_zero_when_no_edge_and_capped()
    test_decide_verdicts()
    test_brier_and_aggregate()
    test_spend_tracker_cap_and_cost()
    test_brain_pick_side_buys_the_underpriced_side_with_correct_token()
    test_billing_breaker_and_output_config()
    test_clamp_guards_bad_model_output()
    print("all brain tests passed")
