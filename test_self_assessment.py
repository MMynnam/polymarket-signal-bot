"""Tests for self_assessment.py — the bot's honest self-read + propose-only tuner.

Guarantees: (1) says nothing beyond 'insufficient data' below the min book; (2) computes
the price-controlled edge correctly; (3) NEVER proposes a weight change unless a component
clears a strict, cost-cleared, walk-forward bar in BOTH time halves. Pure-stdlib."""
import importlib.util, os, sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


sa = _load("sa", os.path.join(ROOT, "self_assessment.py"))


def _bet(won, px, ts, **comps):
    return {"won": won, "px": px, "score": 70, "ts": ts, "comps": comps}


def test_edge_metric():
    book = [_bet(1, 0.5, 0), _bet(0, 0.5, 1), _bet(1, 0.5, 2), _bet(1, 0.5, 3)]
    n, wr, mp, edge = sa._edge(book)
    assert n == 4 and abs(wr - 0.75) < 1e-9 and abs(mp - 0.5) < 1e-9
    assert abs(edge - 25.0) < 1e-9
    print("  [ok] edge = (win_rate - mean_px) in pp")


def test_insufficient_data_says_nothing():
    book = [_bet(1, 0.5, i) for i in range(10)]  # < _MIN_BOOK
    text, proposal = sa.format_self_assessment(book)
    assert "insufficient" in text.lower() or "below the" in text.lower()
    assert proposal is None
    assert "no edge claimed, none invented" in text
    print("  [ok] below min book: reports insufficient data, no proposal")


def test_no_proposal_on_pure_noise():
    # 200 coin-flip bets, component values random-ish but uncorrelated with outcome
    import math
    book = []
    for i in range(200):
        won = 1 if (i * 7) % 2 == 0 else 0          # deterministic ~50/50, independent of comp
        comp = (i * 13) % 30                          # spread component values
        book.append(_bet(won, 0.5, i, size_anomaly=comp))
    text, proposal = sa.format_self_assessment(book)
    assert proposal is None, "must NOT propose on noise"
    assert "none" in text.lower()
    print("  [ok] pure noise: no weight-change proposal (the whole point)")


def test_proposal_requires_both_halves():
    # Construct a component that predicts ONLY in the first half (in-sample artifact).
    # It must NOT yield a proposal — walk-forward requires both halves.
    book = []
    n = 200
    for i in range(n):
        first_half = i < n // 2
        comp = 30 if (i % 3 == 0) else 0
        # in the first half, high comp wins; in the second half, no relationship
        if first_half:
            won = 1 if comp >= 30 else 0
        else:
            won = 1 if (i % 2 == 0) else 0
        book.append(_bet(won, 0.40, i, size_anomaly=comp))
    text, proposal = sa.format_self_assessment(book)
    assert proposal is None, "must not propose when the signal fails the second half"
    print("  [ok] proposal needs BOTH time halves (no in-sample artifacts)")


def test_proposal_fires_on_genuine_durable_signal():
    # A genuinely durable, large, cost-clearing component edge in BOTH halves SHOULD propose
    # (proves the tuner isn't dead — it's strict, not broken). Synthetic, not real data.
    book = []
    n = 240
    for i in range(n):
        comp = 30 if (i % 2 == 0) else 0   # half high, half low
        # high-comp bets win ~90% at price 0.50 (huge real edge), low-comp ~30% — in ALL halves
        if comp >= 30:
            won = 1 if (i % 10 != 0) else 0     # ~90%
        else:
            won = 1 if (i % 10 < 3) else 0      # ~30%
        book.append(_bet(won, 0.50, i, size_anomaly=comp))
    text, proposal = sa.format_self_assessment(book)
    assert proposal is not None and proposal[0] == "size_anomaly"
    assert "review — NOT applied" in text or "NOT applied" in text
    print("  [ok] genuine durable cost-cleared signal DOES propose (review-only)")


if __name__ == "__main__":
    test_edge_metric()
    test_insufficient_data_says_nothing()
    test_no_proposal_on_pure_noise()
    test_proposal_requires_both_halves()
    test_proposal_fires_on_genuine_durable_signal()
    print("all self-assessment tests passed")
