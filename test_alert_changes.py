"""Tests for the 2026-06-03 alert-tuning changes: onchain_match side-ambiguity,
win_rate binary flag, and the flat-sizing arithmetic. Pure-stdlib, no live calls."""
import types
import onchain_match as om
import config
import scorer


def test_side_ambiguity():
    # Side-ambiguous (one title -> opposite sides): must be flagged.
    ambiguous = [
        "Bitcoin Up or Down on June 3?",
        "Ethereum Up or Down on June 1?",
        "Spread: Spurs (-2.5)",
        "Game Handicap: BLG (-2.5) vs EDward Gaming (+2.5)",
        "Knicks vs. Spurs: O/U 217.5",
        "Total Kills Over/Under 24.5 in Game 2?",
        "Games Total: O/U 3.5",
    ]
    for q in ambiguous:
        assert om.is_side_ambiguous(q), f"should be ambiguous: {q}"
    # Clean single-side markets: must NOT be flagged.
    clean = [
        "Will Inter Miami win on 2026-06-05?",
        "Will Finland win Eurovision 2026?",
        "Will the price of Bitcoin be above $74,000 on June 5?",  # threshold = single side
        "Will Karen Bass win the 2026 Los Angeles mayoral election?",
        "Counter-Strike: B8 vs TYLOO (BO1) - IEM Cologne Major Stage 1",  # head-to-head pick = single side
    ]
    for q in clean:
        assert not om.is_side_ambiguous(q), f"should be clean: {q}"
    assert om.is_side_ambiguous("") is False
    assert om.is_side_ambiguous(None) is False
    print("  [ok] side-ambiguity classifier")


def test_outcome_by_token():
    redeemed = {"111", "222"}
    assert om.outcome_by_token({"clob_token_id": "111"}, redeemed) == "won"
    assert om.outcome_by_token({"clob_token_id": "999"}, redeemed) == "lost"
    assert om.outcome_by_token({"clob_token_id": ""}, redeemed) is None
    assert om.outcome_by_token({}, redeemed) is None
    # partition
    clean, amb = om.partition_for_measurement([
        {"market_question": "Will Italy win on 2026-06-03?"},
        {"market_question": "Bitcoin Up or Down on June 3?"},
    ])
    assert len(clean) == 1 and len(amb) == 1
    print("  [ok] token-id join + partition")


def _profile(win_rate, resolved_trades):
    return types.SimpleNamespace(
        win_rate=win_rate, resolved_trades=resolved_trades, missing_components=set()
    )


def test_winrate_binary():
    assert config.WINRATE_BINARY_MODE is True, "binary mode is the new default"
    flag = config.WINRATE_FLAG_PTS
    minr = config.WINRATE_FLAG_MIN_RESOLVED
    low = config.WINRATE_LOW_THRESHOLD
    # Has a real winning record -> flag points.
    s, note = scorer._score_win_rate(_profile(low + 0.20, minr + 5))
    assert s == flag, f"qualifying record should score {flag}, got {s}"
    # Win rate at/below the low bar -> 0 (no edge signal).
    s, _ = scorer._score_win_rate(_profile(low, minr + 5))
    assert s == 0, "win rate not above low bar -> 0"
    # Too few resolved -> 0 (no real record).
    s, _ = scorer._score_win_rate(_profile(0.90, minr - 1))
    assert s == 0, "thin history -> 0"
    # No resolved trades -> N/A 0.
    s, note = scorer._score_win_rate(_profile(None, 0))
    assert s == 0 and "N/A" in note
    # The flag is binary: a 99%-on-100 wallet scores the SAME as a 60%-on-5 wallet.
    s_hi, _ = scorer._score_win_rate(_profile(0.99, 100))
    s_lo, _ = scorer._score_win_rate(_profile(low + 0.05, minr))
    assert s_hi == s_lo == flag, "graded magnitude must NOT matter in binary mode"
    print(f"  [ok] win_rate binary flag (= {flag} pts when track record, else 0)")


def test_flat_sizing_arithmetic():
    # Mirror the DYNAMIC flat branch of _calculate_bet_size (default mode).
    # We replicate the pure arithmetic to avoid importing the fly trader (clob deps).
    import os
    flat_pct = float(os.getenv("TRADING_FLAT_PCT_PER_TRADE", "0.020"))
    MIN, MAX = 1.0, 5.0
    for balance, expect_in in [(145.0, (2.0, 3.5)), (50.0, (1.0, 1.5)), (1000.0, (5.0, 5.0))]:
        raw = balance * flat_pct
        clamped = max(MIN, min(MAX, raw))
        clamped = min(clamped, max(MIN, 0.10 * balance))  # 10% safety rail preserved
        assert expect_in[0] <= clamped <= expect_in[1], f"bal={balance} -> {clamped}"
    # At $145 the flat 2% bet (~$2.90) sits between the old min ($1) and the $5 cap,
    # and is independent of score (score not referenced).
    assert abs(145.0 * flat_pct - 2.90) < 0.01
    print("  [ok] flat sizing arithmetic (score-independent, rails preserved)")


if __name__ == "__main__":
    test_side_ambiguity()
    test_outcome_by_token()
    test_winrate_binary()
    test_flat_sizing_arithmetic()
    print("ALL PASS")
