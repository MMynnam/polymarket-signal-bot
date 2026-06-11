"""Tests for the 2026-06-11 side-resolution fix: resolve_token_id_for_side must
parse Gamma's JSON-string outcomes, match the bet side to the right token, and
NEVER fall back to token_ids[0]. Plus the trade_monitor SELL skip.
Pure-stdlib, no live calls, no DB."""
import json
import database
import trade_monitor


def test_resolves_string_encoded_outcomes():
    # Gamma stores outcomes as a JSON-encoded STRING — the historical bug.
    tids = json.dumps(["tok_yes", "tok_no"])
    assert database.resolve_token_id_for_side(tids, '["Yes", "No"]', "Yes") == "tok_yes"
    assert database.resolve_token_id_for_side(tids, '["Yes", "No"]', "No") == "tok_no"
    # case/whitespace insensitive
    assert database.resolve_token_id_for_side(tids, '["Yes", "No"]', "  no ") == "tok_no"
    assert database.resolve_token_id_for_side(tids, '["Up", "Down"]', "DOWN") == "tok_no"
    print("  [ok] string-encoded outcomes resolve to the correct token")


def test_resolves_list_outcomes():
    tids = json.dumps(["t0", "t1"])
    assert database.resolve_token_id_for_side(tids, ["Cleveland Guardians", "New York Yankees"],
                                              "New York Yankees") == "t1"
    print("  [ok] plain-list outcomes resolve")


def test_never_falls_back_to_token0():
    tids = json.dumps(["t0", "t1"])
    # Unknown side -> None, NOT t0 (the bug that bought token[0] on 636/636 fills).
    assert database.resolve_token_id_for_side(tids, '["Yes", "No"]', "Over") is None
    assert database.resolve_token_id_for_side(tids, "[]", "Yes") is None
    assert database.resolve_token_id_for_side(tids, None, "Yes") is None
    assert database.resolve_token_id_for_side(tids, "not json at all", "Yes") is None
    assert database.resolve_token_id_for_side(None, '["Yes", "No"]', "Yes") is None
    assert database.resolve_token_id_for_side(tids, '["Yes", "No"]', "") is None
    print("  [ok] unresolvable side returns None (no token[0] fallback)")


def test_normalized_name_fallback():
    tids = json.dumps(["t0", "t1"])
    # punctuation-stripped alert side still resolves by NAME (never position)
    assert database.resolve_token_id_for_side(
        tids, '["Anyone\'s Legend", "Team WE"]', "Anyones Legend") == "t0"
    assert database.resolve_token_id_for_side(
        tids, '["Team WE", "Anyone\'s Legend"]', "Anyones Legend") == "t1"
    # ambiguous normalization (two outcomes collapse to same key) -> None
    assert database.resolve_token_id_for_side(
        tids, '["A-B", "A.B"]', "A B") is None
    print("  [ok] normalized name fallback (non-positional)")


def test_malformed_inputs_return_none():
    assert database.resolve_token_id_for_side("not json", '["Yes","No"]', "Yes") is None
    assert database.resolve_token_id_for_side(json.dumps(["only_one"]), '["Yes","No"]', "No") is None
    assert database.resolve_token_id_for_side(json.dumps({"a": 1}), '["Yes","No"]', "Yes") is None
    print("  [ok] malformed inputs return None")


def test_rest_parse_skips_sells():
    base = {"id": "t1", "outcome": "Yes", "price": 0.5, "size": 2000,
            "amount": 1000, "timestamp": 1780000000, "proxyWallet": "0xabc"}
    buy = dict(base, side="BUY")
    sell = dict(base, side="SELL")
    assert trade_monitor._parse_trade_from_rest(buy, "0xmkt") is not None
    assert trade_monitor._parse_trade_from_rest(sell, "0xmkt") is None
    assert trade_monitor._parse_trade_from_rest(dict(base, side="sell "), "0xmkt") is None
    # rows without a side field still parse (legacy shape)
    assert trade_monitor._parse_trade_from_rest(base, "0xmkt") is not None
    print("  [ok] REST parser skips insider SELLs")


if __name__ == "__main__":
    test_resolves_string_encoded_outcomes()
    test_resolves_list_outcomes()
    test_never_falls_back_to_token0()
    test_normalized_name_fallback()
    test_malformed_inputs_return_none()
    test_rest_parse_skips_sells()
    print("all side-resolution tests passed")
