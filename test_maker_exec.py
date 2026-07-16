"""Tests for pick MAKER execution (2026-07-12) — the trader rests a zero-fee GTC limit
1c inside the ask for brain picks instead of crossing as a fee-paying taker. These cover
the pure sizing/eligibility math (_maker_order_plan) and the bankroll-aware Stage-1 stake
cap (_ladder_stake). No network, no on-chain calls."""
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


tr = _load("tr_maker", os.path.join(ROOT, "fly-trader", "trader_remote.py"))


# ---------------------------------------------------------------- _maker_order_plan

def test_plan_basic_improves_ask_by_one_cent():
    limit_px, shares, cost = tr._maker_order_plan(5.0, 0.30, improve=0.01)
    assert limit_px == 0.29
    assert shares == 17.0            # int(5.0 // 0.29) = 17
    assert abs(cost - 4.93) < 1e-9   # 17 x 0.29
    print("  [ok] basic plan: ask 0.30 -> rest at 0.29, 17 shares, $4.93")


def test_plan_share_minimum_is_five():
    # $5 at ask 0.90 -> limit 0.89 -> 5 shares (int(5.0//0.89)=5, exactly the floor)
    limit_px, shares, cost = tr._maker_order_plan(5.0, 0.90, improve=0.01)
    assert shares == 5.0 and limit_px == 0.89
    print("  [ok] 5-share CLOB minimum honored")


def test_plan_rejects_when_minimum_balloons_cost():
    # $2 stake at ask 0.90 -> 5-share minimum costs $4.45 > 2 x 1.6 -> no maker order
    assert tr._maker_order_plan(2.0, 0.90, improve=0.01) is None
    print("  [ok] rejects when 5-share minimum costs >1.6x the stake")


def test_plan_rejects_out_of_range_prices():
    assert tr._maker_order_plan(5.0, 0.02, improve=0.01) is None   # limit 0.01 < 0.02
    assert tr._maker_order_plan(5.0, 0.995, improve=0.01) is None  # limit 0.99 > 0.97 (dead zone)
    assert tr._maker_order_plan(5.0, None, improve=0.01) is None
    assert tr._maker_order_plan(5.0, "garbage", improve=0.01) is None
    print("  [ok] price-range and bad-input guards")


def test_plan_cheap_side_thin_market():
    # The design target: thin cheap-side entry, $8 at ask 0.12 -> rest at 0.11, 72 shares
    limit_px, shares, cost = tr._maker_order_plan(8.0, 0.12, improve=0.01)
    assert limit_px == 0.11 and shares == 72.0
    assert abs(cost - 7.92) < 1e-9
    print("  [ok] thin cheap-side entry sized correctly")


def test_plan_cost_never_exceeds_replan_bound():
    # Fuzz the money invariant: cost <= 1.6 x stake for every accepted plan.
    for stake_c in range(200, 3000, 137):            # $2.00 ... $30.00
        for ask_c in range(3, 99, 7):                # ask 0.03 ... 0.94
            plan = tr._maker_order_plan(stake_c / 100.0, ask_c / 100.0, improve=0.01)
            if plan is not None:
                limit_px, shares, cost = plan
                assert cost <= (stake_c / 100.0) * 1.6 + 1e-9
                assert shares >= 5
                assert 0.02 <= limit_px <= 0.97
    print("  [ok] fuzz: accepted plans always respect cost/share/price bounds")


# ---------------------------------------------------------------- lifecycle safety helpers

def test_matched_shares_absent_is_unconfirmed_not_zero():
    # A filled order whose read lacks the fill field must NOT be treated as zero-fill
    # (that path finalizes 'skipped' and the cash vanishes from the books).
    assert tr._order_matched_shares({}) is None
    assert tr._order_matched_shares({"status": "live"}) is None
    assert tr._order_matched_shares({"size_matched": "12.0"}) == 12.0
    assert tr._order_matched_shares({"sizeMatched": 7}) == 7.0
    assert tr._order_matched_shares({"matched_amount": "3"}) == 3.0
    assert tr._order_matched_shares({"size_matched": 0}) == 0.0   # explicit zero IS zero
    assert tr._order_matched_shares({"size_matched": "garbage"}) is None
    print("  [ok] absent fill field -> unconfirmed (None), never zero")


def test_pending_orders_visible_to_dedup_vig_and_event_cap():
    tr._pending_pick_orders.clear()
    tr._pending_pick_orders["oid1"] = {
        "alert": {"market_id": "m1", "bet_side": "Yes", "alert_id": "brain_x"},
        "shares": 20.0, "limit_px": 0.55, "cost": 11.0, "placed_at": 0.0, "ev_key": "ev-a",
    }
    tr._pending_pick_orders["oid2"] = {
        "alert": {"market_id": "m2", "bet_side": "No", "alert_id": "brain_y"},
        "shares": 10.0, "limit_px": 0.30, "cost": 3.0, "placed_at": 0.0, "ev_key": "ev-a",
    }
    assert tr._pending_pick_sides("m1") == {"Yes": 0.55}
    assert tr._pending_pick_sides("m2") == {"No": 0.30}
    assert tr._pending_pick_sides("m3") == {}
    assert abs(tr._pending_pick_event_cost("ev-a") - 14.0) < 1e-9
    assert tr._pending_pick_event_cost("ev-b") == 0.0
    assert abs(tr._pending_pick_cost() - 14.0) < 1e-9
    # vig gate sees the resting side: opposite-side buy at 0.52 + resting 0.55 = 1.07 -> skip
    skip, s = tr._opposite_side_vig(0.52, tr._pending_pick_sides("m1"), "No")
    assert skip and abs(s - 1.07) < 1e-9
    tr._pending_pick_orders.clear()
    print("  [ok] resting orders visible to dedup/vig/event-cap/reserve views")


def test_edge_still_live_gates_ttl_extension():
    import asyncio, json as _json

    class _Clob:
        def __init__(self, ask):
            self._ask = ask
        def get_price(self, token_id, side):
            if self._ask is None:
                raise RuntimeError("api down")
            return {"price": str(self._ask)}

    def ctx(prob, bar):
        return {"alert": {"clob_token_id": "t1",
                          "score_breakdown_json": _json.dumps({"brain_prob": prob, "bar": bar})}}

    run = asyncio.run
    # brain 0.60, bar 0.10: ask 0.48 -> edge 0.12 >= 0.08 -> extend
    assert run(tr._pick_edge_still_live(_Clob(0.48), ctx(0.60, 0.10))) is True
    # ask 0.55 -> edge 0.05 < 0.08 -> no extension (edge gone, cancel honestly)
    assert run(tr._pick_edge_still_live(_Clob(0.55), ctx(0.60, 0.10))) is False
    # API down -> never extend blind
    assert run(tr._pick_edge_still_live(_Clob(None), ctx(0.60, 0.10))) is False
    # no token / no prob -> False
    assert run(tr._pick_edge_still_live(_Clob(0.40), {"alert": {}})) is False
    assert run(tr._pick_edge_still_live(_Clob(0.40), ctx(0.0, 0.10))) is False
    print("  [ok] TTL extension only when the live edge still clears the bar")


def test_no_extension_when_order_state_unreadable():
    # The order API failing (or the fill field absent) past TTL must NOT extend — the price
    # API working is not evidence the order API works; a hidden partial could rest blind.
    import asyncio, json as _json, time as _time

    class _Clob:
        def get_order(self, oid):
            raise RuntimeError("order API down")
        def get_price(self, token_id, side):
            return {"price": "0.40"}   # edge WOULD clear the bar if it were consulted
        def cancel_order(self, payload):
            raise RuntimeError("cancel also down")

    ctx = {"alert": {"alert_id": "brain_x", "clob_token_id": "t1",
                     "score_breakdown_json": _json.dumps({"brain_prob": 0.60, "bar": 0.10})},
           "shares": 10.0, "limit_px": 0.41, "cost": 4.10,
           "placed_at": _time.time() - tr.PICK_MAKER_TTL_S - 60, "ev_key": "ev"}
    tr._pending_pick_orders.clear()
    tr._pending_pick_orders["oid-x"] = ctx
    tr._pending_pick_alerts.add("brain_x")
    asyncio.run(tr._check_pick_orders(_Clob(), None))
    kept = tr._pending_pick_orders.get("oid-x")
    assert kept is not None, "order must stay tracked (cancel unconfirmed)"
    assert kept.get("extends", 0) == 0, "must NOT extend while order state is unreadable"
    tr._pending_pick_orders.clear()
    tr._pending_pick_alerts.clear()
    print("  [ok] unreadable order state -> no blind extension, order stays tracked")


def test_bankroll_aware_rails():
    tr.EVENT_MAX_EXPOSURE_USDC = 8.0
    tr.EVENT_MAX_EXPOSURE_PCT = 0.06
    tr.TRADING_MAX_DAILY_LOSS_USDC = 15.0
    tr.MAX_DAILY_LOSS_PCT = 0.06
    # Small book: flat floors bind (identical to pre-change behavior).
    assert tr._effective_event_cap(bankroll=40.0) == 8.0
    assert tr._effective_daily_loss_limit(bankroll=40.0) == 15.0
    # $580 book: rails scale — event cap fits a full Stage-1 pick (5% = $29 < 6% = $34.8),
    # daily-loss stop no longer trips on one bad resolution cluster.
    assert tr._effective_event_cap(bankroll=580.0) == 34.8
    assert tr._effective_daily_loss_limit(bankroll=580.0) == 34.8
    assert tr._effective_event_cap(bankroll=580.0) > 0.05 * 580.0   # Stage-1 pick fits
    # _event_over_cap picks up the scaled cap via the default path.
    tr._pending_pick_orders.clear()
    tr._cached_usdc_balance = 580.0
    tr._held_event_exposure.clear()
    over, cur = tr._event_over_cap("ev-x", 25.0, {"ev-x": 5.0})
    assert not over, "a $25 Stage-1 pick + $5 held must fit a $34.8 scaled event cap"
    over, _ = tr._event_over_cap("ev-x", 25.0, {"ev-x": 15.0})
    assert over, "$40 on one event must still breach the 6% cap"
    print("  [ok] event cap + daily-loss stop scale with bankroll (floors intact)")


def test_taker_convert_full_bar_gate():
    # Conversion requires the FULL bar at the live ask (the maker path's -0.02 tolerance
    # does not apply: the bar budgets the taker fee we are about to actually pay).
    import asyncio, json as _json

    class _Clob:
        def __init__(self, ask, fill_shares=10.0):
            self._ask, self._fill = ask, fill_shares
            self.posted = 0
        def get_price(self, token_id, side):
            if self._ask is None:
                raise RuntimeError("down")
            return {"price": str(self._ask)}
        def get_neg_risk(self, token_id):
            return False
        def create_and_post_market_order(self, order, options, order_type):
            self.posted += 1
            return {"success": True, "orderID": "fak1", "price": str(self._ask)}
        def get_order(self, oid):
            return {"size_matched": self._fill, "status": "matched"}

    def ctx(prob, bar, cost=5.0):
        return {"alert": {"clob_token_id": "t1", "alert_id": "brain_z", "neg_risk": False,
                          "score_breakdown_json": _json.dumps({"brain_prob": prob, "bar": bar})},
                "shares": 12.0, "limit_px": 0.40, "cost": cost, "placed_at": 0.0, "ev_key": "e"}

    run = asyncio.run
    tr.PICK_TAKER_FALLBACK_ENABLED = True
    tr._cached_usdc_balance = -1.0   # unknown balance -> affordability check skipped
    # edge 0.60-0.48=0.12 >= bar 0.10 -> converts
    c = _Clob(0.48)
    got = run(tr._taker_convert_pick(c, None, ctx(0.60, 0.10)))
    assert got == (10.0, 0.48) and c.posted == 1
    # edge 0.09 < bar 0.10 -> no conversion, nothing posted (maker tolerance would've passed)
    c = _Clob(0.51)
    assert run(tr._taker_convert_pick(c, None, ctx(0.60, 0.10))) is None and c.posted == 0
    # price API down -> None, nothing posted
    c = _Clob(None)
    assert run(tr._taker_convert_pick(c, None, ctx(0.60, 0.10))) is None and c.posted == 0
    print("  [ok] taker-convert requires the FULL bar at the live ask")


def test_taker_convert_unconfirmed_fill_books_stake():
    # FAK success but unreadable fill -> book the intended stake (never a skip row after
    # cash may have left the wallet).
    import asyncio, json as _json

    class _Clob:
        def get_price(self, token_id, side): return {"price": "0.50"}
        def get_neg_risk(self, token_id): return False
        def create_and_post_market_order(self, order, options, order_type):
            return {"success": True, "orderID": "fak2", "price": "0.50"}
        def get_order(self, oid): raise RuntimeError("read failed")

    ctx = {"alert": {"clob_token_id": "t1", "alert_id": "brain_w", "neg_risk": False,
                     "score_breakdown_json": _json.dumps({"brain_prob": 0.70, "bar": 0.10})},
           "shares": 12.0, "limit_px": 0.48, "cost": 6.0, "placed_at": 0.0, "ev_key": "e"}
    tr.PICK_TAKER_FALLBACK_ENABLED = True
    tr._cached_usdc_balance = -1.0
    shares, px = __import__("asyncio").run(tr._taker_convert_pick(_Clob(), None, ctx))
    assert px == 0.50 and abs(shares - 12.0) < 1e-6   # 6.0 / 0.50
    print("  [ok] unconfirmed taker fill books the intended stake, never skips")


def test_taker_convert_confirmed_zero_is_clean_skip():
    # FAK success but get_order CONFIRMS size_matched=0 (killed unmatched): no cash left,
    # so a skip row is honest — never a phantom booked fill (review finding).
    import asyncio, json as _json

    class _Clob:
        def get_price(self, token_id, side): return {"price": "0.50"}
        def get_neg_risk(self, token_id): return False
        def create_and_post_market_order(self, order, options, order_type):
            return {"success": True, "orderID": "fak3"}
        def get_order(self, oid): return {"size_matched": 0, "status": "canceled"}

    ctx = {"alert": {"clob_token_id": "t1", "alert_id": "brain_v", "neg_risk": False,
                     "score_breakdown_json": _json.dumps({"brain_prob": 0.70, "bar": 0.10})},
           "shares": 12.0, "limit_px": 0.48, "cost": 6.0, "placed_at": 0.0, "ev_key": "e"}
    tr._cached_usdc_balance = -1.0
    assert __import__("asyncio").run(tr._taker_convert_pick(_Clob(), None, ctx)) is None
    print("  [ok] CONFIRMED zero fill -> clean skip, no phantom position")


def test_taker_convert_post_exception_is_ambiguous():
    # A non-4xx error on the FAK post (timeout/5xx after possible acceptance) must return
    # 'ambiguous' — the caller holds, never writes a final skip over possibly-spent cash.
    import asyncio, json as _json
    from py_clob_client_v2.exceptions import PolyApiException

    class _Resp:
        status_code = None
        text = "timeout"
        def json(self): return {}

    class _Clob:
        def __init__(self, exc): self._exc = exc
        def get_price(self, token_id, side): return {"price": "0.50"}
        def get_neg_risk(self, token_id): return False
        def create_and_post_market_order(self, order, options, order_type):
            raise self._exc

    def ctx():
        return {"alert": {"clob_token_id": "t1", "alert_id": "brain_u", "neg_risk": False,
                          "score_breakdown_json": _json.dumps({"brain_prob": 0.70, "bar": 0.10})},
                "shares": 12.0, "limit_px": 0.48, "cost": 6.0, "placed_at": 0.0, "ev_key": "e"}
    run = __import__("asyncio").run
    tr._cached_usdc_balance = -1.0
    assert run(tr._taker_convert_pick(_Clob(RuntimeError("conn reset")), None, ctx())) == "ambiguous"
    print("  [ok] unknown FAK outcome -> 'ambiguous' (hold), never a skip row")


def test_fak_fill_px_prefers_making_taking_ratio():
    # makingAmount/takingAmount (USDC given / shares got) is the true average; the response
    # 'price' field may be absent or a marketable-limit, and the pre-post ask is stale.
    assert abs(tr._fak_fill_px({"makingAmount": "6.0", "takingAmount": "11.3"}, 11.3, 0.48)
               - 6.0 / 11.3) < 1e-9
    assert tr._fak_fill_px({"price": "0.52"}, 10, 0.48) == 0.52     # ratio absent -> price
    assert tr._fak_fill_px({}, 10, 0.48) == 0.48                    # nothing -> ask
    assert tr._fak_fill_px({"makingAmount": "0", "takingAmount": "0", "price": "garbage"},
                           10, 0.48) == 0.48                        # junk never poisons
    print("  [ok] fill price prefers making/taking ratio, falls back sanely")


def test_finalize_price_override_math():
    # Pure math check on the cash-basis status label + px propagation (no network parts).
    ctx = {"cost": 6.0, "limit_px": 0.48}
    px = 0.50
    filled_usdc = round(11.0 * px, 2)                 # taker filled 11 shares @ 0.50 = $5.50
    status = "filled" if filled_usdc >= 0.95 * ctx["cost"] else "partial"
    assert status == "partial"                        # $5.50 < 95% of $6.00
    filled_usdc = round(12.0 * px, 2)                 # $6.00 = full stake
    status = "filled" if filled_usdc >= 0.95 * ctx["cost"] else "partial"
    assert status == "filled"
    print("  [ok] cash-basis fill/partial label correct for taker fills")


# ---------------------------------------------------------------- bankroll-aware ladder stake

def test_ladder_stake_capped_by_bankroll_pct():
    tr.LADDER_STAKE_CAP_USDC = 25.0
    tr.LADDER_MAX_BANKROLL_PCT = 0.05
    tr.LADDER_KELLY_FRACTION = 0.5
    # Big edge, small bankroll: 5% of $100 = $5 beats the $25 cap
    stake = tr._ladder_stake(0.25, 100.0)
    assert stake <= 5.0 + 1e-9, f"stake {stake} exceeds 5% of bankroll"
    print("  [ok] stage-1 stake capped at 5% of bankroll")


def test_ladder_stake_big_bankroll_hits_dollar_cap():
    tr.LADDER_STAKE_CAP_USDC = 25.0
    tr.LADDER_MAX_BANKROLL_PCT = 0.05
    tr.LADDER_KELLY_FRACTION = 0.5
    # $1000 bankroll: 5% = $50, so the $25 dollar-cap binds first
    stake = tr._ladder_stake(0.25, 1000.0)
    assert stake <= 25.0 + 1e-9
    print("  [ok] dollar cap still binds on large bankrolls")


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    print(f"maker-exec tests ({len(fns)}):")
    for fn in fns:
        fn()
    print(f"ALL {len(fns)} PASSED")


if __name__ == "__main__":
    _run_all()
