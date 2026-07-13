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
