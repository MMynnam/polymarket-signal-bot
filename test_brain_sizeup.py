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


def test_event_key_groups_derivative_markets():
    # All legs of the same match share one exposure bucket…
    base = tr._event_key("fifwc-eng-gha-2026-06-23", "0xA")
    assert tr._event_key("fifwc-eng-gha-2026-06-23-more-markets", "0xB") == base
    assert tr._event_key("fifwc-eng-gha-2026-06-23-exact-score", "0xC") == base
    assert tr._event_key("fifwc-eng-gha-2026-06-23-halftime-result", "0xD") == base
    # …different matches don't
    assert tr._event_key("fifwc-pan-hrv-2026-06-23", "0xE") != base
    # no slug → market stands alone (fail-safe, never wrongly groups)
    assert tr._event_key(None, "0xF") == "0xF"
    assert tr._event_key("", "0xG") == "0xG"
    print("  [ok] event key: derivative suffixes collapse to one bucket; no slug = alone")


def test_event_cap_decision():
    tr.EVENT_MAX_EXPOSURE_USDC = 8.0
    exp = {"fifwc-eng-gha": 6.50}
    over, cur = tr._event_over_cap("fifwc-eng-gha", 2.00, exp)
    assert over is True and cur == 6.50          # 6.50 + 2.00 > 8
    over, _ = tr._event_over_cap("fifwc-eng-gha", 1.00, exp)
    assert over is False                          # 7.50 <= 8
    over, cur = tr._event_over_cap("unseen-event", 5.00, exp)
    assert over is False and cur == 0.0           # new event starts at zero
    print("  [ok] event cap: blocks the leg that would breach, allows under-cap adds")


def test_depth_capped_size():
    asks = [{"price": "0.50", "size": "40"},   # $20 within tolerance
            {"price": "0.51", "size": "20"},   # $10.20 within tolerance
            {"price": "0.60", "size": "500"},  # outside 0.50+0.02 → ignored
            {"price": "bogus", "size": "x"}]   # malformed → skipped
    # depth = 30.20; 15% → $4.53 cap
    assert tr._depth_capped_size(asks, 0.50, 15.0, tol=0.02, frac=0.15) == 4.53
    # desired below the cap → unchanged
    assert tr._depth_capped_size(asks, 0.50, 2.0, tol=0.02, frac=0.15) == 2.0
    # tuple rows work; empty book → 0
    assert tr._depth_capped_size([(0.50, 40)], 0.50, 10.0, tol=0.02, frac=0.5) == 10.0
    assert tr._depth_capped_size([], 0.50, 10.0) == 0.0
    assert tr._depth_capped_size(None, 0.50, 10.0) == 0.0
    print("  [ok] depth sizing: caps at fraction of in-tolerance depth, never raises size")


def test_sweat_trigger():
    tr.SWEAT_MOVE_THRESHOLD = 0.15
    # first alert measures from ENTRY
    assert tr._sweat_trigger(0.41, 0.58, None) == "rising"      # +17¢
    assert tr._sweat_trigger(0.62, 0.40, None) == "falling"     # -22¢
    assert tr._sweat_trigger(0.41, 0.50, None) is None          # +9¢ < threshold
    # after an alert, baseline moves — needs ANOTHER full move (no oscillation spam)
    assert tr._sweat_trigger(0.41, 0.60, 0.58) is None          # only +2¢ since last alert
    assert tr._sweat_trigger(0.41, 0.74, 0.58) == "rising"      # +16¢ since last alert
    assert tr._sweat_trigger(0.41, 0.42, 0.58) == "falling"     # -16¢ since last alert
    # garbage in → no card
    assert tr._sweat_trigger(None, 0.5, None) is None
    assert tr._sweat_trigger(0.5, "x", None) is None
    print("  [ok] sweat trigger: full move from last-alerted price, both directions, no spam")


def test_sweat_card_text():
    s = tr._build_sweat_text("England vs Ghana: O/U 3.5", "Over", 0.41, 0.58, 1.95, "rising", seed=3)
    assert "41¢" in s and "58¢" in s and "📈" in s and "$2.76" in s   # (1.95/0.41)*0.58
    s2 = tr._build_sweat_text("X <tag>", "No", 0.62, 0.40, 2.0, "falling", seed=1)
    assert "📉" in s2 and "&lt;tag&gt;" in s2
    print("  [ok] sweat card: entry→now, live position value, HTML-escaped")


def test_next_resolving():
    import time as _t
    now = _t.time()
    from datetime import datetime, timezone
    def iso(hours):
        return datetime.fromtimestamp(now + hours * 3600, tz=timezone.utc).isoformat()
    ps = [{"market_question": "far", "end_date": iso(30)},
          {"market_question": "soon", "end_date": iso(2)},
          {"market_question": "past", "end_date": iso(-5)},
          {"market_question": "none", "end_date": None}]
    q, h = tr._next_resolving(ps, now)
    assert q == "soon" and 1.9 < h < 2.1
    assert tr._next_resolving([{"end_date": iso(-1)}], now) is None
    assert tr._next_resolving([], now) is None
    print("  [ok] next-resolving: soonest FUTURE end date wins; past/missing ignored")


def test_ladder_stake():
    tr.LADDER_KELLY_MULT = 0.5
    tr.LADDER_MAX_STAKE_USDC = 25.0
    tr.BRAIN_PICK_SIZE_USDC = 2.0
    # eighth-Kelly on a $1000 bankroll: 0.5 × 0.12 × 1000 = $60 → capped at $25
    assert tr._ladder_stake(0.12, 1000.0) == 25.0
    # modest kelly, modest bankroll: 0.5 × 0.08 × 300 = $12
    assert tr._ladder_stake(0.08, 300.0) == 12.0
    # small kelly on today's ~$60 bankroll: 0.5 × 0.10 × 60 = $3
    assert tr._ladder_stake(0.10, 60.0) == 3.0
    # never below the discovery floor; garbage-safe
    assert tr._ladder_stake(0.001, 50.0) == 2.0
    assert tr._ladder_stake(None, 1000.0) == 2.0
    assert tr._ladder_stake(0.25, None) == 2.0
    print("  [ok] ladder stake: kelly x bankroll, floored at discovery, hard $25 cap")


def test_dashboard_shows_ladder_stage():
    d0 = tr._build_dashboard_text(bank=60.0, vault=4.61, open_n=20, cap=40, tier='normal',
                                  today_w=1, today_l=1, today_pnl=0.0, picks_today=1,
                                  sizeups_today=0, ts_utc='12:00', stage1=False)
    assert 'stage 0 (discovery)' in d0
    d1 = tr._build_dashboard_text(bank=1100.0, vault=50.0, open_n=20, cap=40, tier='normal',
                                  today_w=1, today_l=1, today_pnl=0.0, picks_today=1,
                                  sizeups_today=0, ts_utc='12:00', stage1=True)
    assert 'STAGE 1' in d1 and 'conviction × bankroll' in d1
    print("  [ok] dashboard shows the ladder stage (0 vs 1)")


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
    test_event_key_groups_derivative_markets()
    test_event_cap_decision()
    test_depth_capped_size()
    test_sweat_trigger()
    test_sweat_card_text()
    test_next_resolving()
    test_ladder_stake()
    test_dashboard_shows_ladder_stage()
    test_betslip_renders_brain_verdict_on_every_vetted_slip()
    print("all brain-sizeup tests passed")
