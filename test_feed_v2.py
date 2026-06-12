"""Tests for feed v2 (2026-06-12 Telegram audience-feed overhaul): pure builders on
both services — betslip/settle/timeout cards (fly trader), recap formats + image card
(Railway), research-channel routing, and the notification-discipline (loud) rules.
Pure-stdlib + Pillow, no live calls, no DB writes."""
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


tr = _load("tr_feedv2", os.path.join(ROOT, "fly-trader", "trader_remote.py"))
import results_recap  # noqa: E402
import feed_card  # noqa: E402
import alerter  # noqa: E402
import config  # noqa: E402

NASTY = 'Will "Bob & Alice" <script>alert(1)</script> win?'


# ---------------------------------------------------------------------------
# fly trader: odds vocabulary
# ---------------------------------------------------------------------------

def test_odds_bands():
    assert tr._odds_key(0.10) == ("big_longshot", "big longshot")
    assert tr._odds_key(0.25)[0] == "underdog"
    assert tr._odds_key(0.50)[0] == "coin_flip"
    assert tr._odds_key(0.70)[0] == "favorite"
    assert tr._odds_key(0.95)[0] == "heavy_chalk"
    # unknowable prices never crash, land in "mystery"
    for bad in (None, 0.0, -0.2, 1.0, 1.5):
        assert tr._odds_key(bad)[0] == "mystery"
    # every band key has a copy pool
    for _cap, key, _word in tr._ODDS_BANDS:
        assert tr._SLIP_BANTER.get(key), f"no banter pool for {key}"
    assert tr._SLIP_BANTER.get("mystery")
    print("  [ok] odds bands + banter pools")


def test_pick_line_deterministic():
    pool = ["a", "b", "c"]
    assert tr._pick_line(pool, 0) == "a"
    assert tr._pick_line(pool, 4) == "b"
    assert tr._pick_line(pool, 4) == "b"  # same seed, same line
    assert tr._pick_line([], 7) == ""     # empty pool never crashes
    print("  [ok] _pick_line deterministic + empty-safe")


# ---------------------------------------------------------------------------
# fly trader: betslip
# ---------------------------------------------------------------------------

def test_slip_text_basic():
    text = tr._build_slip_text("Will the Lakers win?", "Lakers", 0.38, 5.0, bet_no=641)
    assert "BET #641" in text
    assert "38¢" in text and "$5.00" in text
    assert "$8.16" in text                      # (5/0.38 - 5) payout math
    assert "score" not in text.lower()          # the dead signal stays dead
    print("  [ok] betslip: number, odds, stake, payout, no score")


def test_slip_text_escapes_and_degrades():
    text = tr._build_slip_text(NASTY, "<Bob&Alice>", 0.38, 5.0, bet_no=1)
    assert "<script>" not in text and "&lt;script&gt;" in text
    assert "<Bob" not in text and "&lt;Bob&amp;Alice&gt;" in text
    # zero/None price: no crash, no division, no bogus payout
    for bad in (0.0, None):
        t = tr._build_slip_text("Q?", "Yes", bad, 5.0, bet_no=2)
        assert "$0.00" in t or "?" in t
    # missing bet number still renders a slip
    t = tr._build_slip_text("Q?", "Yes", 0.5, 5.0, bet_no=None)
    assert "Yes" in t
    print("  [ok] betslip: HTML-escaped, None/0 price safe")


def test_slip_registry_fifo():
    tr._slip_msgs.clear()
    for i in range(tr._SLIP_MSGS_CAP + 50):
        tr._remember_slip(f"a{i}", i)
    assert len(tr._slip_msgs) == tr._SLIP_MSGS_CAP
    assert "a0" not in tr._slip_msgs            # oldest evicted
    assert tr._slip_msgs[f"a{tr._SLIP_MSGS_CAP + 49}"] == tr._SLIP_MSGS_CAP + 49
    tr._remember_slip(None, 1)                  # junk inputs are no-ops
    tr._remember_slip("x", None)
    assert None not in tr._slip_msgs and "x" not in tr._slip_msgs
    tr._slip_msgs.clear()
    print("  [ok] slip registry: FIFO cap, junk-safe")


# ---------------------------------------------------------------------------
# fly trader: settle card + loud rules
# ---------------------------------------------------------------------------

WIN = dict(market_question="Will the Lakers win?", bet_side="Lakers",
           bet_price_filled=0.55, winning_outcome="Lakers", alert_id="a1")
LOSS = dict(market_question=NASTY, bet_side="Yes",
            bet_price_filled=0.62, winning_outcome="<No&>", alert_id="a2")


def test_settle_ordinary_silent():
    text, loud = tr._build_settle_text(WIN, "won", 4.09, 1, "", threaded=True, balance=60.19)
    assert not loud                              # ordinary win: silent
    assert "+$4.09" in text and "55¢" in text and "x1.8" in text
    assert "bank $60.19" in text
    assert "Will the Lakers win?" not in text    # threaded: slip already shows it
    text, loud = tr._build_settle_text(LOSS, "lost", -3.0, -1, "", threaded=True, balance=53.10)
    assert not loud                              # ordinary loss: silent
    assert "−$3.00" in text
    assert "&lt;No&amp;&gt; took it" in text     # winner named, escaped
    print("  [ok] settle: ordinary win/loss silent, money lines, winner named")


def test_settle_unthreaded_self_contained():
    text, _ = tr._build_settle_text(LOSS, "lost", -3.0, 0, "", threaded=False, balance=None)
    assert "&lt;script&gt;" in text              # market line present + escaped
    assert "bank" not in text                    # no balance -> no bank line
    print("  [ok] settle: unthreaded includes market, balance-less omits bank")


def test_settle_loud_rules():
    # streak >= 3 buzzes
    _, loud = tr._build_settle_text(LOSS, "lost", -3.0, -3, "", threaded=True, balance=None)
    assert loud
    # snap banner buzzes
    _, loud = tr._build_settle_text(WIN, "won", 1.0, 1, "🛑 snap", threaded=True, balance=None)
    assert loud
    # big-moment thresholds (feed v2 retune): >= $4.50 win, <= -$4.75 loss
    _, loud = tr._build_settle_text(WIN, "won", 4.49, 1, "", threaded=True, balance=None)
    assert not loud
    _, loud = tr._build_settle_text(WIN, "won", 4.50, 1, "", threaded=True, balance=None)
    assert loud
    _, loud = tr._build_settle_text(WIN, "lost", -4.74, -1, "", threaded=True, balance=None)
    assert not loud
    _, loud = tr._build_settle_text(WIN, "lost", -4.75, -1, "", threaded=True, balance=None)
    assert loud
    # a VOID never buzzes for a stale running streak (voids-only batch keeps streak)
    _, loud = tr._build_settle_text(WIN, "invalid", 0.0, 4, "", threaded=True, balance=None)
    assert not loud
    print("  [ok] settle: loud only on streak/snap/big-moment; VOIDs stay quiet")


def test_settle_streak_banter_survives():
    # |streak| >= 2 keeps _streak_headline's curated escalation banter — the
    # ordinary-settle rotation must not overwrite it (review finding).
    text2, _ = tr._build_settle_text(WIN, "won", 2.0, 2, "", threaded=True, balance=None)
    text5, _ = tr._build_settle_text(WIN, "won", 2.0, 5, "", threaded=True, balance=None)
    hits = {l.strip("<i></i>") for l in tr._HIT_LINES}
    last2 = text2.splitlines()[-1].replace("<i>", "").replace("</i>", "")
    last5 = text5.splitlines()[-1].replace("<i>", "").replace("</i>", "")
    assert last2 not in hits and last5 not in hits
    assert last2 != last5  # escalation actually escalates
    # streak 0/1 rotates the house lines
    t0, _ = tr._build_settle_text(WIN, "won", 2.0, 1, "", threaded=True, balance=None)
    l0 = t0.splitlines()[-1].replace("<i>", "").replace("</i>", "")
    assert l0 in {h for h in tr._HIT_LINES}
    print("  [ok] settle: curated streak banter survives, house lines on ordinary")


def test_big_moment_tiers():
    assert tr._big_moment("won", 0.55, 4.49) is None
    assert "BIG WIN" in tr._big_moment("won", 0.51, 4.80)[0]
    assert "HUGE WIN" in tr._big_moment("won", 0.51, 9.0)[0]
    assert "LONGSHOT" in tr._big_moment("won", 0.12, 2.0)[0]
    assert "UNICORN" in tr._big_moment("won", 0.09, 2.0)[0]
    assert "UPSET" in tr._big_moment("lost", 0.85, -2.0)[0]
    assert "BRUTAL" in tr._big_moment("lost", 0.55, -5.0)[0]
    assert tr._big_moment("lost", 0.55, -3.0) is None
    assert tr._big_moment("won", None, 99.0) is None
    assert tr._big_moment("invalid", 0.5, 0.0) is None
    # money grammar: sign before the $
    assert "+$4.80" in tr._big_moment("won", 0.51, 4.80)[0]
    assert "−$5.00" in tr._big_moment("lost", 0.55, -5.0)[0]
    print("  [ok] big-moment tiers + money grammar")


def test_loss_lines_escaped():
    # '<' in a market title must not 400 the circuit-breaker ops message
    out = tr._format_loss_lines([dict(market_question="Will Elon post <40 tweets?",
                                      bet_side="<Yes&No>", bet_price_intended=0.6, score=70)])
    assert "<40" not in out and "&lt;40" in out
    assert "<Yes" not in out and "&lt;Yes&amp;No&gt;" in out
    print("  [ok] CB loss lines HTML-escaped")


def test_timeout_card():
    text = tr._build_timeout_text("📉 dropped 6 straight", 1781137800, seed=6)
    assert "BOT IN TIMEOUT" in text
    assert "dropped 6 straight" in text
    assert "back at" in text and "UTC" in text
    assert len(text) < 400                       # short audience card, not the ops wall
    print("  [ok] timeout card")


# ---------------------------------------------------------------------------
# Railway: recap formats
# ---------------------------------------------------------------------------

RES = [
    dict(market_question="Will the Lakers win?", bet_side="Lakers", bet_price_filled=0.38,
         bet_price_intended=0.4, pnl=8.16, resolution_status="won", resolved_at=1781050000),
    dict(market_question=NASTY, bet_side="Yes", bet_price_filled=0.62,
         bet_price_intended=0.6, pnl=-5.0, resolution_status="lost", resolved_at=1781060000),
]


def test_winbar():
    assert results_recap._winbar(0, 0) == "▱" * 10
    assert results_recap._winbar(5, 5) == "▰" * 5 + "▱" * 5
    assert results_recap._winbar(10, 0) == "▰" * 10
    assert results_recap._winbar(0, 10) == "▱" * 10
    print("  [ok] winbar")


def test_money_grammar():
    # One grammar feed-wide: sign first, true minus (matches card + settle cards)
    assert results_recap._money(4.8) == "+$4.80"
    assert results_recap._money(-147.42) == "−$147.42"
    assert results_recap._money(0.0) == "+$0.00"
    print("  [ok] money grammar unified")


def test_lifetime_line_sign_aware():
    # down-bad copy only when actually down; up gets its own pool
    for seed in range(8):
        down = results_recap._lifetime_line(-147.42, seed)
        up = results_recap._lifetime_line(12.5, seed)
        assert "−$147.42" in down
        assert "+$12.50" in up
        assert up.format() not in [l.format(net="x") for l in results_recap._LIFETIME_LINES]
    print("  [ok] lifetime footer is sign-aware")


def test_sweat_line():
    assert "one" in results_recap._sweat_line(1)          # singular grammar
    assert "1 " not in results_recap._sweat_line(1)
    assert "7" in results_recap._sweat_line(7)
    print("  [ok] sweat line: singular handled, count rendered")


def test_daily_recap():
    out = results_recap.format_results_recap(RES, 3, ("lost", 2), 56.10,
                                             milestone="🥇 <b>New record</b>",
                                             lifetime=("333W–303L", -147.42))
    assert "THE DAILY" in out
    assert "1W–1L" in out and "▰" in out
    assert "+$3.16" in out                       # real net, and...
    assert "(notional)" not in out               # ...the hedge is gone
    assert "all-time <b>−$147.42</b>" in out     # honest lifetime line
    assert "New record" in out
    assert "&lt;script&gt;" in out and "<script>" not in out
    assert "score" not in out.lower()
    # empty day with open positions still posts the sweat line
    out2 = results_recap.format_results_recap([], 4, ("none", 0), None, lifetime=(None, None))
    assert out2 and "4" in out2
    # truly nothing -> no post
    assert results_recap.format_results_recap([], 0, ("none", 0), None) is None
    print("  [ok] daily recap: v2 grammar, honest dollars, escaped, None-safe")


def test_daily_recap_fits_photo_caption():
    long_q = "Will " + "a very long market question " * 8 + "happen?"
    res = [dict(market_question=long_q, bet_side="SomeVeryLongTeamName FC", bet_price_filled=0.5,
                bet_price_intended=0.5, pnl=(2.5 if i % 2 else -2.5),
                resolution_status=("won" if i % 2 else "lost"), resolved_at=1781050000 + i)
           for i in range(40)]
    out = results_recap.format_results_recap(res, 9, ("won", 4), 56.10,
                                             milestone="🔥 <b>New record:</b> longest win streak — <b>9 in a row</b>",
                                             lifetime=("333W–303L", -147.42))
    assert len(out) <= 1024, f"caption too long: {len(out)}"
    print(f"  [ok] daily recap fits sendPhoto caption (worst-case {len(out)}/1024)")


def test_weekly_and_monthly():
    wk = results_recap.format_weekly_highlights(RES, 56.10, lifetime=("333W–303L", -147.42))
    assert "THE WEEK" in wk and "(notional)" not in wk and "▰" in wk
    assert "−$147.42" in wk
    mo = results_recap.format_monthly_highlights(RES, 56.10, prev_net=-12.4,
                                                 lifetime=("333W–303L", -147.42))
    assert "THE MONTH" in mo and "(notional)" not in mo and "▰" in mo
    assert "prev 30d −$12.40" in mo
    assert "−$147.42" in mo
    # quiet variants stay graceful
    assert "quiet week" in results_recap.format_weekly_highlights([], None)
    assert "quiet month" in results_recap.format_monthly_highlights([], None)
    print("  [ok] weekly + monthly: v2 grammar, honest lifetime footer")


# ---------------------------------------------------------------------------
# Railway: image card
# ---------------------------------------------------------------------------

def test_card_renders_png():
    curve = [(1781000000 + i * 86400, -0.5 * i) for i in range(30)]
    png = feed_card.render_recap_card(title="the daily", date_str="Thursday, Jun 11",
                                      wins=2, losses=3, net=2.75, curve=curve,
                                      lifetime_net=-147.42, balance=56.10)
    assert png and png[:8] == b"\x89PNG\r\n\x1a\n"
    # degenerate inputs still render (or at worst return None — never raise)
    for curve in ([], [(1781000000, 1.0)], [(None, None)], [(1781000000, 1.0)] * 2):
        feed_card.render_recap_card(title="t", date_str="d", wins=0, losses=0, net=0.0,
                                    curve=curve, lifetime_net=None, balance=None)
    print("  [ok] image card renders PNG; degenerate curves never raise")


def test_card_failure_returns_none():
    real = feed_card._render

    def boom(*a, **k):
        raise RuntimeError("pillow exploded")

    feed_card._render = boom
    try:
        assert feed_card.render_recap_card(title="t", date_str="d", wins=0, losses=0,
                                           net=0.0, curve=[], lifetime_net=None,
                                           balance=None) is None
    finally:
        feed_card._render = real
    print("  [ok] image card: ANY render failure -> None (text fallback)")


# ---------------------------------------------------------------------------
# Routing: research/ops output leaves the audience channel
# ---------------------------------------------------------------------------

def test_research_sender_routing():
    saved = (config.TELEGRAM_OPS_CHAT_ID, config.FEED_RESEARCH_TO_MAIN, config.TELEGRAM_CHAT_ID)
    try:
        config.TELEGRAM_CHAT_ID = "-100MAIN"
        config.TELEGRAM_OPS_CHAT_ID = "-100OPS"
        config.FEED_RESEARCH_TO_MAIN = False
        s = alerter.make_research_sender()
        assert s is not None and s._chat_id == "-100OPS"
        config.TELEGRAM_OPS_CHAT_ID = ""
        config.FEED_RESEARCH_TO_MAIN = True
        s = alerter.make_research_sender()
        assert s is not None and s._chat_id == "-100MAIN"
        config.FEED_RESEARCH_TO_MAIN = False
        assert alerter.make_research_sender() is None
    finally:
        config.TELEGRAM_OPS_CHAT_ID, config.FEED_RESEARCH_TO_MAIN, config.TELEGRAM_CHAT_ID = saved
    print("  [ok] research sender: ops channel > main override > drop")


if __name__ == "__main__":
    test_odds_bands()
    test_pick_line_deterministic()
    test_slip_text_basic()
    test_slip_text_escapes_and_degrades()
    test_slip_registry_fifo()
    test_settle_ordinary_silent()
    test_settle_unthreaded_self_contained()
    test_settle_loud_rules()
    test_settle_streak_banter_survives()
    test_big_moment_tiers()
    test_loss_lines_escaped()
    test_timeout_card()
    test_winbar()
    test_money_grammar()
    test_lifetime_line_sign_aware()
    test_sweat_line()
    test_daily_recap()
    test_daily_recap_fits_photo_caption()
    test_weekly_and_monthly()
    test_card_renders_png()
    test_card_failure_returns_none()
    test_research_sender_routing()
    print("all feed-v2 tests passed")
