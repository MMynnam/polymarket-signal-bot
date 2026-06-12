"""
preview_feed_v2.py — renders BEFORE/AFTER previews of every audience-facing message
touched by feed v2, using the REAL builders (no network, no Telegram).

BEFORE = the pre-feed-v2 modules (extracted via `git show 9e88204:` — the last commit
before the overhaul; auto-extracted on first run), with _send_telegram and balance
fetches monkeypatched to capture instead of send.
AFTER  = the working-tree builders (pure functions; no patching needed).

Outputs:
  feed_v2_preview.md   — every message, before/after, in HTML-source form
  preview_card.png     — the daily recap image card rendered from fixture data

Run:  python preview_feed_v2.py
"""

import asyncio
import importlib.util
import io
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "fly-trader"))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_BEFORE_REF = "9e88204"  # last commit before feed v2


def _extract_before(repo_relpath, dest):
    """git show <pre-feed-v2>:<path> → dest (UTF-8, bypasses console encoding)."""
    if not os.path.exists(dest):
        import subprocess
        blob = subprocess.run(["git", "show", f"{_BEFORE_REF}:{repo_relpath}"],
                              capture_output=True, cwd=ROOT, check=True)
        with open(dest, "wb") as f:
            f.write(blob.stdout)
    return dest


# --- modules -----------------------------------------------------------------
tr_new = _load("tr_new", os.path.join(ROOT, "fly-trader", "trader_remote.py"))
tr_old = _load("tr_old", _extract_before("fly-trader/trader_remote.py",
                                         os.path.join(ROOT, "_head_trader_remote.py")))
rc_new = _load("rc_new", os.path.join(ROOT, "results_recap.py"))
rc_old = _load("rc_old", _extract_before("results_recap.py",
                                         os.path.join(ROOT, "_head_results_recap.py")))
import feed_card  # noqa: E402

# --- capture harness for the OLD async notifiers ------------------------------
_captured: list = []


async def _fake_send(client, text, *a, **kw):
    _captured.append(text)


async def _fake_balance():
    return 56.10


tr_old._send_telegram = _fake_send
tr_old._get_usdc_balance = _fake_balance


def old_render(coro) -> str:
    _captured.clear()
    asyncio.run(coro)
    return "\n\n— — (next message) — —\n\n".join(_captured) if _captured else "(nothing sent)"


# --- fixtures -----------------------------------------------------------------
NOW = 1781136000  # 2026-06-11 00:00 UTC — fixed so previews are reproducible

BETS = [
    dict(q="Will the Lakers beat the Celtics tonight?", side="Lakers",
         price=0.38, size=5.00, url="https://polymarket.com/event/lakers-celtics"),
    dict(q="Will Bitcoin close above $150k on Friday?", side="Yes",
         price=0.12, size=3.00, url="https://polymarket.com/event/btc-150k"),
    dict(q="Will Man City win the Premier League? <script>&amp;</script>", side="Man City",
         price=0.85, size=5.00, url="https://polymarket.com/event/mancity-epl"),
]

TRADE_WIN = dict(market_question="Will the Lakers beat the Celtics tonight?",
                 bet_side="Lakers", bet_price_filled=0.55, bet_price_intended=0.55,
                 winning_outcome="Lakers", alert_id="a-001")
TRADE_WIN_51 = dict(market_question="Will the Fed cut rates in June?",
                    bet_side="No", bet_price_filled=0.51, bet_price_intended=0.50,
                    winning_outcome="No", alert_id="a-003")
TRADE_LOSS = dict(market_question="Will Bitcoin close above $150k on Friday?",
                  bet_side="Yes", bet_price_filled=0.12, bet_price_intended=0.15,
                  winning_outcome="No", alert_id="a-002")

RESOLVED_DAY = [
    dict(market_question="Will the Lakers beat the Celtics tonight?", bet_side="Lakers",
         bet_price_filled=0.38, bet_price_intended=0.40, pnl=8.16,
         resolution_status="won", resolved_at=NOW - 3600 * 20),
    dict(market_question="Will it rain in NYC tomorrow?", bet_side="Yes",
         bet_price_filled=0.55, bet_price_intended=0.55, pnl=4.09,
         resolution_status="won", resolved_at=NOW - 3600 * 15),
    dict(market_question="Will Bitcoin close above $150k on Friday?", bet_side="Yes",
         bet_price_filled=0.12, bet_price_intended=0.15, pnl=-3.00,
         resolution_status="lost", resolved_at=NOW - 3600 * 9),
    dict(market_question="Will Man City win the Premier League?", bet_side="Man City",
         bet_price_filled=0.85, bet_price_intended=0.85, pnl=-5.00,
         resolution_status="lost", resolved_at=NOW - 3600 * 4),
    dict(market_question="Will the Fed cut rates in June?", bet_side="No",
         bet_price_filled=0.62, bet_price_intended=0.60, pnl=-2.50,
         resolution_status="lost", resolved_at=NOW - 3600 * 2),
]
RESOLVED_WEEK = RESOLVED_DAY * 3  # ~15 settles over a week
LIFETIME = ("333W–303L", -147.42)
BALANCE = 56.10
# A believable rocky 30-day tape: drifts down, small bounces.
CURVE = []
_cum = 0.0
for i in range(30):
    _cum += [-1.2, 0.8, -2.1, 1.5, -0.7, -1.8, 2.2, -1.1, 0.4, -1.6][i % 10]
    CURVE.append((NOW - (29 - i) * 86400, round(_cum, 2)))

OUT = ["# feed v2 — before/after previews",
       "",
       "BEFORE = real HEAD code (prod today), send/balance monkeypatched to capture.",
       "AFTER = real working-tree builders. HTML source as sent to Telegram.",
       ""]


def section(title, before, after, note=""):
    OUT.append(f"## {title}")
    if note:
        OUT.append(f"_{note}_")
    OUT.append("")
    OUT.append("**BEFORE**")
    OUT.append("```html\n" + (before or "(none)") + "\n```")
    OUT.append("**AFTER**")
    OUT.append("```html\n" + (after or "(none)") + "\n```")
    OUT.append("")


# --- 1) betslips ----------------------------------------------------------------
for i, b in enumerate(BETS, 1):
    before = old_render(tr_old._notify_trade_filled(
        None, b["q"], b["side"], b["price"], b["size"], 78, 0.02, b["url"]))
    after = tr_new._build_slip_text(b["q"], b["side"], b["price"], b["size"], bet_no=640 + i)
    section(f"Betslip {i} — entry {b['price']:.2f}", before, after,
            note="AFTER posts SILENT with an inline '⚡ watch it live' URL button; "
                 "the link line is gone from the body.")

# --- 2) settles -------------------------------------------------------------------
before = old_render(tr_old._notify_trade_resolution(None, TRADE_WIN, "won", 4.09, 1, ""))
def _buzz(loud):
    return "loud=True (buzzes phones)" if loud else "loud=False (silent push)"


after, loud = tr_new._build_settle_text(TRADE_WIN, "won", 4.09, 1, "", threaded=True,
                                        balance=BALANCE + 4.09)
section("Settle — ordinary WIN at 55¢ (threaded under its slip)", before, after,
        note=f"AFTER replies to the betslip message; {_buzz(loud)}.")

before = old_render(tr_old._notify_trade_resolution(None, TRADE_LOSS, "lost", -3.00, -1, ""))
after, loud = tr_new._build_settle_text(TRADE_LOSS, "lost", -3.00, -1, "", threaded=True,
                                        balance=BALANCE - 3.00)
section("Settle — ordinary LOSS (threaded)", before, after, note=f"{_buzz(loud)}.")

after, loud = tr_new._build_settle_text(TRADE_LOSS, "lost", -3.00, -4, "", threaded=False,
                                        balance=BALANCE - 3.00)
section("Settle — 4-loss skid, slip unknown (post-restart)", "(same shape as above)",
        after, note=f"threaded=False keeps it self-contained; {_buzz(loud)}.")

before = old_render(tr_old._notify_trade_resolution(None, TRADE_LOSS, "won", 22.0, 2, ""))
after, loud = tr_new._build_settle_text(TRADE_LOSS, "won", 22.0, 2, "", threaded=True,
                                        balance=BALANCE + 22.0)
section("Settle — LONGSHOT HITS at 12¢ (big moment)", before, after,
        note=f"{_buzz(loud)}.")

after, loud = tr_new._build_settle_text(TRADE_WIN_51, "won", 4.80, 0, "", threaded=True,
                                        balance=BALANCE + 4.80)
section("Settle — BIG WIN at full stake, 51¢", "(n/a — new tier)", after,
        note=f"{_buzz(loud)}.")

# --- 3) timeout (pause) card — the LIVE pause path (magnitude/drawdown CB) --------
losses_fix = [dict(market_question=t["market_question"], bet_side=t["bet_side"],
                   bet_price_intended=t["bet_price_filled"], score=71)
              for t in RESOLVED_DAY if t["resolution_status"] == "lost"]
before = old_render(tr_old._notify_cb_drawdown_pause(
    None, 8.40, 8.41, BALANCE, NOW + 1800, losses_fix))
# AFTER: the exact reason string the live call site builds (trader_remote
# _notify_cb_drawdown_pause); its forensic twin goes to ops, not shown here.
_pct = (8.40 / max(BALANCE, 1.0)) * 100
after = tr_new._build_timeout_text(
    f"down <b>$8.40</b> in {tr_new.TRADING_CB_WINDOW_HOURS:.0f}h "
    f"(that's {_pct:.0f}% of the wallet, chief)",
    NOW + 1800, int(8.40 * 100))
section("Drawdown circuit-breaker pause (the live pause path)", before, after,
        note="AFTER: short audience card (loud); the full forensic block goes to ops.")

# --- 4) daily recap ----------------------------------------------------------------
streak = ("lost", 3)
before = rc_old.format_results_recap(RESOLVED_DAY, 3, streak, BALANCE, None)
after = rc_new.format_results_recap(RESOLVED_DAY, 3, streak, BALANCE, None,
                                    lifetime=LIFETIME)
section("Daily recap", before, after,
        note=f"AFTER rides the image card as its caption (len={len(after)} ≤ 1024); "
             "falls back to plain text if the render fails.")

# --- 5) weekly / monthly -------------------------------------------------------------
before = rc_old.format_weekly_highlights(RESOLVED_WEEK, BALANCE)
after = rc_new.format_weekly_highlights(RESOLVED_WEEK, BALANCE, lifetime=LIFETIME)
section("Weekly recap (Sundays)", before, after)

before = rc_old.format_monthly_highlights(RESOLVED_WEEK, BALANCE, prev_net=-12.40)
after = rc_new.format_monthly_highlights(RESOLVED_WEEK, BALANCE, prev_net=-12.40,
                                         lifetime=LIFETIME)
section("Monthly recap (last Sunday)", before, after)

# --- 6) the image card ----------------------------------------------------------------
png = feed_card.render_recap_card(
    title="the daily", date_str="Thursday, Jun 11",
    wins=2, losses=3, net=2.75, curve=CURVE,
    lifetime_net=LIFETIME[1], balance=BALANCE)
if png:
    with open(os.path.join(ROOT, "preview_card.png"), "wb") as f:
        f.write(png)
    OUT.append(f"## Image card\nRendered OK -> preview_card.png ({len(png):,} bytes, 1200x675).")
else:
    OUT.append("## Image card\nRENDER FAILED — text fallback would be used.")

with open(os.path.join(ROOT, "feed_v2_preview.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(OUT))

print("wrote feed_v2_preview.md")
print("card:", "OK preview_card.png" if png else "FAILED (fallback to text)")
