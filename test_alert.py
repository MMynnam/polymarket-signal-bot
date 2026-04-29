"""
test_alert.py — End-to-end alert pipeline smoke test.

What this tests:
  1. format_alert() produces valid HTML with no crashes on realistic data.
  2. compute_score() returns a sensible breakdown for known inputs.
  3. TelegramSender.send_message() delivers a real message to your chat.

Run:
  python test_alert.py

Exit 0 = all passed. Exit 1 = at least one failure.
"""

import asyncio
import sys
import time
import logging

import httpx

import config
from alerter import AlertPayload, TelegramSender, format_alert
from scorer import ScoreBreakdown, compute_score
from trade_monitor import Trade
from wallet_profiler import WalletProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("test_alert")

# ---------------------------------------------------------------------------
# Fixtures — realistic "maximum signal" scenario
# ---------------------------------------------------------------------------

def make_trade() -> Trade:
    return Trade(
        trade_id="test-trade-abc123",
        market_id="0xdeadbeef00000000000000000000000000000000000000000000000000000001",
        outcome="YES",
        price=0.18,          # 18% underdog — max underdog score
        size_usd=12_500.0,   # Large bet
        maker_address="0xabc123def456abc123def456abc123def456abc1",
        taker_address="0x1111222233334444555566667777888899990000",
        timestamp=time.time(),
        source="ws",
        raw={},
    )


def make_profile() -> WalletProfile:
    return WalletProfile(
        address="0x1111222233334444555566667777888899990000",
        total_trades=47,
        resolved_trades=22,
        win_count=19,
        loss_count=3,
        win_rate=19 / 22,                  # ~86% — kept as profile metadata
        median_bet_usd=800.0,
        mean_bet_usd=950.0,
        total_volume_usd=44_650.0,
        open_positions=3,
        open_markets=[
            "0xdeadbeef00000000000000000000000000000000000000000000000000000001",
            "0xdeadbeef00000000000000000000000000000000000000000000000000000002",
        ],
        observable_capital_usd=3_200.0,    # 12500 / 3200 ≈ 390% concentration
        wallet_age_days=12.0,              # New wallet — max age score
        first_tx_timestamp=time.time() - 12 * 86400,
        recent_tx_count=18,
        cluster_id="0xfundingsource000000000000000000000000001",
        in_cluster=True,                   # Cluster bonus fires
        last_inbound_transfer_ts=time.time() - 1800,  # 30 min ago → near-max funding velocity
        profile_complete=True,
        missing_components=[],
        fetched_at=time.time(),
    )


def make_breakdown(trade: Trade, profile: WalletProfile) -> ScoreBreakdown:
    """Use the real scorer so we test the actual math, not a hand-crafted dict."""
    # 1.5 hours to resolution → max timing score
    end_date = "2026-04-11T14:00:00Z"
    return compute_score(
        trade_size_usd=trade.size_usd,
        price=trade.price,
        market_end_date=end_date,
        profile=profile,
        current_market_id=trade.market_id,
        trade_timestamp=trade.timestamp,
    )


def make_payload(trade: Trade, profile: WalletProfile, breakdown: ScoreBreakdown) -> AlertPayload:
    return AlertPayload(
        trade=trade,
        profile=profile,
        breakdown=breakdown,
        market_title="Will Trump sign executive order on AI regulation before May 2026?",
        market_end_date="2026-04-11T14:00:00Z",
        hours_to_resolution=1.5,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_format_alert(payload: AlertPayload) -> bool:
    log.info("─── TEST 1: format_alert() ───")
    try:
        text = format_alert(payload)
    except Exception as exc:
        log.error("format_alert() raised: %s", exc, exc_info=True)
        return False

    # Basic sanity checks on the output
    checks = [
        ("INSIDER SIGNAL" in text,           "Missing INSIDER SIGNAL header"),
        ("Score:" in text,                   "Missing Score line"),
        ("Will Trump" in text,               "Missing market title"),
        ("Wallet" in text,                   "Missing Wallet section"),
        ("Score breakdown" in text,          "Missing score breakdown section"),
        ("<b>" in text,                      "No bold tags — HTML formatting broken"),
        ("Polygonscan" in text,              "Missing Polygonscan link"),
        ("Cluster" in text,                  "Missing cluster line in breakdown"),
        ("0x1111222233334444555566667777888899990000" in text, "Missing full wallet address"),
        ("profit" in text,                   "Missing payout calculation"),
    ]

    all_ok = True
    for passed, msg in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_ok = False
        log.info("  [%s] %s", status, msg if not passed else msg.replace("Missing ", "Has "))

    # Print the rendered message so you can eyeball it
    clean = (
        text.replace("<b>", "**").replace("</b>", "**")
            .replace("<i>", "_").replace("</i>", "_")
            .replace("<code>", "`").replace("</code>", "`")
    )
    safe = clean.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8")
    print("\n" + "=" * 60)
    print("RENDERED ALERT PREVIEW:")
    print("=" * 60)
    print(safe)
    print("=" * 60 + "\n")

    return all_ok


def test_score(breakdown: ScoreBreakdown) -> bool:
    log.info("─── TEST 2: compute_score() ───")
    checks = [
        (breakdown.total > 0,                       f"Score is 0 — expected >0, got {breakdown.total}"),
        (breakdown.timing is not None,               "timing component is None"),
        (breakdown.funding_velocity is not None,     "funding_velocity component is None"),
        (breakdown.win_rate is not None,             "win_rate component is None"),
        (breakdown.size_anomaly is not None,         "size_anomaly component is None"),
        (breakdown.wallet_age is not None,           "wallet_age component is None"),
        (breakdown.cluster_bonus == 10,              f"cluster_bonus should be 10, got {breakdown.cluster_bonus}"),
        (breakdown.total >= 60,                      f"Score {breakdown.total} below alert threshold 60"),
    ]

    all_ok = True
    for passed, msg in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_ok = False
        log.info("  [%s] %s", status, msg)

    log.info(
        "  Score breakdown: total=%d | timing=%s | funding=%s | "
        "winrate=%s | size=%s | age=%s | conc=%s | cluster=+%d",
        breakdown.total, breakdown.timing, breakdown.funding_velocity,
        breakdown.win_rate, breakdown.size_anomaly, breakdown.wallet_age,
        breakdown.concentration, breakdown.cluster_bonus,
    )
    return all_ok


async def test_telegram_send(payload: AlertPayload) -> bool:
    log.info("─── TEST 3: TelegramSender.send_message() ───")

    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        log.warning("  [SKIP] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return True

    text = format_alert(payload)
    sender = TelegramSender(
        token=config.TELEGRAM_BOT_TOKEN,
        chat_id=config.TELEGRAM_CHAT_ID,
    )

    async with httpx.AsyncClient() as client:
        ok = await sender.send_message(text, client)

    if ok:
        log.info("  [PASS] Message delivered to Telegram chat %s", config.TELEGRAM_CHAT_ID)
    else:
        log.error("  [FAIL] send_message() returned False — check logs above for HTTP errors")

    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> int:
    log.info("Polymarket Signal Bot — Alert Pipeline Smoke Test")
    log.info("=" * 60)

    trade = make_trade()
    profile = make_profile()
    breakdown = make_breakdown(trade, profile)
    payload = make_payload(trade, profile, breakdown)

    results = [
        test_format_alert(payload),
        test_score(breakdown),
        await test_telegram_send(payload),
    ]

    passed = sum(results)
    total = len(results)
    log.info("=" * 60)
    log.info("Results: %d/%d passed", passed, total)

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
