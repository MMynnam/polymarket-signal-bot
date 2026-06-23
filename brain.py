"""
brain.py — the bot's "brain": an independent, calibrated LLM forecaster.

WHY THIS EXISTS
    The 2026-06-13 edge audit proved, on clean on-chain cash truth, that the
    insider-copy signal has NO exploitable edge — win rate tracks price, wallet
    skill heterogeneity is exactly zero, every score component is ~AUC 0.5. The
    bot cannot tune its way to profit from that signal. This module is the bot's
    attempt at REAL alpha: instead of copying insiders, it forms its OWN view of
    a market by researching the open web and reasoning about the outcome, then
    compares that view to the market price to find mispricings.

WHAT IT DOES (two sources, one engine)
    • SCANNER  — hunts obscure/thin Polymarket markets (Gamma) for mispricing.
                 The forecasting literature (AIA Forecaster) finds the LLM edge
                 is real on thin/obscure markets and absent on liquid ones, so
                 the scanner deliberately targets the long tail.
    • VETO     — re-judges recent high-score insider alerts: does the brain think
                 the insider's side is more/less likely than the price they paid?
                 (CONFIRM vs VETO.) Reads alert_outcomes, so grading is clean.

THE PIPELINE (per market) — grounded in the AIA Forecaster recipe
    triage (cheap Haiku) → web-search research (Sonnet) → N independent forecasts
    → reconcile if they disagree → Platt/log-odds calibration (×√3, de-hedges the
    LLM) → edge decision → conviction (Kelly-capped) sizing.

    Note on API shape: server-side web search returns citations, and structured
    JSON output is INCOMPATIBLE with citations. So the pipeline is split: the
    research call uses web search (free-form text + citations, NO schema); the
    forecast/triage/reconcile calls use structured JSON output (NO tools). This
    is the right architecture anyway — research first, then forecast from it.

DISCIPLINE (non-negotiable)
    • SHADOW by default: it logs to brain_forecasts and posts to ops, NEVER trades.
    • No API key / BRAIN_ENABLED=false → the whole subsystem no-ops cleanly.
    • Hard daily USD cap; every call's token + web-search cost is tracked.
    • NO backtest — LLM training-cutoff lookahead makes any backtest a lie.
      Validation is forward-only: log forecasts, grade on resolution, compare
      Brier(brain) vs Brier(market). Graduate to real money only if the brain
      demonstrably beats the market over weeks of out-of-sample resolutions.

Pure logic (calibration / edge / Kelly / Brier / spend) has no network deps and
is unit-tested in test_brain.py. Heavy imports (anthropic, httpx, database,
alerter) are loaded lazily so the pure functions import cleanly anywhere.
"""

import asyncio
import html
import json
import logging
import math
import statistics
import time
from datetime import datetime, timedelta

import config

log = logging.getLogger("brain")

# Per-1M-token pricing (USD): (input, output). Cache write ~1.25×, read ~0.1× input.
_PRICE = {
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-4-6": (3.00, 15.00),
    "claude-opus-4-8": (5.00, 25.00),
    "claude-fable-5": (10.00, 50.00),
}
# Server-side web search: ~$10 per 1000 searches.
_WEB_SEARCH_USD = 0.01

# JSON schemas for structured output (no numeric min/max — structured outputs
# don't support those; we clamp client-side).
_TRIAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "worth_forecasting": {"type": "boolean"},
        "researchability": {"type": "integer"},
        "rationale": {"type": "string"},
    },
    "required": ["worth_forecasting", "researchability", "rationale"],
    "additionalProperties": False,
}
_FORECAST_SCHEMA = {
    "type": "object",
    "properties": {
        "probability": {"type": "number"},
        "confidence": {"type": "number"},
        "key_factors": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["probability", "confidence", "key_factors"],
    "additionalProperties": False,
}
_RECONCILE_SCHEMA = {
    "type": "object",
    "properties": {
        "probability": {"type": "number"},
        "confidence": {"type": "number"},
        "reasoning": {"type": "string"},
    },
    "required": ["probability", "confidence", "reasoning"],
    "additionalProperties": False,
}


# ===========================================================================
# Pure logic — no network, unit-tested in test_brain.py
# ===========================================================================

def _clamp(x, lo=0.0, hi=1.0):
    try:
        x = float(x)
    except (TypeError, ValueError):
        return lo
    return max(lo, min(hi, x))


def platt_calibrate(p: float, coef: float = None) -> float:
    """Extremize a probability in log-odds space to counteract LLM hedging toward
    0.5. coef>1 pushes away from 0.5 toward 0/1; coef=1 is identity. The √3≈1.73
    default is the AIA Forecaster prior — re-fit from resolved forecasts later."""
    if coef is None:
        coef = config.BRAIN_PLATT_COEF
    p = _clamp(p, 1e-6, 1 - 1e-6)
    logit = math.log(p / (1 - p))
    z = coef * logit
    return 1.0 / (1.0 + math.exp(-z))


def edge(brain_prob: float, market_price: float) -> float:
    """Signed edge on the target outcome: positive ⇒ the brain thinks the outcome
    is more likely than the price implies (underpriced / confirm)."""
    return float(brain_prob) - float(market_price)


def kelly_fraction(brain_prob: float, market_price: float, cap: float = None) -> float:
    """Quarter-Kelly-style stake fraction for buying the EV-positive side of a
    binary at `market_price` (cost per $1 share) given true prob `brain_prob`.
    Returns 0 when no edge. Sizing only — shadow mode never uses it, but it's the
    lever for graduation."""
    if cap is None:
        cap = config.BRAIN_KELLY_CAP
    p = _clamp(market_price, 1e-6, 1 - 1e-6)
    q = _clamp(brain_prob)
    if q > p:                       # buy the target outcome at price p
        f = (q - p) / (1.0 - p)     # Kelly for a binary paying 1 at cost p
    elif q < p:                     # buy the complement at price (1-p)
        f = (p - q) / p
    else:
        f = 0.0
    return max(0.0, min(cap, f))


def decide(brain_prob, market_price, source, edge_threshold=None, min_confidence=None,
           confidence=1.0):
    """Turn a calibrated probability into a verdict + act flag. `act` means it
    cleared both the edge and confidence bars (in live mode this would gate a
    trade; in shadow it only controls what gets flagged to ops)."""
    if edge_threshold is None:
        edge_threshold = config.BRAIN_EDGE_THRESHOLD
    if min_confidence is None:
        min_confidence = config.BRAIN_MIN_CONFIDENCE
    e = edge(brain_prob, market_price)
    strong = abs(e) >= edge_threshold and confidence >= min_confidence
    if source == "veto":
        verdict = "CONFIRM" if e >= edge_threshold else ("VETO" if e <= -edge_threshold else "NEUTRAL")
    else:  # scanner
        verdict = "UNDERPRICED" if e >= edge_threshold else ("OVERPRICED" if e <= -edge_threshold else "FAIR")
    return {"edge": e, "verdict": verdict, "act": bool(strong)}


def brier(prob: float, outcome: int) -> float:
    """Brier score for a single forecast; outcome is 1 (target won) or 0 (lost)."""
    return (float(prob) - float(outcome)) ** 2


def aggregate_brier(rows):
    """rows: iterable of (brain_prob, market_price, outcome). Returns the mean
    Brier for the brain and the market plus n — the head-to-head calibration
    comparison. brain < market ⇒ the brain is better calibrated than the price."""
    bb = []
    bm = []
    for bp, mp, o in rows:
        if o is None:
            continue
        bb.append(brier(bp, o))
        bm.append(brier(mp, o))
    n = len(bb)
    if n == 0:
        return {"n": 0, "brain": None, "market": None}
    return {"n": n, "brain": sum(bb) / n, "market": sum(bm) / n}


# ===========================================================================
# Spend tracking — hard daily USD cap, UTC-midnight rollover
# ===========================================================================

class SpendTracker:
    """Accumulates token + web-search cost per UTC day and enforces a hard cap.
    Cheap to call; pure (no network) so it's unit-tested. Resets at UTC midnight.
    The in-process counter resets on restart, which is fine — the cap is a safety
    rail against runaway spend within a day, not an accounting ledger."""

    def __init__(self, daily_cap_usd: float):
        self.daily_cap = float(daily_cap_usd)
        self._day = None
        self._spent = 0.0
        self.web_searches = 0

    def _today(self):
        return datetime.utcnow().strftime("%Y-%m-%d")

    def _roll(self):
        d = self._today()
        if d != self._day:
            self._day = d
            self._spent = 0.0
            self.web_searches = 0

    def spent_today(self) -> float:
        self._roll()
        return self._spent

    def remaining(self) -> float:
        self._roll()
        return max(0.0, self.daily_cap - self._spent)

    def can_afford(self, estimate: float = 0.0) -> bool:
        return self.remaining() > max(0.0, estimate)

    def usage_cost(self, model: str, usage) -> float:
        pin, pout = _PRICE.get(model, (5.0, 25.0))
        it = getattr(usage, "input_tokens", 0) or 0
        ot = getattr(usage, "output_tokens", 0) or 0
        cc = getattr(usage, "cache_creation_input_tokens", 0) or 0
        cr = getattr(usage, "cache_read_input_tokens", 0) or 0
        return (it * pin + ot * pout + cc * pin * 1.25 + cr * pin * 0.10) / 1e6

    def record_usage(self, model: str, usage) -> float:
        self._roll()
        cost = self.usage_cost(model, usage)
        self._spent += cost
        return cost

    def record_web_searches(self, n: int) -> float:
        self._roll()
        cost = max(0, int(n)) * _WEB_SEARCH_USD
        self._spent += cost
        self.web_searches += max(0, int(n))
        return cost


# Module-level tracker (created on first use against the configured cap).
_spend = SpendTracker(config.BRAIN_DAILY_USD_CAP)
_client = None  # lazily-created AsyncAnthropic


def _enabled() -> bool:
    """The brain runs only with an explicit opt-in AND an API key AND the SDK."""
    if not config.BRAIN_ENABLED or not config.BRAIN_API_KEY:
        return False
    try:
        import anthropic  # noqa: F401
    except ImportError:
        log.warning("[Brain] anthropic SDK not installed — brain disabled")
        return False
    return True


def _get_client():
    global _client
    if _client is None:
        import anthropic
        _client = anthropic.AsyncAnthropic(api_key=config.BRAIN_API_KEY)
    return _client


def _first_text(resp) -> str:
    return "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")


def _count_web_searches(resp) -> int:
    n = 0
    for b in resp.content:
        if getattr(b, "type", None) == "server_tool_use" and getattr(b, "name", None) == "web_search":
            n += 1
    return n


# ===========================================================================
# Claude pipeline stages
# ===========================================================================

async def _triage(market: dict):
    """Cheap Haiku gate: is this market worth spending research budget on?
    Returns the parsed dict or None on error/refusal."""
    client = _get_client()
    sys = (
        "You are a triage filter for a prediction-market forecasting bot with a strict "
        "research budget. You decide whether a market is WORTH researching. Say yes only "
        "when public information could plausibly give a forecaster an edge over the market "
        "price: the question is researchable from news/data/base-rates, it is not a pure "
        "coin flip with nothing findable, and it is not effectively already settled. Be "
        "selective — most markets are efficiently priced and not worth the spend."
    )
    user = (
        f"Question: {market['question']}\n"
        f"Resolution detail: {market.get('description', '(none)')}\n"
        f"Current price (implied prob of '{market['target_label']}'): {market['market_price']:.2f}\n"
        f"Category: {market.get('category', 'unknown')}\n"
        f"Closes in: {market.get('hours_to_close', 0):.0f}h\n\n"
        "Worth researching?"
    )
    try:
        resp = await client.messages.create(
            model=config.BRAIN_TRIAGE_MODEL,
            max_tokens=400,
            system=sys,
            messages=[{"role": "user", "content": user}],
            output_config={"format": {"type": "json_schema", "schema": _TRIAGE_SCHEMA}},
        )
    except Exception as exc:
        log.warning("[Brain] triage call failed: %s", exc)
        return None
    _spend.record_usage(config.BRAIN_TRIAGE_MODEL, resp.usage)
    if resp.stop_reason == "refusal":
        return None
    try:
        return json.loads(_first_text(resp))
    except Exception:
        return None


async def _research(market: dict):
    """Sonnet + server-side web search → free-form evidence brief (with citations).
    Handles the server-tool pause_turn loop. Returns (brief, n_web_searches) or
    (None, 0). NO structured output here — web search citations forbid it."""
    client = _get_client()
    sys = (
        "You are a forecasting research analyst. Given a prediction-market question, "
        "search the web for the most relevant CURRENT evidence: recent news, base rates, "
        "scheduled events, expert views, and anything that bears on the outcome. Then write "
        "a concise evidence brief (≈200 words): the key factors for and against the target "
        "outcome, the most decisive facts, and your qualitative sense of likelihood. Do NOT "
        "simply restate the market price — reason from the evidence. Note your uncertainty."
    )
    user = (
        f"Question: {market['question']}\n"
        f"Resolution detail: {market.get('description', '(none)')}\n"
        f"Target outcome to assess: '{market['target_label']}'\n"
        f"Closes in: {market.get('hours_to_close', 0):.0f}h\n\n"
        "Research this and write the evidence brief."
    )
    messages = [{"role": "user", "content": user}]
    tools = [{"type": "web_search_20260209", "name": "web_search"}]
    n_search = 0
    resp = None
    try:
        for _ in range(5):  # cap server-tool continuations
            resp = await client.messages.create(
                model=config.BRAIN_FORECAST_MODEL,
                max_tokens=3000,
                system=sys,
                messages=messages,
                tools=tools,
                output_config={"effort": config.BRAIN_EFFORT},
            )
            _spend.record_usage(config.BRAIN_FORECAST_MODEL, resp.usage)
            n_search += _count_web_searches(resp)
            if resp.stop_reason == "pause_turn":
                messages.append({"role": "assistant", "content": resp.content})
                continue
            break
    except Exception as exc:
        log.warning("[Brain] research call failed: %s", exc)
        return None, n_search
    _spend.record_web_searches(n_search)
    if resp is None or resp.stop_reason == "refusal":
        return None, n_search
    brief = _first_text(resp).strip()
    return (brief or None), n_search


async def _forecast_once(market: dict, brief: str, run_idx: int):
    """One structured forecast run (Sonnet, no tools). Returns a dict or None."""
    client = _get_client()
    sys = (
        "You are a calibrated superforecaster. Output an honest probability that the target "
        "outcome occurs, grounded in the research brief and base rates. Be decisive: if the "
        "evidence points one way, do not hedge toward 50%. Report genuine confidence (how much "
        "the evidence constrains the answer), not false certainty. Probability and confidence "
        "are both in [0,1]."
    )
    user = (
        f"Market: {market['question']}\n"
        f"The probability you output is P('{market['target_label']}' occurs).\n"
        f"Current market price for this outcome: {market['market_price']:.2f}\n\n"
        f"Research brief:\n{brief}\n\n"
        f"Analytical pass #{run_idx + 1}: weigh the evidence from your own angle and give your probability."
    )
    try:
        resp = await client.messages.create(
            model=config.BRAIN_FORECAST_MODEL,
            max_tokens=700,
            system=sys,
            messages=[{"role": "user", "content": user}],
            output_config={"effort": config.BRAIN_EFFORT,
                           "format": {"type": "json_schema", "schema": _FORECAST_SCHEMA}},
        )
    except Exception as exc:
        log.warning("[Brain] forecast call failed: %s", exc)
        return None
    _spend.record_usage(config.BRAIN_FORECAST_MODEL, resp.usage)
    if resp.stop_reason == "refusal":
        return None
    try:
        d = json.loads(_first_text(resp))
    except Exception:
        return None
    return {
        "probability": _clamp(d.get("probability")),
        "confidence": _clamp(d.get("confidence")),
        "factors": d.get("key_factors", [])[:4],
    }


async def _reconcile(market: dict, brief: str, forecasts: list):
    """Supervisor call: only spent when the ensemble disagrees a lot. Sees all
    runs and produces a single reconciled probability. Returns dict or None."""
    client = _get_client()
    runs = "\n".join(
        f"  run {i+1}: p={f['probability']:.2f} conf={f['confidence']:.2f} "
        f"factors={'; '.join(f['factors'][:2])}"
        for i, f in enumerate(forecasts)
    )
    sys = (
        "You are a forecasting supervisor reconciling disagreeing independent forecasts into "
        "one calibrated probability. Identify why they differ, weigh the better-reasoned runs, "
        "and output a single final probability and confidence in [0,1]."
    )
    user = (
        f"Market: {market['question']}\n"
        f"Target: P('{market['target_label']}')\n\n"
        f"Research brief:\n{brief}\n\n"
        f"Independent runs:\n{runs}\n\n"
        "Reconcile into one final probability."
    )
    try:
        resp = await client.messages.create(
            model=config.BRAIN_FORECAST_MODEL,
            max_tokens=700,
            system=sys,
            messages=[{"role": "user", "content": user}],
            output_config={"effort": config.BRAIN_EFFORT,
                           "format": {"type": "json_schema", "schema": _RECONCILE_SCHEMA}},
        )
    except Exception as exc:
        log.warning("[Brain] reconcile call failed: %s", exc)
        return None
    _spend.record_usage(config.BRAIN_FORECAST_MODEL, resp.usage)
    if resp.stop_reason == "refusal":
        return None
    try:
        d = json.loads(_first_text(resp))
    except Exception:
        return None
    return {"probability": _clamp(d.get("probability")), "confidence": _clamp(d.get("confidence"))}


# ===========================================================================
# Orchestration — forecast one market end-to-end
# ===========================================================================

def _recently_forecast(market_id: str) -> bool:
    """True if we already logged a forecast for this market within the
    re-forecast window — keeps cost down and avoids spamming duplicates."""
    import database
    cutoff = int(time.time()) - int(config.BRAIN_REFORECAST_HOURS * 3600)
    row = database.get_db().execute(
        "SELECT 1 FROM brain_forecasts WHERE market_id = ? AND created_at >= ? LIMIT 1",
        (market_id, cutoff),
    ).fetchone()
    return row is not None


async def forecast_market(market: dict, sender, http_client, source: str):
    """Run the full pipeline on one market and log a brain_forecasts row.
    `market` requires: market_id, question, target_label, market_price; optional:
    description, category, hours_to_close, alert_id. Returns the row dict or None."""
    import database

    if _recently_forecast(market["market_id"]):
        return None
    if not _spend.can_afford(config.BRAIN_EST_FORECAST_USD):
        log.info("[Brain] daily budget exhausted ($%.2f spent) — skipping", _spend.spent_today())
        return None

    cost_before = _spend.spent_today()

    triage = await _triage(market)
    if not triage or not triage.get("worth_forecasting"):
        log.info("[Brain] triage declined %.40s (%s)", market["question"],
                 (triage or {}).get("rationale", "n/a")[:60])
        return None

    if not _spend.can_afford(config.BRAIN_EST_FORECAST_USD * 0.6):
        return None
    brief, _ = await _research(market)
    if not brief:
        log.info("[Brain] research produced no brief for %.40s", market["question"])
        return None

    forecasts = []
    for i in range(max(1, config.BRAIN_ENSEMBLE_N)):
        if not _spend.can_afford(0.0):
            break
        f = await _forecast_once(market, brief, i)
        if f:
            forecasts.append(f)
    if not forecasts:
        return None

    probs = [f["probability"] for f in forecasts]
    raw = statistics.fmean(probs)
    stdev = statistics.pstdev(probs) if len(probs) > 1 else 0.0
    mean_conf = statistics.fmean(f["confidence"] for f in forecasts)

    # Reconcile only when the runs disagree a lot (cost-efficient supervisor step).
    if stdev > config.BRAIN_RECONCILE_STD and len(forecasts) > 1 and _spend.can_afford(0.0):
        rec = await _reconcile(market, brief, forecasts)
        if rec:
            raw = rec["probability"]
            mean_conf = rec["confidence"]

    # Dispersion haircut: disagreement lowers confidence.
    confidence = _clamp(mean_conf * (1.0 - min(0.5, 2.0 * stdev)))
    calibrated = platt_calibrate(raw)
    price = market["market_price"]
    d = decide(calibrated, price, source, confidence=confidence)
    kelly = kelly_fraction(calibrated, price)
    cost = _spend.spent_today() - cost_before

    row = {
        "created_at": int(time.time()),
        "source": source,
        "market_id": market["market_id"],
        "question": market["question"],
        "target_label": market["target_label"],
        "market_price": float(price),
        "brain_prob_raw": float(raw),
        "brain_prob": float(calibrated),
        "confidence": float(confidence),
        "edge": float(d["edge"]),
        "verdict": d["verdict"],
        "act": 1 if d["act"] else 0,
        "kelly_fraction": float(kelly),
        "ensemble_n": len(forecasts),
        "prob_stdev": float(stdev),
        "evidence": brief[:1200],
        "models_json": json.dumps({"triage": config.BRAIN_TRIAGE_MODEL,
                                   "forecast": config.BRAIN_FORECAST_MODEL}),
        "cost_usd": float(cost),
        "alert_id": market.get("alert_id"),
    }
    _insert_forecast(database, row)
    log.info("[Brain] %s | %.45s | P=%.2f vs px=%.2f edge=%+.2f %s%s | $%.3f",
             source, market["question"], calibrated, price, d["edge"], d["verdict"],
             " ACT" if d["act"] else "", cost)

    # Shadow: only flag strong, high-conviction calls to ops (never trade).
    if d["act"] and sender is not None:
        try:
            await sender.send_message(_format_call(row), http_client)
        except Exception as exc:
            log.warning("[Brain] ops post failed: %s", exc)
    return row


def _insert_forecast(database, row: dict) -> None:
    database.get_db().execute(
        "INSERT INTO brain_forecasts (created_at, source, market_id, question, target_label, "
        "market_price, brain_prob_raw, brain_prob, confidence, edge, verdict, act, kelly_fraction, "
        "ensemble_n, prob_stdev, evidence, models_json, cost_usd, alert_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (row["created_at"], row["source"], row["market_id"], row["question"], row["target_label"],
         row["market_price"], row["brain_prob_raw"], row["brain_prob"], row["confidence"], row["edge"],
         row["verdict"], row["act"], row["kelly_fraction"], row["ensemble_n"], row["prob_stdev"],
         row["evidence"], row["models_json"], row["cost_usd"], row["alert_id"]),
    )
    database.get_db().commit()


def _format_call(row: dict) -> str:
    q = html.escape(row["question"][:140])
    tgt = html.escape(row["target_label"][:40])
    src = "🔎 SCANNER" if row["source"] == "scanner" else "⚖️ VETO"
    return (
        f"🧠 <b>BRAIN — {src}</b> <i>(shadow · paper only)</i>\n\n"
        f"<b>{q}</b>\n"
        f"outcome: <b>{tgt}</b>\n"
        f"brain <b>{row['brain_prob']*100:.0f}%</b> vs market <b>{row['market_price']*100:.0f}%</b> "
        f"→ edge <b>{row['edge']*100:+.0f}pp</b> · <b>{row['verdict']}</b>\n"
        f"conf {row['confidence']*100:.0f}% · {row['ensemble_n']} runs · "
        f"<i>{html.escape(row['evidence'][:240])}…</i>\n"
        f"<i>no position taken — logged for calibration.</i>"
    )


# ===========================================================================
# Market sources
# ===========================================================================

async def _veto_candidates() -> list:
    """Recent high-score insider alerts not yet brain-judged → veto/confirm source.
    Forecasts P(the insider's side wins); grading is clean via alert_outcomes."""
    import database
    cutoff = int(time.time()) - int(config.BRAIN_VETO_LOOKBACK_HOURS * 3600)
    rows = database.get_db().execute(
        "SELECT ao.alert_id, ao.market_id, ao.market_question, ao.bet_side, "
        "       ao.bet_price_at_alert, ao.hours_to_close_at_alert, ao.market_category "
        "FROM alert_outcomes ao "
        "WHERE ao.created_at >= ? AND ao.score >= ? AND ao.resolution_status = 'pending' "
        "  AND NOT EXISTS (SELECT 1 FROM brain_forecasts bf WHERE bf.alert_id = ao.alert_id) "
        "ORDER BY ao.score DESC LIMIT ?",
        (cutoff, config.BRAIN_VETO_MIN_SCORE, config.BRAIN_MAX_PER_CYCLE),
    ).fetchall()
    out = []
    for r in rows:
        px = r["bet_price_at_alert"]
        if px is None or not (0.0 < px < 1.0):
            continue
        out.append({
            "market_id": r["market_id"],
            "question": r["market_question"],
            "target_label": r["bet_side"],
            "market_price": float(px),
            "description": "(insider alert — assessing whether this side is mispriced)",
            "category": r["market_category"] or "unknown",
            "hours_to_close": r["hours_to_close_at_alert"] or 0.0,
            "alert_id": r["alert_id"],
        })
    return out


def _parse_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


async def _scanner_candidates(http_client) -> list:
    """Fetch active Gamma markets and filter to the thin/obscure long tail where
    the LLM forecasting edge actually lives. Returns binary markets only."""
    url = f"{config.GAMMA_API_BASE}/events"
    params = {"active": "true", "closed": "false", "order": "volume24hr",
              "ascending": "false", "limit": config.BRAIN_SCAN_GAMMA_LIMIT, "offset": 0}
    try:
        resp = await http_client.get(url, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("[Brain] scanner Gamma fetch failed: %s", exc)
        return []
    if isinstance(data, dict):
        data = data.get("data") or data.get("events") or []
    now = datetime.utcnow()
    out = []
    for event in data if isinstance(data, list) else []:
        for m in event.get("markets") or []:
            cand = _scanner_market(m, now)
            if cand:
                out.append(cand)
    # Cheapest-first by volume so the long tail (thin/obscure) gets the budget.
    out.sort(key=lambda c: c["_volume"])
    return out[: config.BRAIN_MAX_PER_CYCLE]


def _scanner_market(m: dict, now: datetime):
    """Parse + filter one Gamma market dict into a scanner candidate or None."""
    cond = m.get("conditionId") or m.get("condition_id")
    question = m.get("question") or m.get("title")
    if not cond or not question:
        return None
    if m.get("closed") or m.get("active") is False:
        return None

    def _jl(v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return None
        return v

    outcomes = _jl(m.get("outcomes"))
    prices = _jl(m.get("outcomePrices"))
    if not (isinstance(outcomes, list) and isinstance(prices, list)) or len(outcomes) != 2 or len(prices) != 2:
        return None  # binary markets only
    p0 = _parse_float(prices[0])
    if p0 is None or not (config.BRAIN_SCAN_MIN_PRICE <= p0 <= config.BRAIN_SCAN_MAX_PRICE):
        return None

    vol = _parse_float(m.get("volumeNum")) or _parse_float(m.get("volume")) or 0.0
    if not (config.BRAIN_SCAN_MIN_VOL_USD <= vol <= config.BRAIN_SCAN_MAX_VOL_USD):
        return None

    end_raw = m.get("endDate") or m.get("end_date")
    hours = 0.0
    if end_raw:
        try:
            end = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00")).replace(tzinfo=None)
            hours = (end - now).total_seconds() / 3600.0
        except Exception:
            hours = 0.0
        days = hours / 24.0
        if not (config.BRAIN_SCAN_MIN_DAYS <= days <= config.BRAIN_SCAN_MAX_DAYS):
            return None

    return {
        "market_id": cond,
        "question": question,
        "target_label": str(outcomes[0]),
        "market_price": p0,
        "description": m.get("description", "")[:600] or "(none)",
        "category": m.get("category") or "unknown",
        "hours_to_close": hours,
        "_volume": vol,
    }


# ===========================================================================
# Calibration grading + report
# ===========================================================================

def grade_resolved_forecasts() -> int:
    """Grade ungraded VETO forecasts against resolved insider alerts (clean side
    join via alert_outcomes). Scanner-row grading via Gamma resolution is a future
    step (marked below). Returns number newly graded."""
    import database
    db = database.get_db()
    rows = db.execute(
        "SELECT bf.id, bf.brain_prob, bf.market_price, ao.resolution_status "
        "FROM brain_forecasts bf JOIN alert_outcomes ao ON bf.alert_id = ao.alert_id "
        "WHERE bf.source = 'veto' AND bf.resolved_outcome IS NULL "
        "  AND ao.resolution_status IN ('resolved_won', 'resolved_lost')"
    ).fetchall()
    n = 0
    for r in rows:
        outcome = 1 if r["resolution_status"] == "resolved_won" else 0  # target = insider's side
        db.execute(
            "UPDATE brain_forecasts SET resolved_outcome = ?, resolved_at = ?, "
            "brier_brain = ?, brier_market = ? WHERE id = ?",
            (outcome, int(time.time()), brier(r["brain_prob"], outcome),
             brier(r["market_price"], outcome), r["id"]),
        )
        n += 1
    if n:
        db.commit()
    # NOTE: scanner forecasts await a Gamma-resolution grader (follow-up) — until
    # then only veto rows contribute to the Brier comparison.
    return n


def calibration_report() -> str:
    """Forward-only calibration: Brier(brain) vs Brier(market) over graded rows.
    This is the graduation gate — the brain earns real money only if it beats the
    market here over weeks of out-of-sample resolutions."""
    import database
    db = database.get_db()
    graded = db.execute(
        "SELECT brain_prob, market_price, resolved_outcome FROM brain_forecasts "
        "WHERE resolved_outcome IS NOT NULL"
    ).fetchall()
    agg = aggregate_brier((g["brain_prob"], g["market_price"], g["resolved_outcome"]) for g in graded)
    total = db.execute("SELECT COUNT(*) FROM brain_forecasts").fetchone()[0]
    pending = total - agg["n"]
    head = "🧠 <b>BRAIN CALIBRATION</b> <i>(shadow — paper only, no money at risk)</i>"
    if agg["n"] < 20:
        return (f"{head}\n\n📋 {total} forecasts logged · {agg['n']} graded · {pending} awaiting resolution.\n"
                f"<i>Need ≥20 graded for an honest Brier read. Logging, not yet judging.</i>")
    delta = agg["market"] - agg["brain"]   # positive ⇒ brain better than market
    beating = "✅ beating the market" if delta > 0 else "❌ not beating the market"
    return (
        f"{head}\n\n"
        f"📊 <b>{agg['n']}</b> graded forecasts\n"
        f"Brier — brain <b>{agg['brain']:.4f}</b> vs market <b>{agg['market']:.4f}</b> "
        f"(Δ {delta:+.4f})\n"
        f"<b>{beating}</b> · lower Brier = better calibrated.\n"
        f"<i>{pending} still pending. Graduation needs a durable brain&lt;market over weeks — not yet.</i>"
    )


# ===========================================================================
# The loop — shadow, ops-routed, cost-capped
# ===========================================================================

async def brain_loop(dry_run: bool = False) -> None:
    """Single supervised loop. Each cycle: grade resolved forecasts, run the veto
    + scanner sources through the forecast engine (bounded by the per-cycle cap and
    the hard daily spend cap), and post a daily calibration report to ops. No-ops
    cleanly when the brain is disabled (no key / BRAIN_ENABLED=false)."""
    import httpx
    from alerter import make_research_sender

    if not _enabled():
        log.info("[Brain] disabled (BRAIN_ENABLED=%s, api_key=%s) — loop idle.",
                 config.BRAIN_ENABLED, "set" if config.BRAIN_API_KEY else "MISSING")
        # Stay alive but inert so the supervisor doesn't thrash restarting us.
        while True:
            await asyncio.sleep(3600)

    log.info("[Brain] STARTED — shadow=%s cap=$%.2f/day models=%s/%s effort=%s ensemble=%d",
             config.BRAIN_SHADOW, config.BRAIN_DAILY_USD_CAP, config.BRAIN_TRIAGE_MODEL,
             config.BRAIN_FORECAST_MODEL, config.BRAIN_EFFORT, config.BRAIN_ENSEMBLE_N)
    sender = make_research_sender()
    last_report_day = None

    async with httpx.AsyncClient() as http_client:
        while True:
            cycle_start = time.monotonic()
            try:
                graded = grade_resolved_forecasts()
                if graded:
                    log.info("[Brain] graded %d newly-resolved forecasts", graded)

                # Veto/confirm layer first (cheap, clean grading), then the scanner.
                done = 0
                for cand in await _veto_candidates():
                    if done >= config.BRAIN_MAX_PER_CYCLE or not _spend.can_afford(config.BRAIN_EST_FORECAST_USD):
                        break
                    if await forecast_market(cand, None if dry_run else sender, http_client, "veto"):
                        done += 1
                for cand in await _scanner_candidates(http_client):
                    if done >= config.BRAIN_MAX_PER_CYCLE or not _spend.can_afford(config.BRAIN_EST_FORECAST_USD):
                        break
                    if await forecast_market(cand, None if dry_run else sender, http_client, "scanner"):
                        done += 1

                # Daily calibration report to ops.
                now = datetime.utcnow()
                if now.hour == config.BRAIN_CAL_REPORT_HOUR_UTC and now.strftime("%Y-%m-%d") != last_report_day:
                    last_report_day = now.strftime("%Y-%m-%d")
                    text = calibration_report()
                    if dry_run or sender is None:
                        log.info("[Brain] calibration report:\n%s", text)
                    else:
                        await sender.send_message(text, http_client)

                log.info("[Brain] cycle done — %d forecast(s), $%.3f/$%.2f spent today",
                         done, _spend.spent_today(), config.BRAIN_DAILY_USD_CAP)
            except Exception as exc:
                log.exception("[Brain] cycle error: %s", exc)

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(60.0, config.BRAIN_SCAN_INTERVAL_SECONDS - elapsed))
