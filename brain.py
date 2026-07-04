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
        # The brain's voice: a punchy one/two-line rationale with personality —
        # the line a friend would screenshot. Surfaced in the Telegram ops post.
        "take": {"type": "string"},
    },
    "required": ["probability", "confidence", "key_factors", "take"],
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

    def record_usage(self, model: str, usage, batch: bool = False) -> float:
        self._roll()
        cost = self.usage_cost(model, usage)
        if batch:
            cost *= 0.5  # Batches API: 50% discount on all token usage
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

# Billing circuit-breaker: when the Anthropic account runs out of credits, every call 400s.
# Without this the brain hammers failed requests every cycle (observed 2026-07-04) and would
# instantly re-burn fresh credits on low-value triage. On a billing error we pause ALL brain
# API activity for BRAIN_BILLING_COOLDOWN_S, then probe again.
_billing_pause_until: float = 0.0


def _billing_paused() -> bool:
    return time.time() < _billing_pause_until


def _note_billing_error(exc) -> bool:
    """If exc is the out-of-credits billing error, arm the cooldown (log once). Returns True
    when it was a billing error so callers can bail without retrying."""
    global _billing_pause_until
    msg = str(exc).lower()
    if "credit balance is too low" not in msg and "billing" not in msg:
        return False
    if not _billing_paused():
        log.warning("[Brain] API credits EXHAUSTED — pausing all brain calls for %dh "
                    "(top up Anthropic credits to resume)", config.BRAIN_BILLING_COOLDOWN_S // 3600)
    _billing_pause_until = time.time() + config.BRAIN_BILLING_COOLDOWN_S
    return True


def _output_config(schema: dict = None, model: str = None) -> dict:
    """output_config for a call: structured format always (when given); effort only on models
    that support it (Haiku 4.5 rejects the effort param)."""
    cfg = {}
    if schema is not None:
        cfg["format"] = {"type": "json_schema", "schema": schema}
    if not (model or config.BRAIN_FORECAST_MODEL).startswith("claude-haiku"):
        cfg["effort"] = config.BRAIN_EFFORT
    return cfg


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

def _triage_params(market: dict) -> dict:
    """Complete Messages-API kwargs for one triage call (shared by sync + batch paths)."""
    sys = (
        "You are a triage filter for a prediction-market forecasting bot with a research "
        "budget. You decide whether a market is WORTH a full research forecast. Lean toward "
        "YES whenever the outcome is researchable — named teams, people, or events, scheduled "
        "games, elections, product launches, or anything where news, base rates, or public "
        "data could inform a view. A knowable matchup IS researchable even when it resolves "
        "today. Say NO mainly when there is genuinely nothing findable, the market is "
        "effectively settled (price near 0 or 1), or the question is too vague to pin an "
        "outcome on. Use your own judgment — you don't have to forecast everything, but don't "
        "reflexively pass on a researchable event just because it's a coin-flip-looking sport."
    )
    user = (
        f"Question: {market['question']}\n"
        f"Resolution detail: {market.get('description', '(none)')}\n"
        f"Current price (implied prob of '{market['target_label']}'): {market['market_price']:.2f}\n"
        f"Category: {market.get('category', 'unknown')}\n"
        f"Closes in: {market.get('hours_to_close', 0):.0f}h\n\n"
        "Worth researching?"
    )
    return {
        "model": config.BRAIN_TRIAGE_MODEL,
        "max_tokens": 400,
        "system": sys,
        "messages": [{"role": "user", "content": user}],
        "output_config": _output_config(_TRIAGE_SCHEMA, config.BRAIN_TRIAGE_MODEL),
    }


async def _triage(market: dict):
    """Cheap Haiku gate: is this market worth spending research budget on?
    Returns the parsed dict or None on error/refusal."""
    client = _get_client()
    try:
        resp = await client.messages.create(**_triage_params(market))
    except Exception as exc:
        if not _note_billing_error(exc):
            log.warning("[Brain] triage call failed: %s", exc)
        return None
    _spend.record_usage(config.BRAIN_TRIAGE_MODEL, resp.usage)
    if resp.stop_reason == "refusal":
        return None
    try:
        return json.loads(_first_text(resp))
    except Exception:
        return None


def _research_params(market: dict, siblings: list = None) -> dict:
    """Complete Messages-API kwargs for one research call (web search, free-form — shared by
    sync + batch paths). NO structured output here — web-search citations forbid it.
    `siblings`: other derivative markets of the SAME event — the brief should carry the facts
    that inform all of them (expected goals, form, lineups, likely scorelines, etc.)."""
    sys = (
        "You are a forecasting research analyst. Given a prediction-market question, "
        "search the web for the most relevant CURRENT evidence: recent news, base rates, "
        "scheduled events, expert views, and anything that bears on the outcome. Then write "
        "a concise evidence brief (≈250 words): the key factors for and against, the most "
        "decisive facts, and your qualitative sense of likelihood. When related markets on "
        "the same event are listed, make the brief rich enough to inform ALL of them "
        "(totals, margins, both-sides dynamics — not just the headline question). Do NOT "
        "simply restate the market price — reason from the evidence. Note your uncertainty."
    )
    sib_bit = ""
    if siblings:
        lines = "\n".join(f"- {m['question']}" for m in siblings[:6])
        sib_bit = f"\nRelated markets on the SAME event (the brief must inform these too):\n{lines}\n"
    user = (
        f"Question: {market['question']}\n"
        f"Resolution detail: {market.get('description', '(none)')}\n"
        f"Target outcome to assess: '{market['target_label']}'\n"
        f"Closes in: {market.get('hours_to_close', 0):.0f}h\n"
        f"{sib_bit}\n"
        "Research this and write the evidence brief."
    )
    return {
        "model": config.BRAIN_FORECAST_MODEL,
        "max_tokens": 3000,
        "system": sys,
        "messages": [{"role": "user", "content": user}],
        "tools": [{"type": "web_search_20260209", "name": "web_search",
                   "max_uses": max(1, config.BRAIN_RESEARCH_MAX_SEARCHES)}],
        "output_config": _output_config(None, config.BRAIN_FORECAST_MODEL),
    }


async def _research(market: dict):
    """Sonnet + server-side web search → free-form evidence brief (with citations).
    Handles the server-tool pause_turn loop. Returns (brief, n_web_searches) or (None, 0)."""
    client = _get_client()
    params = _research_params(market)
    messages = params["messages"]
    n_search = 0
    resp = None
    try:
        for _ in range(5):  # cap server-tool continuations
            resp = await client.messages.create(**{**params, "messages": messages})
            _spend.record_usage(config.BRAIN_FORECAST_MODEL, resp.usage)
            n_search += _count_web_searches(resp)
            if resp.stop_reason == "pause_turn":
                messages = messages + [{"role": "assistant", "content": resp.content}]
                continue
            break
    except Exception as exc:
        if not _note_billing_error(exc):
            log.warning("[Brain] research call failed: %s", exc)
        return None, n_search
    _spend.record_web_searches(n_search)
    if resp is None or resp.stop_reason == "refusal":
        return None, n_search
    brief = _first_text(resp).strip()
    return (brief or None), n_search


def _parse_forecast_msg(msg):
    """Parse one forecast response message into the ensemble dict (shared sync + batch)."""
    if msg is None or msg.stop_reason == "refusal":
        return None
    try:
        d = json.loads(_first_text(msg))
    except Exception:
        return None
    return {
        "probability": _clamp(d.get("probability")),
        "confidence": _clamp(d.get("confidence")),
        "factors": d.get("key_factors", [])[:4],
        "take": (d.get("take") or "").strip()[:280],
    }


def _forecast_params(market: dict, brief: str, run_idx: int, model: str = None) -> dict:
    """Complete Messages-API kwargs for one forecast run (shared by sync + batch paths)."""
    model = model or config.BRAIN_FORECAST_MODEL
    sys = (
        "You are the brain of a scrappy Polymarket bot whose friends watch its calls on "
        "Telegram. You have a personality: a sharp, witty prediction-market analyst — confident "
        "when the evidence is real, candid when it's thin, dry sense of humor, never a hype-man. "
        "Output an honest probability that the target outcome occurs, grounded in the research "
        "brief and base rates. Be decisive: if the evidence points one way, do not hedge toward "
        "50%. Report genuine confidence (how much the evidence constrains the answer), not false "
        "certainty. Then write 'take': one or two punchy sentences in YOUR voice giving the real "
        "reason for the call, with a bit of wit where it fits — the line a friend would "
        "screenshot. No hedging-speak, no disclaimers, no emoji. Probability and confidence are "
        "both in [0,1]."
    )
    user = (
        f"Market: {market['question']}\n"
        f"The probability you output is P('{market['target_label']}' occurs).\n"
        f"Current market price for this outcome: {market['market_price']:.2f}\n\n"
        f"Research brief:\n{brief}\n\n"
        f"Analytical pass #{run_idx + 1}: weigh the evidence from your own angle and give your probability."
    )
    return {
        "model": model,
        "max_tokens": 700,
        "system": sys,
        "messages": [{"role": "user", "content": user}],
        "output_config": _output_config(_FORECAST_SCHEMA, model),
    }


async def _forecast_once(market: dict, brief: str, run_idx: int, model: str = None):
    """One structured forecast run (no tools). Model defaults to the Sonnet forecast model;
    the real-time vet passes the cheap Haiku vet model. Returns a dict or None."""
    model = model or config.BRAIN_FORECAST_MODEL
    client = _get_client()
    try:
        resp = await client.messages.create(**_forecast_params(market, brief, run_idx, model))
    except Exception as exc:
        if not _note_billing_error(exc):
            log.warning("[Brain] forecast call failed: %s", exc)
        return None
    _spend.record_usage(model, resp.usage)
    return _parse_forecast_msg(resp)


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
        if not _note_billing_error(exc):
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
# Batch API scanner — same pipeline at a 50% token discount, event-grouped
# ===========================================================================

# Mirror of the trader's event normalization: derivative "event" slugs collapse to one bucket.
_EVENT_SLUG_SUFFIXES = ("-more-markets", "-exact-score", "-halftime-result",
                        "-first-half", "-second-half")


def _event_key_b(slug, market_id: str) -> str:
    s = (slug or "").strip().lower()
    if not s:
        return market_id or "?"
    for suf in _EVENT_SLUG_SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    return s


def _group_events(cands: list) -> list:
    """Group scanner candidates by underlying event, preserving candidate order (which is
    soonest-to-resolve first). Returns [(event_key, [markets...]), ...]."""
    groups: dict = {}
    order: list = []
    for c in cands:
        k = _event_key_b(c.get("slug"), c["market_id"])
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(c)
    return [(k, groups[k]) for k in order]

async def _batch_run(reqs: list) -> dict:
    """Submit [(custom_id, params), ...] as ONE Message Batch, poll to completion, return
    {custom_id: message} for succeeded requests. The scanner is latency-insensitive, so the
    50% batch discount is free money. Raises on submission failure (caller falls back)."""
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    client = _get_client()
    batch = await client.messages.batches.create(requests=[
        Request(custom_id=cid, params=MessageCreateParamsNonStreaming(**params))
        for cid, params in reqs
    ])
    t0 = time.monotonic()
    while True:
        b = await client.messages.batches.retrieve(batch.id)
        if b.processing_status == "ended":
            break
        if time.monotonic() - t0 > config.BRAIN_BATCH_TIMEOUT_S:
            log.warning("[Brain] batch %s timed out after %ds — cancelling",
                        batch.id, config.BRAIN_BATCH_TIMEOUT_S)
            try:
                await client.messages.batches.cancel(batch.id)
            except Exception:
                pass
            break
        await asyncio.sleep(config.BRAIN_BATCH_POLL_SECONDS)
    out = {}
    results = client.messages.batches.results(batch.id)
    if hasattr(results, "__await__"):
        results = await results
    async for entry in results:
        if entry.result.type == "succeeded":
            out[entry.custom_id] = entry.result.message
    return out


async def _scan_cycle_batched(candidates: list, sender, http_client) -> int:
    """One scanner cycle through the Batches API, EVENT-GROUPED: triage one representative
    per event → research the EVENT once (brief written to inform all its derivative markets)
    → forecast every sibling market against the shared brief → finalize each. One research
    call now feeds up to BRAIN_EVENT_SIBLINGS forecastable markets (~3x picks per research
    dollar); the trader's $8/event exposure cap bounds the resulting correlated picks.
    Returns the number of markets fully forecast."""
    cands = [c for c in candidates if not _recently_forecast(c["market_id"])]
    if not cands or _billing_paused():
        return 0
    if not _spend.can_afford(config.BRAIN_EST_FORECAST_USD * 0.5):
        log.info("[Brain] daily budget exhausted ($%.2f spent) — skipping batch cycle",
                 _spend.spent_today())
        return 0
    events = _group_events(cands)[: max(1, config.BRAIN_BATCH_MARKETS)]
    events = [(k, ms[: max(1, config.BRAIN_EVENT_SIBLINGS)]) for k, ms in events]
    mcost: dict = {}
    for _, ms in events:
        for c in ms:
            mcost[c["market_id"]] = 0.0

    # Stage 1 — triage one REPRESENTATIVE market per event (Haiku, one batch).
    res = await _batch_run([(f"t{i}", _triage_params(ms[0])) for i, (k, ms) in enumerate(events)])
    keep = []
    for i, (k, ms) in enumerate(events):
        rep = ms[0]
        msg = res.get(f"t{i}")
        ok = False
        if msg is not None:
            mcost[rep["market_id"]] += _spend.record_usage(config.BRAIN_TRIAGE_MODEL, msg.usage, batch=True)
            if msg.stop_reason != "refusal":
                try:
                    ok = bool(json.loads(_first_text(msg)).get("worth_forecasting"))
                except Exception:
                    ok = False
        if ok:
            keep.append((k, ms))
        else:
            log.info("[Brain] batch triage declined event %.40s (%d markets)", rep["question"], len(ms))
    if not keep:
        return 0

    # Budget guard: shrink the research fan-out to what the remaining budget covers.
    est_each = config.BRAIN_EST_FORECAST_USD * 0.5   # batch rate, per event
    afford = max(1, int(_spend.remaining() / max(est_each, 0.01)))
    keep = keep[:afford]

    # Stage 2 — research each EVENT once (Sonnet + web search, brief covers the siblings).
    res = await _batch_run([(f"r{i}", _research_params(ms[0], siblings=ms[1:]))
                            for i, (k, ms) in enumerate(keep)])
    briefs: dict = {}
    for i, (k, ms) in enumerate(keep):
        rep = ms[0]
        msg = res.get(f"r{i}")
        if msg is None:
            continue
        mcost[rep["market_id"]] += _spend.record_usage(config.BRAIN_FORECAST_MODEL, msg.usage, batch=True)
        mcost[rep["market_id"]] += _spend.record_web_searches(_count_web_searches(msg))
        if msg.stop_reason == "refusal":
            continue
        brief = _first_text(msg).strip()
        if brief:
            briefs[k] = brief

    # Stage 3 — forecast ensemble for EVERY sibling market of each researched event.
    reqs = []
    for i, (k, ms) in enumerate(keep):
        if k not in briefs:
            continue
        for j, c in enumerate(ms):
            for r in range(max(1, config.BRAIN_ENSEMBLE_N)):
                reqs.append((f"f{i}_{j}_{r}", _forecast_params(c, briefs[k], r)))
    if not reqs:
        return 0
    res = await _batch_run(reqs)
    done = 0
    for i, (k, ms) in enumerate(keep):
        if k not in briefs:
            continue
        for j, c in enumerate(ms):
            forecasts = []
            for r in range(max(1, config.BRAIN_ENSEMBLE_N)):
                msg = res.get(f"f{i}_{j}_{r}")
                if msg is None:
                    continue
                mcost[c["market_id"]] += _spend.record_usage(config.BRAIN_FORECAST_MODEL, msg.usage, batch=True)
                f = _parse_forecast_msg(msg)
                if f:
                    forecasts.append(f)
            if forecasts:
                await _finalize_forecast(c, briefs[k], forecasts, "scanner",
                                         sender, http_client, mcost[c["market_id"]])
                done += 1
    return done


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
    if _billing_paused():
        return None  # API credits exhausted — cooldown armed, don't hammer failed calls
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

    cost = _spend.spent_today() - cost_before
    return await _finalize_forecast(market, brief, forecasts, source, sender, http_client, cost)


async def _finalize_forecast(market: dict, brief: str, forecasts: list, source: str,
                             sender, http_client, cost: float):
    """Shared tail of the pipeline (sequential AND batched paths): aggregate the ensemble,
    reconcile-if-divergent, calibrate, decide, persist, post high-conviction calls to ops,
    and emit a Brain Pick when the bar clears. Returns the row dict."""
    import database

    probs = [f["probability"] for f in forecasts]
    raw = statistics.fmean(probs)
    stdev = statistics.pstdev(probs) if len(probs) > 1 else 0.0
    mean_conf = statistics.fmean(f["confidence"] for f in forecasts)

    # Reconcile only when the runs disagree a lot (cost-efficient supervisor step).
    # (Runs synchronously even in batch mode — it's rare and budget-gated; its cost lands in
    # the daily total but not this row's cost_usd, which is fine for an attribution field.)
    if stdev > config.BRAIN_RECONCILE_STD and len(forecasts) > 1 and _spend.can_afford(0.0):
        rec = await _reconcile(market, brief, forecasts)
        if rec:
            raw = rec["probability"]
            mean_conf = rec["confidence"]

    # Personality: surface the take from the run nearest the final probability
    # (the voice that best matches where the ensemble landed).
    best = min(forecasts, key=lambda f: abs(f["probability"] - raw))
    take = best.get("take") or ""

    # Dispersion haircut: disagreement lowers confidence.
    confidence = _clamp(mean_conf * (1.0 - min(0.5, 2.0 * stdev)))
    calibrated = platt_calibrate(raw)
    price = market["market_price"]
    d = decide(calibrated, price, source, confidence=confidence)
    kelly = kelly_fraction(calibrated, price)

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
        "take": take,
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

    # BRAIN PICK: if this scanner forecast is a high-conviction tradeable edge on a thin/obscure
    # market, emit it as a token-safe synthetic alert the trader can take at discovery stakes.
    if source == "scanner" and config.BRAIN_PICK_ENABLED:
        try:
            _emit_brain_pick(market, calibrated, confidence, take)
        except Exception as exc:
            log.warning("[Brain] pick emit error: %s", exc)
    return row


def _insert_forecast(database, row: dict) -> None:
    database.get_db().execute(
        "INSERT INTO brain_forecasts (created_at, source, market_id, question, target_label, "
        "market_price, brain_prob_raw, brain_prob, confidence, edge, verdict, act, kelly_fraction, "
        "ensemble_n, prob_stdev, evidence, take, models_json, cost_usd, alert_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (row["created_at"], row["source"], row["market_id"], row["question"], row["target_label"],
         row["market_price"], row["brain_prob_raw"], row["brain_prob"], row["confidence"], row["edge"],
         row["verdict"], row["act"], row["kelly_fraction"], row["ensemble_n"], row["prob_stdev"],
         row["evidence"], row.get("take", ""), row["models_json"], row["cost_usd"], row["alert_id"]),
    )
    database.get_db().commit()


def _format_call(row: dict) -> str:
    q = html.escape(row["question"][:140])
    tgt = html.escape(row["target_label"][:40])
    src = "🔎 SCANNER" if row["source"] == "scanner" else "⚖️ VETO"
    take = html.escape((row.get("take") or "").strip()[:300])
    take_line = f"💬 <i>{take}</i>\n\n" if take else ""
    return (
        f"🧠 <b>BRAIN — {src}</b> <i>(shadow · paper only)</i>\n\n"
        f"<b>{q}</b>\n"
        f"outcome: <b>{tgt}</b>\n"
        f"brain <b>{row['brain_prob']*100:.0f}%</b> vs market <b>{row['market_price']*100:.0f}%</b> "
        f"→ edge <b>{row['edge']*100:+.0f}pp</b> · <b>{row['verdict']}</b>\n"
        f"conf {row['confidence']*100:.0f}% · {row['ensemble_n']} runs\n\n"
        f"{take_line}"
        f"<i>no position taken — logged for calibration.</i>"
    )


# ===========================================================================
# Real-time vet — called synchronously by the trader at trade time
# ===========================================================================

async def vet_alert(alert: dict) -> dict:
    """Real-time, on-demand vet of ONE tradeable alert — called synchronously by the trader at
    trade time so the brain actually weighs in BEFORE the position is taken (the hourly loop is
    far too slow to action a ~30s trade). Streamlined for latency: research → small ensemble →
    calibrate (no separate triage; the trader already wants this one). Logs to brain_forecasts
    (source='live') for the calibration record + the audience digest, and returns the verdict
    for the trader to size on. Respects the daily spend cap (returns ok=False on exhaustion)."""
    import database
    if not _enabled():
        return {"ok": False, "reason": "brain disabled"}
    if _billing_paused():
        return {"ok": False, "reason": "api credits exhausted", "verdict": "BUDGET"}
    market_id = alert.get("market_id")
    bet_side = alert.get("bet_side")
    try:
        price = float(alert.get("bet_price_at_alert"))
    except (TypeError, ValueError):
        price = None
    if not (market_id and bet_side) or price is None or not (0.0 < price < 1.0):
        return {"ok": False, "reason": "unvettable alert"}
    if not _spend.can_afford(config.BRAIN_EST_FORECAST_USD * 0.6):
        log.info("[Brain] LIVE vet skipped — daily budget reached ($%.2f spent)", _spend.spent_today())
        return {"ok": False, "reason": "daily budget reached", "verdict": "BUDGET"}

    market = {
        "market_id": market_id,
        "question": alert.get("market_question") or market_id,
        "target_label": bet_side,
        "market_price": price,
        "description": "(live insider alert — is this side mispriced right now?)",
        "category": alert.get("market_category") or "unknown",
        "hours_to_close": alert.get("hours_to_close_at_alert") or 0.0,
        "alert_id": alert.get("alert_id"),
    }
    cost_before = _spend.spent_today()
    if config.BRAIN_VET_WEB_SEARCH:
        # Opt-in deep path: research the open web (slow ~45-70s, costly — risks the trade-path
        # timeout). Only use when the caller accepts the latency/cost.
        brief, _ = await _research(market)
        if not brief:
            return {"ok": False, "reason": "no research produced"}
    else:
        # Default fast path: no web search. Reason from the model's own knowledge + base rates +
        # the market price (~8s, ~$0.02). The model is told to stay humble without live research.
        brief = ("(Fast real-time vet — NO live web search. Reason from base rates, your own "
                 "knowledge of the participants/event, and the market price. Factor in the extra "
                 "uncertainty of not having searched — do NOT claim a strong edge unless your "
                 "priors are genuinely strong; when unsure, stay near the market price.)")
    forecasts = []
    for i in range(max(1, config.BRAIN_VET_ENSEMBLE_N)):
        if not _spend.can_afford(0.0):
            break
        # Vets run on the cheap Haiku model (~$0.001/vet) — they're commentary + rare gates;
        # Sonnet is reserved for the scanner where real research (and money) rides.
        f = await _forecast_once(market, brief, i, model=config.BRAIN_VET_MODEL)
        if f:
            forecasts.append(f)
    if not forecasts:
        return {"ok": False, "reason": "no forecast produced"}

    probs = [f["probability"] for f in forecasts]
    raw = statistics.fmean(probs)
    stdev = statistics.pstdev(probs) if len(probs) > 1 else 0.0
    mean_conf = statistics.fmean(f["confidence"] for f in forecasts)
    best = min(forecasts, key=lambda f: abs(f["probability"] - raw))
    take = best.get("take") or ""
    confidence = _clamp(mean_conf * (1.0 - min(0.5, 2.0 * stdev)))
    calibrated = platt_calibrate(raw)
    d = decide(calibrated, price, "veto", confidence=confidence)
    kelly = kelly_fraction(calibrated, price)
    cost = _spend.spent_today() - cost_before

    row = {
        "created_at": int(time.time()), "source": "live", "market_id": market_id,
        "question": market["question"], "target_label": bet_side, "market_price": float(price),
        "brain_prob_raw": float(raw), "brain_prob": float(calibrated), "confidence": float(confidence),
        "edge": float(d["edge"]), "verdict": d["verdict"], "act": 1 if d["act"] else 0,
        "kelly_fraction": float(kelly), "ensemble_n": len(forecasts), "prob_stdev": float(stdev),
        "evidence": brief[:1200], "take": take,
        "models_json": json.dumps({"forecast": config.BRAIN_FORECAST_MODEL, "mode": "live"}),
        "cost_usd": float(cost), "alert_id": alert.get("alert_id"),
    }
    _insert_forecast(database, row)
    log.info("[Brain] LIVE vet | %.45s | %s P=%.2f vs px=%.2f edge=%+.2f conf=%.2f %s%s | $%.3f",
             market["question"], bet_side, calibrated, price, d["edge"], confidence,
             d["verdict"], " ACT" if d["act"] else "", cost)
    return {
        "ok": True, "verdict": d["verdict"], "act": bool(d["act"]),
        "confidence": float(confidence), "edge": float(d["edge"]),
        "brain_prob": float(calibrated), "market_price": float(price), "take": take,
    }


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
            # Gamma carries the slug on the EVENT object, not the market — inject it so
            # sibling markets of one match group into a single research bucket.
            if not m.get("_event_slug"):
                m["_event_slug"] = event.get("slug") or ""
            cand = _scanner_market(m, now)
            if cand:
                out.append(cand)
    # Soonest-to-resolve first (volume as tiebreak): every one of the picks' first 7 wins was a
    # short-dated market, and fast resolution means fast capital turnover, fast grading, and a
    # fast path to the statistical proof that unlocks bigger stakes. (Was cheapest-volume-first.)
    out.sort(key=lambda c: (c["hours_to_close"] or 1e9, c["_volume"]))
    if config.BRAIN_BATCH_ENABLED:
        # Wide set for the batched path: event grouping needs the SIBLING markets of the
        # chosen events to still be present (they'd be cut by a tight per-market limit).
        return out[: max(24, config.BRAIN_BATCH_MARKETS * config.BRAIN_EVENT_SIBLINGS * 3)]
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
    tokens = _jl(m.get("clobTokenIds"))
    if not (isinstance(outcomes, list) and isinstance(prices, list)) or len(outcomes) != 2 or len(prices) != 2:
        return None  # binary markets only
    # Brain Picks require resolvable token ids parallel to outcomes (token-safe side selection).
    tradeable = isinstance(tokens, list) and len(tokens) == 2 and all(tokens)
    p0 = _parse_float(prices[0])
    p1 = _parse_float(prices[1])
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
        # Brain Pick fields (token-safe trading): the outcome labels, their prices, the
        # parallel CLOB token ids, end date, neg_risk + slug, and the raw market for upsert.
        "tradeable": tradeable,
        "outcomes": [str(outcomes[0]), str(outcomes[1])],
        "prices": [p0, p1 if p1 is not None else (1.0 - p0)],
        "clob_token_ids": [str(tokens[0]), str(tokens[1])] if tradeable else None,
        "end_date": end_raw,
        "neg_risk": bool(m.get("negRisk")) if m.get("negRisk") is not None else None,
        "slug": m.get("_event_slug") or m.get("slug"),
        "raw": m,
    }


# ===========================================================================
# Brain Picks — the brain's own research-driven trades on thin/obscure markets
# ===========================================================================

def _brain_pick_side(market: dict, brain_prob: float):
    """Pure, token-SAFE buy-side selection. brain_prob is the calibrated P(outcomes[0]).
    Returns the side the brain thinks is underpriced — the outcome label, its parallel CLOB
    token id, its current price, and the brain's edge on THAT side (>= the other side's edge).
    Returns None if the market isn't tradeable (no token ids). The outcome↔token mapping is
    by index (both come parallel from the same Gamma market), so the side is never inverted."""
    if not market.get("tradeable"):
        return None
    outcomes = market.get("outcomes") or []
    prices = market.get("prices") or []
    tokens = market.get("clob_token_ids") or []
    if len(outcomes) != 2 or len(prices) != 2 or len(tokens) != 2:
        return None
    p0, p1 = float(prices[0]), float(prices[1])
    edge0 = brain_prob - p0              # >0 ⇒ outcomes[0] underpriced
    edge1 = (1.0 - brain_prob) - p1      # >0 ⇒ outcomes[1] underpriced
    idx, edge = (0, edge0) if edge0 >= edge1 else (1, edge1)
    return {
        "buy_side": str(outcomes[idx]),
        "buy_token": str(tokens[idx]),
        "buy_price": float(prices[idx]),
        "edge": float(edge),
        "brain_prob_side": float(brain_prob if idx == 0 else 1.0 - brain_prob),
    }


def _emit_brain_pick(market: dict, calibrated: float, confidence: float, take: str) -> bool:
    """If the scanner forecast is a high-conviction tradeable edge, write it as a synthetic
    alert_outcomes row (a Brain Pick) so the trader takes it via the PROVEN token-safe path
    (resolve_token_id_for_side + the normal order/resolution machinery). Returns True if emitted."""
    import database
    pick = _brain_pick_side(market, calibrated)
    if not pick:
        return False
    if pick["edge"] < config.BRAIN_PICK_MIN_EDGE or confidence < config.BRAIN_PICK_MIN_CONFIDENCE:
        return False
    ts = int(time.time())
    alert_id = f"brain_{market['market_id']}_{ts}"
    # Ensure the market is in `markets` so resolve_token_id_for_side has clob_token_ids + outcomes.
    raw = dict(market.get("raw") or {})
    raw.setdefault("outcomes", market["outcomes"])
    if market.get("neg_risk") is not None:
        raw.setdefault("negRisk", market["neg_risk"])
    if market.get("slug"):
        raw.setdefault("_event_slug", market["slug"])
    database.upsert_market(
        condition_id=market["market_id"], title=market["question"],
        clob_token_ids=market["clob_token_ids"], end_date=market.get("end_date"),
        raw_json=raw, active=True,
    )
    database.insert_alert_outcome(
        alert_id=alert_id, market_id=market["market_id"], market_question=market["question"],
        wallet_address="brain", score=config.BRAIN_PICK_SCORE,
        score_breakdown_json=json.dumps({"source": "brain_pick", "edge": round(pick["edge"], 4),
                                         "confidence": round(confidence, 3),
                                         "brain_prob": round(pick["brain_prob_side"], 4),
                                         "take": (take or "")[:300]}),
        bet_side=pick["buy_side"], bet_price_at_alert=pick["buy_price"], bet_size_usd=0.0,
        market_category="brain", hours_to_close_at_alert=market.get("hours_to_close"),
    )
    # Link the scanner's brain_forecasts row to this pick's alert so it GRADES on resolution
    # (pre-2026-07-04 scanner rows never graded — the calibration report was blind to the one
    # strategy that was actually winning).
    db = database.get_db()
    db.execute(
        "UPDATE brain_forecasts SET alert_id = ? WHERE id = ("
        "  SELECT id FROM brain_forecasts WHERE market_id = ? AND source = 'scanner' "
        "  AND alert_id IS NULL ORDER BY created_at DESC LIMIT 1)",
        (alert_id, market["market_id"]),
    )
    db.commit()
    log.info("[Brain] PICK emitted: %.45s | buy %s @ %.2f | edge=%+.2f conf=%.2f",
             market["question"], pick["buy_side"], pick["buy_price"], pick["edge"], confidence)
    return True


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
        "SELECT bf.id, bf.brain_prob, bf.market_price, bf.target_label, "
        "       ao.bet_side, ao.resolution_status "
        "FROM brain_forecasts bf JOIN alert_outcomes ao ON bf.alert_id = ao.alert_id "
        "WHERE bf.source IN ('veto', 'live', 'scanner') AND bf.resolved_outcome IS NULL "
        "  AND ao.resolution_status IN ('resolved_won', 'resolved_lost')"
    ).fetchall()
    n = 0
    for r in rows:
        # Did the ALERT's side win? For veto/live rows target_label == bet_side (identity).
        # Scanner rows forecast P(outcomes[0]) but the linked pick may have bought the OTHER
        # side — flip the outcome so the Brier grades the probability actually stored.
        outcome = 1 if r["resolution_status"] == "resolved_won" else 0
        if (r["target_label"] or "").strip().lower() != (r["bet_side"] or "").strip().lower():
            outcome = 1 - outcome
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

    def _agg(where: str):
        rows = db.execute(
            f"SELECT brain_prob, market_price, resolved_outcome FROM brain_forecasts "
            f"WHERE resolved_outcome IS NOT NULL AND {where}"
        ).fetchall()
        return aggregate_brier((g["brain_prob"], g["market_price"], g["resolved_outcome"]) for g in rows)

    # The scoreboard that matters: the RESEARCHED scanner/pick forecasts (real money rides on
    # these) vs the no-web trade-time vets (commentary — expected to roughly track the market).
    picks = _agg("source = 'scanner'")
    vets = _agg("source IN ('veto', 'live')")
    total = db.execute("SELECT COUNT(*) FROM brain_forecasts").fetchone()[0]
    head = "🧠 <b>BRAIN CALIBRATION</b>"

    def _line(label, a):
        if a["n"] == 0:
            return f"{label}: <i>none graded yet</i>"
        delta = a["market"] - a["brain"]
        mark = "✅" if delta > 0 else "❌"
        return (f"{label}: <b>{a['n']}</b> graded · brain <b>{a['brain']:.4f}</b> vs "
                f"market <b>{a['market']:.4f}</b> {mark} (Δ {delta:+.4f})")

    return (
        f"{head}\n\n"
        f"🔬 {_line('<b>PICKS</b> (researched, real $)', picks)}\n"
        f"💬 {_line('vets (no-web commentary)', vets)}\n\n"
        f"<i>{total} forecasts logged · lower Brier = better calibrated. The PICKS line is the "
        f"graduation gate — a durable ✅ there over weeks earns bigger stakes.</i>"
    )


# ===========================================================================
# Scaling scoreboard — the numbers that gate each capital raise
# ===========================================================================

def _mean_ci(values: list) -> tuple:
    """(mean, lo, hi) 95% normal-approx CI. Honest degenerate cases for n<2."""
    n = len(values)
    if n == 0:
        return (0.0, 0.0, 0.0)
    m = statistics.fmean(values)
    if n < 2:
        return (m, m, m)
    half = 1.96 * statistics.stdev(values) / math.sqrt(n)
    return (m, m - half, m + half)


def scaling_scoreboard() -> str:
    """Weekly ops report: the four numbers a capital-raise decision needs — graded sample
    size, ROI with CI, PICKS Brier vs market, and execution headroom — plus an explicit
    Stage-1 gate checklist. This makes 'wire in the next $X?' a look at a card, not a vibe."""
    import database
    db = database.get_db()

    placed = db.execute(
        "SELECT COUNT(*) n, SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) partial, "
        "MIN(created_at) first_ts FROM trade_executions "
        "WHERE alert_id LIKE 'brain_%' AND status IN ('filled','partial')"
    ).fetchone()
    graded = db.execute(
        "SELECT size_usdc, pnl, resolution_status FROM trade_executions "
        "WHERE alert_id LIKE 'brain_%' AND status IN ('filled','partial') "
        "  AND resolution_status IN ('won','lost')"
    ).fetchall()
    rois = [float(g["pnl"] or 0.0) / float(g["size_usdc"]) for g in graded if (g["size_usdc"] or 0) > 0]
    wins = sum(1 for g in graded if g["resolution_status"] == "won")
    losses = len(graded) - wins
    roi_m, roi_lo, roi_hi = _mean_ci(rois)

    cal = db.execute(
        "SELECT AVG(brier_brain) bb, AVG(brier_market) bm, COUNT(*) n FROM brain_forecasts "
        "WHERE source='scanner' AND resolved_outcome IS NOT NULL"
    ).fetchone()
    brier_ok = cal["n"] and cal["bb"] is not None and cal["bm"] is not None and cal["bb"] < cal["bm"]

    n_placed = placed["n"] or 0
    days = max(1.0, (time.time() - (placed["first_ts"] or time.time())) / 86400.0)
    per_day = n_placed / days
    partial_pct = 100.0 * (placed["partial"] or 0) / n_placed if n_placed else 0.0

    n_g = len(graded)
    gate_n = n_g >= 40
    gate_roi = n_g >= 10 and roi_lo > 0
    gate_brier = bool(brier_ok and (cal["n"] or 0) >= 20)
    gates_met = sum([gate_n, gate_roi, gate_brier])

    def _g(ok):
        return "✅" if ok else "❌"

    cal_bit = (f"brain <b>{cal['bb']:.4f}</b> vs market <b>{cal['bm']:.4f}</b> (n={cal['n']})"
               if cal["n"] else "<i>none graded yet</i>")
    verdict = ("<b>STAGE 1 UNLOCKED</b> — evidence supports the first capital raise."
               if gates_met == 3 else
               f"keep running Stage 0 — {gates_met}/3 gates met, evidence accumulating.")
    return (
        "📈 <b>SCALING SCOREBOARD</b> — <i>Stage 0 (discovery)</i>\n\n"
        f"🎯 picks: <b>{n_placed}</b> placed · <b>{n_g}</b> graded ({wins}W-{losses}L) · {per_day:.1f}/day\n"
        f"💵 ROI <b>{roi_m*100:+.1f}%</b> (95% CI {roi_lo*100:+.1f}%..{roi_hi*100:+.1f}%)\n"
        f"🧮 PICKS Brier: {cal_bit}\n"
        f"⚙️ execution: {partial_pct:.0f}% partial fills\n\n"
        f"<b>STAGE 1 GATE</b> (all ✅ → raise stakes + bankroll):\n"
        f"  {_g(gate_n)} n≥40 graded ({n_g}/40)\n"
        f"  {_g(gate_roi)} ROI 95% CI floor &gt; 0\n"
        f"  {_g(gate_brier)} PICKS Brier beats market (n≥20)\n\n"
        f"{verdict}"
    )


# ===========================================================================
# Audience decision digest — keeps V1 Poly alive between bets
# ===========================================================================

def _recent_brain_decisions(since_ts: int, limit: int = 14) -> list:
    """Recent brain decisions (live vets + scanner ideas) for the audience digest."""
    import database
    rows = database.get_db().execute(
        "SELECT question, target_label, verdict, act, edge, confidence, take, source, created_at "
        "FROM brain_forecasts WHERE created_at >= ? ORDER BY created_at DESC LIMIT ?",
        (since_ts, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def format_brain_digest(rows) -> "str | None":
    """Readable AUDIENCE summary (V1 Poly) of what the brain blessed and what it passed on,
    with its reasons — gives the channel life during quiet, alert-less stretches. None when
    there's nothing new to say."""
    if not rows:
        return None
    blessed = [r for r in rows if r["verdict"] == "CONFIRM" and r["act"]]
    passed = [r for r in rows if not (r["verdict"] == "CONFIRM" and r["act"])]
    lines = ["🧠 <b>THE BRAIN — what I've been chewing on</b>", ""]
    if blessed:
        lines.append(f"✅ <b>Liked {len(blessed)}</b> enough to want extra weight on:")
        for r in blessed[:5]:
            lines.append(f"• <b>{html.escape(str(r['target_label'])[:34])}</b> — "
                         f"{html.escape(str(r['question'])[:64])}")
            if r.get("take"):
                lines.append(f"  <i>{html.escape(str(r['take'])[:170])}</i>")
        lines.append("")
    if passed:
        lines.append(f"🤔 <b>Looked at {len(passed)}, stayed flat</b> — not convinced:")
        for r in passed[:6]:
            lines.append(f"• <b>{html.escape(str(r['question'])[:64])}</b>")
            if r.get("take"):
                lines.append(f"  <i>{html.escape(str(r['take'])[:170])}</i>")
        lines.append("")
    lines.append("<i>just the brain thinking out loud — blessed calls get more weight on the "
                 "book, the rest ride normal. shadow opinions, not promises.</i>")
    return "\n".join(lines)


# ===========================================================================
# The loop — shadow, ops-routed, cost-capped
# ===========================================================================

async def brain_loop(dry_run: bool = False) -> None:
    """Single supervised loop. Each cycle: grade resolved forecasts, run the veto
    + scanner sources through the forecast engine (bounded by the per-cycle cap and
    the hard daily spend cap), and post a daily calibration report to ops. No-ops
    cleanly when the brain is disabled (no key / BRAIN_ENABLED=false)."""
    import httpx
    from alerter import make_research_sender, make_audience_sender

    if not _enabled():
        log.info("[Brain] disabled (BRAIN_ENABLED=%s, api_key=%s) — loop idle.",
                 config.BRAIN_ENABLED, "set" if config.BRAIN_API_KEY else "MISSING")
        # Stay alive but inert so the supervisor doesn't thrash restarting us.
        while True:
            await asyncio.sleep(3600)

    log.info("[Brain] STARTED — shadow=%s cap=$%.2f/day models=%s/%s vet=%s effort=%s ensemble=%d "
             "batch=%s (trader alerts vetted in real time via /api/brain/vet)",
             config.BRAIN_SHADOW, config.BRAIN_DAILY_USD_CAP, config.BRAIN_TRIAGE_MODEL,
             config.BRAIN_FORECAST_MODEL, config.BRAIN_VET_MODEL, config.BRAIN_EFFORT,
             config.BRAIN_ENSEMBLE_N,
             f"on({config.BRAIN_BATCH_MARKETS}/cycle, 50%% off)" if config.BRAIN_BATCH_ENABLED else "off")
    sender = make_research_sender()
    audience = make_audience_sender()
    last_report_day = None
    last_scoreboard_day = None
    last_digest_ts = time.time()  # don't dump history on first boot
    digest_interval = max(1800.0, config.BRAIN_DIGEST_HOURS * 3600)

    async with httpx.AsyncClient() as http_client:
        while True:
            cycle_start = time.monotonic()
            try:
                graded = grade_resolved_forecasts()
                if graded:
                    log.info("[Brain] graded %d newly-resolved forecasts", graded)

                # SCANNER only — the trader's own alerts are now vetted in REAL TIME at trade
                # time (vet_alert via /api/brain/vet), so the slow hourly veto source is retired.
                # The scanner is the brain's independent idea generator (and fills the digest
                # during quiet, alert-less stretches). Batched path first (50% discount, ~2x
                # markets per dollar); any batch failure falls back to the sequential pipeline.
                done = 0
                batched_ok = False
                candidates = await _scanner_candidates(http_client)
                if config.BRAIN_BATCH_ENABLED and candidates and not _billing_paused():
                    try:
                        done = await _scan_cycle_batched(
                            candidates, None if dry_run else sender, http_client)
                        batched_ok = True   # ran to completion (0 forecasts ≠ failure)
                    except Exception as exc:
                        if not _note_billing_error(exc):
                            log.warning("[Brain] batch scan failed (%s) — sequential fallback", exc)
                if not batched_ok and not _billing_paused():
                    for cand in candidates:
                        if done >= config.BRAIN_MAX_PER_CYCLE or not _spend.can_afford(config.BRAIN_EST_FORECAST_USD):
                            break
                        if await forecast_market(cand, None if dry_run else sender, http_client, "scanner"):
                            done += 1

                # Audience decision digest — readable summary of recent brain calls to V1 Poly.
                now_ts = time.time()
                if now_ts - last_digest_ts >= digest_interval:
                    decisions = _recent_brain_decisions(int(last_digest_ts))
                    last_digest_ts = now_ts
                    text = format_brain_digest(decisions)
                    if text:
                        if dry_run or audience is None:
                            log.info("[Brain] audience digest:\n%s", text)
                        else:
                            await audience.send_message(text, http_client)
                            log.info("[Brain] posted audience digest (%d decisions)", len(decisions))

                # Daily calibration report to ops.
                now = datetime.utcnow()
                if now.hour == config.BRAIN_CAL_REPORT_HOUR_UTC and now.strftime("%Y-%m-%d") != last_report_day:
                    last_report_day = now.strftime("%Y-%m-%d")
                    text = calibration_report()
                    if dry_run or sender is None:
                        log.info("[Brain] calibration report:\n%s", text)
                    else:
                        await sender.send_message(text, http_client)

                # Weekly scaling scoreboard to ops (Mondays, same hour) — the numbers that
                # gate each capital raise on the Stage-0 → Stage-1 → Stage-2 ladder.
                if (now.weekday() == 0 and now.hour == config.BRAIN_CAL_REPORT_HOUR_UTC
                        and now.strftime("%Y-%m-%d") != last_scoreboard_day):
                    last_scoreboard_day = now.strftime("%Y-%m-%d")
                    text = scaling_scoreboard()
                    if dry_run or sender is None:
                        log.info("[Brain] scaling scoreboard:\n%s", text)
                    else:
                        await sender.send_message(text, http_client)

                log.info("[Brain] cycle done — %d scanner forecast(s), $%.3f/$%.2f spent today",
                         done, _spend.spent_today(), config.BRAIN_DAILY_USD_CAP)
            except Exception as exc:
                log.exception("[Brain] cycle error: %s", exc)

            elapsed = time.monotonic() - cycle_start
            await asyncio.sleep(max(60.0, config.BRAIN_SCAN_INTERVAL_SECONDS - elapsed))
