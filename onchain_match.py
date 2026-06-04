"""
onchain_match.py — reliable execution<->outcome joining for EDGE MEASUREMENT.

WHY THIS EXISTS
---------------
The bot's *live* resolution grading is already correct: it keys each alert on
conditionId (market_id) + bet_side and compares to the Gamma winning_outcome
(see resolution_checker._grade_alert). That path is fine.

The problem is MEASUREMENT/reconcile tooling, which historically joined the bot's
executions to the on-chain CSV by market_question STRING. That string is NOT unique
per tradeable outcome:

  • "Bitcoin Up or Down on June 3?"      -> "Up" and "Down" share ONE title
  • "Spread: Spurs (-2.5)"               -> either side of the spread, one title
  • "Knicks vs. Spurs: O/U 217.5"        -> "Over" and "Under" share ONE title
  • 3-way soccer (Win / Draw / Win)      -> multiple sides, one title

So a redeem keyed on the title cannot tell which SIDE won. A 234-bet audit found 57
bets (24%) unjoinable this way; the apparent "28% DB-vs-onchain disagreement" was almost
entirely this join artifact — on clean single-side markets the agreement was 95.5%.

RULES for trustworthy edge measurement
--------------------------------------
  1. Join executions to on-chain truth by conditionId + clob_token_id + side — NEVER by
     market_question. The token id (ERC-1155 position id) is unique per outcome.
  2. EXCLUDE side-ambiguous markets (is_side_ambiguous() below) from any name-based metric
     unless you have the token id to disambiguate.

This module is measurement-only. It touches no trading, sizing, or accounting state.
"""

from __future__ import annotations
import re
from typing import Optional

# Title shapes where ONE question string covers MORE THAN ONE tradeable side, so a
# name-only join cannot attribute a redeem to a side.
_AMBIGUOUS_PATTERNS = [
    r"up or down",                 # "Bitcoin Up or Down on June 3?"
    r"\bspread:",                  # "Spread: Spurs (-2.5)"
    r"\bhandicap\b",               # "Game Handicap: ...", "Map Handicap: ..."
    r"\bo/u\b",                    # "Knicks vs. Spurs: O/U 217.5"
    r"over/under",                 # "Total Kills Over/Under 24.5"
    r"games total", r"total kills", r"\btotal:",  # totals markets (Over/Under share title)
]
_AMBIGUOUS_RE = re.compile("|".join(_AMBIGUOUS_PATTERNS), re.IGNORECASE)


def is_side_ambiguous(market_question: Optional[str]) -> bool:
    """True when the title maps to >1 tradeable side (binary up/down, spreads, totals,
    handicaps), so a name-only redeem join cannot tell which side won. Such markets must
    be joined by token id and should be excluded from name-based edge measurement."""
    if not market_question:
        return False
    return bool(_AMBIGUOUS_RE.search(market_question))


def outcome_by_token(execution: dict, redeemed_token_ids: set) -> Optional[str]:
    """Ground-truth outcome for ONE execution via the RELIABLE token-id join.

    `redeemed_token_ids` = the set of clob_token_id (ERC-1155 position id) values the
    wallet actually redeemed on-chain (build from the Polymarket positions API, where each
    position carries its asset/token id and a redeemable/redeemed flag — NOT from the CSV,
    whose Redeem rows carry no token id).

    Returns 'won' if the execution's token was redeemed, 'lost' if not, None if the
    execution has no token id (cannot be judged reliably — exclude from metrics).
    """
    tid = str(execution.get("clob_token_id") or "").strip()
    if not tid:
        return None
    return "won" if tid in redeemed_token_ids else "lost"


def partition_for_measurement(executions: list) -> tuple[list, list]:
    """Split executions into (clean, ambiguous) by side-ambiguity of their market_question.
    Only the `clean` list is safe for NAME-based edge measurement; the `ambiguous` list
    needs a token-id join (outcome_by_token). Pure, side-effect-free."""
    clean, ambiguous = [], []
    for e in executions:
        (ambiguous if is_side_ambiguous(e.get("market_question")) else clean).append(e)
    return clean, ambiguous
