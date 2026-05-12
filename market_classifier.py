"""
market_classifier.py — Keyword-based market category classification and price band utility.
"""

from typing import Optional

# Priority order: crypto > sports > politics > entertainment > other
_CATEGORIES: list[tuple[str, list[str]]] = [
    ("crypto", [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "sol",
        "price above", "price below", "above $", "below $",
    ]),
    ("sports", [
        # League / org abbreviations
        "ufc", "nba", "nfl", "nhl", "mlb", "mls", "epl", "ncaa", "pga",
        # Generic terms
        "esports", "lol:", "championship", "match", "fight",
        # "X vs. Y" with period (already worked) and "X vs Y" without period
        "vs.", " vs ",
        # NBA teams (titles often omit "nba")
        "lakers", "celtics", "warriors", "bucks", "heat", "nets", "knicks",
        "sixers", "suns", "nuggets", "clippers", "mavericks", "mavs",
        "raptors", "hawks", "bulls", "cavaliers", "pistons", "pacers",
        "hornets", "magic", "wizards", "thunder", "blazers", "jazz",
        "grizzlies", "pelicans", "spurs", "rockets", "kings", "timberwolves",
        # NFL teams
        "chiefs", "eagles", "patriots", "cowboys", "packers", "steelers",
        "ravens", "seahawks", "bills", "49ers", "rams", "broncos", "raiders",
        "bengals", "dolphins", "giants", "jets", "commanders", "bears",
        "lions", "vikings", "jaguars", "texans", "titans", "colts", "chargers",
        "saints", "falcons", "panthers", "buccaneers", "cardinals", "browns",
        # MLB teams
        "yankees", "dodgers", "red sox", "cubs", "astros", "braves",
        "mets", "phillies", "padres", "brewers", "orioles", "tigers",
        # NHL teams
        "bruins", "penguins", "capitals", "lightning", "rangers", "oilers",
        # CS:GO / Valorant esports orgs that slip past "esports" keyword
        "vitality", "natus vincere", "navi", "faze", "astralis", "liquid",
    ]),
    ("politics", [
        "election", "president", "congress", "senate", "vote", "governor",
        "prime minister", "democrat", "republican", "trump", "biden", "parliament",
    ]),
    ("entertainment", [
        "oscar", "grammy", "emmy", "box office", "movie", "album",
        "twitter", "tweet", "youtube",
    ]),
]


def classify_market(title: str) -> str:
    """
    Classify a market title into: crypto, sports, politics, entertainment, or other.
    Matches against lowercased keywords; first category match wins (priority order above).
    Empty or None titles return 'other'.
    """
    if not title:
        return "other"
    lower = title.lower()
    for category, keywords in _CATEGORIES:
        for kw in keywords:
            if kw in lower:
                return category
    return "other"


def bet_price_band(price: float) -> str:
    """
    Classify a bet price (0.0–1.0) into a named band.
      longshot:  0.02–0.30
      uncertain: 0.30–0.50
      lean:      0.50–0.70
      favorite:  0.70–0.98
    Prices outside the filter range (< 0.02 or > 0.98) should never reach here.
    """
    if price <= 0.30:
        return "longshot"
    if price <= 0.50:
        return "uncertain"
    if price <= 0.70:
        return "lean"
    return "favorite"
