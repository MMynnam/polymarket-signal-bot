# Polymarket Insider Signal Bot

Real-time surveillance of Polymarket trades, profiling wallets for insider-like behavior, and firing scored Telegram alerts when conviction signals exceed a configurable threshold.

---

## How It Works

The bot runs six concurrent async loops:

| Loop | Source | Interval |
|---|---|---|
| Market Discovery | Gamma API `/events` | Every 5 min |
| WebSocket Feed | Polymarket CLOB WS | Real-time |
| REST Fallback | Data API `/trades` | Every 30s |
| Trade Processor | Internal queue | Continuous |
| Alert Queue | Telegram Bot API | ≤1 msg/1.5s |
| Heartbeat | SQLite stats | Every 5 min |

When a trade above the size threshold is detected:

1. The wallet is profiled (cached 6 hours in SQLite)
2. An Insider Confidence Score (0–100) is computed
3. If score ≥ threshold (default 60), a Telegram alert fires

---

## Score Components

| Component | Max Pts | What It Measures |
|---|---|---|
| **Timing** | 25 | Distance to market resolution. Bets in the final 2 hours score highest. Uses exponential decay curve — score halves roughly every 24 hours. |
| **Funding Velocity** | 20 | Gap between the wallet's most recent inbound transfer and this bet. Funded and deployed within 1 hour = max score (rapid-deploy insider pattern). Gap > 7 days = 0 pts. Requires Alchemy RPC. |
| **Size Anomaly** | 20 | How large is this bet vs. the wallet's median? 5× median = max score. First-ever large bet scores 12/20 (no baseline). |
| **Wallet Age** | 15 | Inverted: newer wallets score higher. Under 30 days = max. Over 1 year = 0. Burner/purpose-built insider wallets are often fresh. |
| **Concentration** | 10 | What % of the wallet's observable capital is in this single bet? 70%+ = max score. |
| **Underdog Bet** | 10 | Buying a ≤30% outcome scores max. Betting the favorite (≥60%) scores 0. Smart money on underdogs = information signal. |
| **Cluster Bonus** | +10 | Binary. If Alchemy traces reveal this wallet was funded from the same source as another recently-flagged wallet, add 10 pts. |

**Two-tier alerting**: Score ≥ `ALERT_INSTANT_THRESHOLD` (default 70) fires an immediate Telegram message. Scores in [`ALERT_DIGEST_THRESHOLD`–69] (default 60–69) are buffered and sent as a compact digest every `DIGEST_INTERVAL_SECONDS` (default 2 hours).

---

## Setup

### Prerequisites

- Python 3.12+
- A Telegram bot token (from BotFather)
- A Telegram chat ID
- (Optional but recommended) A free Etherscan V2 API key
- (Optional) An Alchemy Polygon RPC URL

### Local Installation

```bash
git clone <your-repo>
cd polymarket-signal-bot

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your credentials
```

### Test Without Sending Messages

```bash
python main.py --dry-run
```

This runs the full pipeline — market discovery, trade monitoring, wallet profiling, scoring — but logs alerts to stdout instead of sending them to Telegram. Use this to verify everything works before going live.

---

## Getting a Free Etherscan V2 API Key

Etherscan V2 is a single unified API that covers Ethereum, Polygon, and other EVM chains under one key.

1. Go to [https://etherscan.io/register](https://etherscan.io/register) and create an account
2. After email verification, go to [https://etherscan.io/myapikey](https://etherscan.io/myapikey)
3. Click **+ Add** to create a new API key
4. Copy the key into your `.env` as `ETHERSCAN_API_KEY=`

Free tier limits: **100,000 API calls/day**, **3 calls/second**. This bot uses Etherscan only for wallet age lookups on new wallets, well within the free tier.

> **Important**: Do NOT use Polygonscan V1 (`api.polygonscan.com`). It was deprecated in August 2025. The bot uses Etherscan V2 with `chainid=137` instead.

---

## Creating a Telegram Bot

1. Open Telegram and search for **@BotFather**
2. Send `/newbot`
3. Choose a name (e.g. "Polymarket Signals") and a username (must end in `bot`)
4. BotFather gives you a token like `1234567890:ABCdef...` — copy it to `TELEGRAM_BOT_TOKEN=`

**Finding your Chat ID:**

Option A — Private chat:
1. Start a conversation with your bot (search its username)
2. Send `/start`
3. Visit `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Find `"chat": {"id": 123456789}` in the response

Option B — Group:
1. Add the bot to your group
2. Send any message in the group
3. Same URL above — group IDs are negative numbers (e.g. `-1001234567890`)

---

## Deploying to Railway

### First Deploy

1. Push your code to a GitHub repository (make sure `.env` is in `.gitignore`)
2. Go to [https://railway.app](https://railway.app) and create an account
3. Click **New Project → Deploy from GitHub repo**
4. Select your repository

### Set Environment Variables

In Railway, go to your service → **Variables** tab, and add every variable from `.env.example` with your real values.

For `SQLITE_DB_PATH`, Railway's free tier does not persist the filesystem between deploys. To keep your database:
1. Add a **Volume** to your service (Railway Dashboard → your service → **Add Volume**)
2. Mount it at `/data`
3. Set `SQLITE_DB_PATH=/data/polymarket_bot.db`

### Deploy

Railway auto-deploys on every push to your default branch. The `railway.toml` in the repo root configures:
- Builder: Nixpacks (auto-detects Python, installs `requirements.txt`)
- Start command: `python main.py`
- Restart policy: always (bot restarts automatically on crashes)

### Monitoring

Railway captures all stdout. The bot logs verbosely:
- Every trade seen (source, market, outcome, price, size)
- Every wallet profiled (cache hit/miss, win rate, age)
- Every alert fired or suppressed (with score breakdown)
- A heartbeat every 5 minutes with database stats

---

## Architecture Notes

### State Persistence

All state lives in SQLite (`polymarket_bot.db`):

| Table | Purpose |
|---|---|
| `markets` | Active markets from Gamma API |
| `last_seen_trades` | Per-market REST dedup cursor |
| `wallet_profiles` | Cached wallet analysis (6h TTL) |
| `alert_history` | Every alert ever fired |
| `flagged_clusters` | Wallets sharing a funding source |

The bot survives Railway restarts cleanly — no in-memory state is lost.

### Failover

- If the WebSocket disconnects, exponential backoff reconnects (2s → 4s → ... → 120s max)
- If Etherscan fails, wallet age scoring is skipped and noted in the alert
- If Alchemy is not configured, cluster detection is disabled (no crash)
- If any single API call fails after 3 retries, it is logged and the pipeline continues

### Rate Limits

| Service | Limit | How We Handle It |
|---|---|---|
| Polymarket WS | 5 connections max | We shard markets across ≤5 connections |
| Etherscan V2 | 3 calls/sec | Token bucket rate limiter in `wallet_profiler.py` |
| Telegram | 1 msg/1.5s per chat | Message queue with enforced sleep |
| Alchemy | 30M CU/month | Used only for new-wallet cluster tracing |

---

## File Structure

```
polymarket-signal-bot/
├── config.py           # All tunable parameters + env var loading
├── database.py         # SQLite schema and all persistence helpers
├── market_discovery.py # Gamma API polling, market registry
├── trade_monitor.py    # WebSocket feed + REST polling fallback
├── wallet_profiler.py  # Full wallet analysis (Data API + Etherscan V2 + Alchemy)
├── scorer.py           # Insider Confidence Score 0–100, documented weights
├── alerter.py          # Telegram message queue + HTML formatting
├── main.py             # Orchestrator (--dry-run flag here)
├── requirements.txt
├── .env.example
├── railway.toml
└── README.md
```

---

## Disclaimer

This bot is for informational and research purposes only. Large trades by wallets with high historical win rates may reflect luck, market making, hedging, or other non-insider activity. Nothing produced by this bot constitutes financial advice.
