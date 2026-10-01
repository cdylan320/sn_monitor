# ⚡ SN Monitor — real-time Bittensor subnet pump/dump alerts

Watches the alpha price of **every** subnet on every block and posts to Discord the moment
one pumps or dumps, e.g.

```
🚀 SN64 Chutes PUMP +20.00% · 0.004300 → 0.005160 τ · 36s
```

## How fast

Prices on Bittensor only change when a block lands (every 12 s), so polling in milliseconds
would only re-read the same number. Instead the monitor does the fastest thing possible
at each step:

| Step | How | Measured |
|---|---|---|
| Hear about a new block | Push subscription on **4 nodes at once**. The first node to deliver wins. | 0.3–0.8 s after the block is produced |
| Get all 129 prices | One `current_alpha_price_all` runtime call on the winning node, hedged to a second node. Decoded by hand. | ~50 ms (p95 ~110 ms) |
| Detect | O(1) sliding min/max per subnet and window | ~0.8 ms for all subnets |
| Notify | Discord connection kept warm (no DNS/TCP/TLS on alert) | ~150–300 ms |

**Early warning (mempool).** Big plaintext stakes and unstakes are seen *before* they
land, typically 3–11 s earlier. Each pending trade's exact price impact is computed with the
chain's own swap simulator. Backtested on real trades, the prediction error was
0.0001 percentage points on average. Limitation: trades sent through MEV Shield
(`submit_encrypted`) are encrypted until they execute, so those only alert once they land.

## What an alert shows

- **Push-notification line:** subnet, PUMP/DUMP, %, `old → new τ`, and how fast.
- **Embed:** green (pump) or red (dump) diff line with old → new price. Fields:
  - **Price** (τ and $)
  - **Move** (from which low/high, over how many blocks)
  - **Pool liquidity**
  - **Trend** (1m / 5m / 1h / 24h)
  - **Trades**: who bought or sold, how much, and the net flow. This is added by an edit about 1 s after the alert, so it never delays the alert.
- **Pending trades:** an amber `⏳ INCOMING BUY/SELL` alert.
  - All pending trades on one subnet within about 2 blocks share **one card**, edited in place. Bots often fire the same order 10–20× at once.
  - The card shows the tx count, the wallets, and the predicted move "if all N land" (capped by the orders' limit prices).
  - Its status updates to `✅ k landed (actual move …)` or `⌛ never included`.
- **Links:** subnet → taomarketcap.com/subnets/&lt;id&gt;, wallet → ss-ims.space/account/&lt;address&gt;.
- **Spacing:** every message starts with a blank line, so cards don't run together.
- **Footer:** block number and the measured detection latency.

## When it alerts

Default `WINDOWS=1:2,5:3,25:4,300:7`. A subnet alerts when it moves:

- ≥2% within 1 block (12 s)
- ≥3% within 1 minute
- ≥4% within 5 minutes
- ≥7% within 1 hour

Pumps are measured from the window's low, dumps from the window's high.

**No spam.** After an alert, that subnet alerts again only when the price is a further
3% away from what was last reported (`REALERT_STEP_PCT`). It is labelled
"continues" if it's the same direction, or "reversal" if it turned. On restart, the last hour
is backfilled silently, so you never get stale alerts.

All settings are listed in [.env.example](.env.example). Copy any you want into `.env`.

## Trend monitor (second channel)

This is a separate stream for sustained direction, as opposed to sharp moves: for example, a
subnet that slides 15% over two days with bounces along the way. It posts to
`TREND_WEB_HOOK_URL`.

**How a trend is measured.** On every block, every subnet is fitted with a log-linear trend
line over each timeframe. All subnets are done in one numpy pass, taking about 3–5 ms.

| Timeframe | Fitted on | A trend starts when |
|---|---|---|
| 12h | 30-min closes | trend line moves ≥8% and R² ≥ 0.70 |
| 24h | 30-min closes | ≥10% and R² ≥ 0.65 |
| 3d  | 1-hour closes | ≥12% and R² ≥ 0.60 |

- **R²** measures how cleanly price follows the line, so a single spike doesn't count as a trend.
- **Shape check:** the window's three thirds must step in the same direction, which rejects V and Λ shapes.
- **Ending a trend:** it ends only when the move halves or R² drops below 0.4. This hysteresis stops it flickering at the threshold.

These thresholds came from replaying 4–7 days of history for every subnet. They produce
about 20–25 cards/day across ~129 subnets. SN80's late-September slide registers as a
24h (−12%, R² 0.82) and 3d (−13%, R² 0.75) downtrend, and its recovery as a 24h uptrend.

**Cards.** There's one card per subnet and direction. If a longer timeframe confirms the
trend within 12h, the same card is updated in place. A trend the other way is posted as
`🔄 REVERSAL`. Each card shows:
- trend line from → to, with % and rate per hour
- strength (R²)
- pool liquidity
- current price against the price at the window start
- high and low over the window
- a table of all timeframes
- a **candlestick chart** with the trend window shaded and the fitted line drawn

**Charts are real.** Candle size depends on the timeframe:

| Card | Candles | Span |
|---|---|---|
| 12h | 30m | 36h |
| 24h | 1h | 72h |
| 3d | 2h | 6 days |

The candle size is printed on the chart.

- **Since the monitor started:** every candle is built from every block.
- **Older history:** the card first shows candles from 5-minute on-chain samples, drawn faded.
- **Exact rebuild:** a few seconds after posting, that subnet's history for the chart span is rebuilt from every block. It uses `state_queryStorage` on the history node (onfinality), from the pool reserves and weight.
- **Validation:** the rebuilt prices are checked against the chain's own price API, and on any mismatch nothing is written.
- **Swap-in:** the card's chart is then replaced with the exact one, and the exact bars are kept for later charts.

**History.** 5-minute OHLC bars for every subnet are kept in `data/bars.db` for 8 days:
- every 5-minute bar back to 3 days, then one per 15 minutes back to 7 days
- gaps are refilled every 15 minutes, newest first
- a restart only fetches what's missing
- trends already under way at startup are noted silently

## Run

```bash
./start.sh                      # start/restart under pm2 (creates the venv if needed)
pm2 logs sn-monitor             # live console: every block, latency, alerts
```

Other commands:

```bash
.venv/bin/python -m snmon test        # post sample alerts to Discord to see the design
.venv/bin/python -m snmon trend-test  # post a sample trend card (strongest live trend) to the trend channel
.venv/bin/python -m snmon replay 900  # dry-run detection over the last 900 blocks (no Discord)
DRY_RUN=true .venv/bin/python -m snmon  # live, console only
.venv/bin/python tests/test_detector.py
```

Every alert is also appended to `data/alerts.jsonl`.

## Layout

```
snmon/rpc.py       raw websocket JSON-RPC client (reconnects, resubscribes)
snmon/chain.py     hot path: header race, price decode, block hash from header, swap simulator
snmon/detector.py  sliding-window pump/dump detection + episodes
snmon/alerts.py    Discord embeds + console lines
snmon/discord.py   warm-connection webhook client, rate limits, edits
snmon/mempool.py   pending-trade decoding + exact impact prediction
snmon/meta.py      names/logos/liquidity, block events → trades (worker thread)
snmon/bars.py      5-minute OHLC bar store (numpy ring + SQLite), history backfill
snmon/trend.py     trend fits, per-timeframe state machine, stories, trend cards
snmon/chart.py     candlestick chart PNG for trend cards
snmon/app.py       wiring, backfill, 24h reference, console UI
```
