# ⚡ SN Monitor — real-time Bittensor subnet pump/dump alerts

Watches the alpha price of **every** subnet on every block and posts to Discord the moment
one pumps or dumps, e.g.

```
🚀 SN64 Chutes PUMP +20.00% · 0.004300 → 0.005160 τ · 36s
```

## Channels

| `.env` variable | Channel | What posts there |
|---|---|---|
| `PRICE_HOOK_URL` | price | pump/dump alerts, pending-trade (mempool) alerts |
| `TREND_WEB_HOOK_URL` | trend | trend cards, reversal signals, the live trend board |
| `NEWS_WEB_HOOK_URL` | news | subnet-channel news, X posts, **subnet lifecycle alerts** |

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

## Subnet lifecycle alerts

These alerts cover changes to the subnets themselves, such as slots, owners and names. They post
in the **news channel** (`NEWS_WEB_HOOK_URL`), or in their own if `SUBNET_WEB_HOOK_URL` is set. They're read from chain
events on every block, about 1 s after the block:

| Card | Chain event |
|---|---|
| ⏳ New subnet incoming | `NetworkRegistrationQueued`: name, description, who, TAO locked, which slot. About **4 minutes before** the subnet exists |
| 💀 Subnet deregistered | `NetworkRemoved`: last price, pool, owner, lifetime, and what it made room for |
| 🆕 New subnet registered | `NetworkAdded`: owner, hotkey, price, pool, whose slot it took |
| ⚠️ Dissolve scheduled | `DissolveNetworkScheduled` |
| 🔑 Owner coldkey swap | `ColdkeySwapAnnounced` / `Swapped` / `Disputed` / `Reset` / `Cleared`, only for coldkeys that own a subnet |
| 👑 Owner changed, 🗝️ owner hotkey changed | `SubnetOwnerChanged`, `SubnetOwnerHotkeySet` |
| ✏️ Renamed / identity changed, 🔤 symbol | `SubnetIdentitySet`, `SymbolUpdated`, shown as a before/after diff |
| 🚀 Emissions start, 📜 lease, 🧮 slot limit | `FirstEmissionBlockNumberSet`, `SubnetLease*`, `SubnetLimitSet` |
| ⚠️ Next in line to be deregistered | polled; announced once a new candidate has held for 10 minutes |

A registration is one story told over several blocks. In one block the bottom subnet is pruned and
the newcomer is queued. A few minutes later, when cleanup finishes, the newcomer is added, and its
own hotkey and identity events are folded into that card. The cards were verified by replaying the
real SN116 slot change (blocks 9210610 → 9210632).

As a safety net, after every metadata refresh (each minute) the subnet list is compared with the
last reported view, which is kept in `data/subnets.json`. A change whose event was missed, including
during downtime, is still announced. A re-registered slot also resets that netuid's price, trend and
signal history, and reversal-signal cards flag subnets under 7 days old.

## Trend monitor (second channel)

This is a separate stream for sustained direction, as opposed to sharp moves: for example, a
subnet that slides 15% over two days with bounces along the way. It posts to
`TREND_WEB_HOOK_URL`.

**How a trend is measured.** On every block, every subnet is fitted with a log-linear trend
line over each timeframe. All subnets are done in one numpy pass, taking about 3–5 ms.

| Window | Fitted on | On the trend board when the line moves | Card posted when it moves |
|---|---|---|---|
| 1h  | 5-min closes  | ≥2%, steadiness (R²) ≥ 0.85 | ≥4%, R² ≥ 0.90 |
| 3h  | 5-min closes  | ≥3.5%, R² ≥ 0.80 | ≥5%, R² ≥ 0.85 |
| 6h  | 10-min closes | ≥6%, R² ≥ 0.80 | same |
| 12h | 30-min closes | ≥8%, R² ≥ 0.70 | same |
| 24h | 30-min closes | ≥10%, R² ≥ 0.65 | same |
| 3d  | 1-hour closes | ≥12%, R² ≥ 0.60 | same |

The short windows show on the board earlier than they post a card. That way the board shows what's
moving right now, while the channel only gets cards for the bigger short-term moves.

- **R²** measures how cleanly price follows the line, so a single spike doesn't count as a trend.
- **Shape check:** the window's three thirds must step in the same direction, which rejects V and Λ shapes.
- **Ending a trend:** it ends only when the move halves or R² drops below 0.4. This hysteresis stops it flickering at the threshold.

These thresholds came from replaying 4–7 days of history for every subnet. They produce
about 20–25 cards/day across ~129 subnets. SN80's late-September slide registers as a
24h (−12%, R² 0.82) and 3d (−13%, R² 0.75) downtrend, and its recovery as a 24h uptrend.

**Pullbacks and bounces.** When a short timeframe turns against a longer trend that's still
running, the card says so instead of calling it a reversal. Example: `↘️ PULLBACK · 12h DOWNTREND
in a 3d UPTREND · still +4.17% over 3d`.

**Reversal signals ("dump → pump starting").** A signal fires when the **1h window turns up
after a drop of ≥5% from the 24h high**, at most once per subnet every 12 hours. This rule was chosen
by backtest on 8 days of 5-minute prices for every subnet (2026-10-05), not by intuition:

| Entry rule | Signals/day | Up after 24h | Median after 24h |
|---|---|---|---|
| Buy at random | — | 39% | ~0% (mean +0.3%) |
| 1h turns up, no dump before | 11 | 51% | (mean +1.4%) |
| **1h turns up after a ≥5% dump, 12h spacing (the signal)** | **~4** | **~63–66%** | **+2.3%** (mean +3.4%) |
| Waiting for the 3h window to confirm | 17 | 42% | (mean −0.7%) |

It's an edge, not a guarantee:
- About 4 signals in 10 lose.
- The typical dip along the way is −3%; the worst was −7% and the best +25%.
- The sample is one week.

So every signal card carries:
- **Entry**, the **invalidation level** (the dump's low) and the **previous high**.
- **Cost to buy** for 10/50/100 τ (slippage + fee, from the chain's swap simulator). On a τ1.5K pool, a 100 τ buy costs ~6–7%, more than the edge.
- **Pool size**, the last hour's **buy/sell flow** and the state of every **trend window**.
- A **chart** with the levels marked.
- The **track record**: the backtest (re-run on stored history every 6 hours) and the live record of signals actually posted.

The board also shows a **👀 Watching** list under the signals: subnets that have dumped ≥5% from
their 24h high, haven't turned up yet, and haven't signalled in the last 12 hours. The closest to a
signal come first. `ready` is the weakest of the three things a signal needs (the 1h trend line's
move, its steadiness, and rising through the hour), so 100% means the signal fires.

Each signal is then tracked for 24 hours. The card is edited with the result at +1h, +6h and +24h,
and if the price closes below the invalidation level. The board lists the last 24 hours of signals,
each with its % since entry. `TREND_SIGNALS=false` turns signals off, and `TREND_SIGNAL_DUMP_PCT`
sets the dump size.

**Live trend board.** One message always sits at the bottom of the trend channel, listing every
subnet that's trending right now:
- each subnet gets a ▲/▼ strip across all 5 timeframes, plus the real move over its longest trend
- it refreshes every minute and is posted silently
- when new cards push it up, it moves itself back to the bottom
- `TREND_BOARD_SECONDS` sets the refresh interval, and 0 turns it off

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

## News monitor (third channel)

This channel watches every subnet channel on the Bittensor Discord, where the channel name
`78 · umi` maps to SN78, and posts news to `NEWS_WEB_HOOK_URL`. Three kinds of post count:

| | What |
|---|---|
| 📢 Announcement | a post that pings @everyone / @here |
| 🐦 X post | a link to an x.com / twitter.com post (each tweet is reported once, even if it's shared in several channels) |
| 📰 Team update | a post by the subnet's team or server staff that has a link or attachment, is long (≥120 chars), or uses announcement words like *release, launch, enrollment, mainnet, upgrade…* |

Short replies and support chatter are skipped.

**Who counts as "the team"** comes from Discord itself: the users and roles that the channel's
permission overwrites give moderation rights. That's how subnet owners are set up in their own
channels. Server-wide staff roles count too.

**Each card shows:**
- the subnet (with its logo), the author and their role
- the full text, with links that open the original message
- a native preview of the tweet or GitHub link
- the **price at the post**
- the **market reaction**, edited in at +5m, +15m and +1h

**How messages arrive.** In real time over Discord's gateway. A light sweep every 20s acts as
a safety net, and after a restart, posts from the last 30 minutes are caught up.

**Account:** a separate alt account that's a member of the server. Its token goes in
`NEWS_DISCORD_TOKEN`. Automating a user account breaks Discord's terms, so the alt is what's at
risk, not your main account. To check the login and preview what would be posted:
`.venv/bin/python -m snmon news-test 78` (add `--post` to send a TEST card).

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
snmon/lifecycle.py subnet lifecycle: registrations, removals, owners, identities, prune candidate
snmon/signals.py   reversal signals: rule, cards, 24h outcome tracking, built-in backtest
snmon/chart.py     candlestick chart PNG for trend and signal cards
snmon/gateway.py   read-only Discord user client (REST + gateway) for the news monitor
snmon/news.py      news classification, team detection, news cards, market reaction
snmon/app.py       wiring, backfill, 24h reference, console UI
```
