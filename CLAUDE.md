# Working notes for Claude (sn_monitor + sn_trader)

Two projects, two repos, worked on together. Sessions start in `/root/draven/sn_monitor`.

| | Repo | Path | Runs where |
|---|---|---|---|
| **sn_monitor**: Bittensor subnet monitor → Discord | `cdylan320/sn_monitor` | `/root/draven/sn_monitor` | the dev VPS, pm2 app `sn-monitor` (`./start.sh`, `pm2 logs sn-monitor`) |
| **sn_trader**: auto staking bot (package `sntrader`) | `cdylan320/trading-bot` (private) | `/root/draven/sn_trader` | **LIVE on the user's other machine** (`/root/.bittensor/trading-bot`), one screen per coldkey: `./bot.sh g2`, `./bot.sh funfun` |

## How we work
- **Push everything to GitHub.** The trading machine only gets changes by `git pull` + restart. Always `git fetch` first:
  the user's other machine (Cursor) also commits to the trader repo.
- **`.env.example` must mirror the values actually run.** After any setting change, update `.env.example` and the code
  default and tell the user which lines to change in `.env.g2` / `.env.funfun` (their files override the defaults).
- **Never print secrets** (`.env` values, webhook URLs, tokens, keys). **Never send a real transaction** from here:
  verify read-only (state_call / validate) or in paper mode (`DRY_RUN=true`, throwaway env, delete it after).
- **Stakes are ALWAYS sealed by MEV shield; no fallback to the open mempool, ever.** Unstakes go in the open. (The user lost
  TAO to a sandwich on an unsealed stake.)
- Speed and clear UX first. Explain in plain, short English; the user is not a native speaker.
- When the chain behaves unexpectedly: read the chain source (`RaoFoundation/subtensor`, node code in
  `RaoFoundation/polkadot-sdk`) and check read-only on chain before guessing.
- `sntrader/sandwich.py` / sandwich mode was written by others and is not ours to extend. Its 2 tests in
  `tests/test_sandwich.py` fail on their code — that is the known baseline.

## Tests
- Monitor: `for f in tests/test_*.py; do .venv/bin/python $f; done` (no pytest in its venv).
- Trader: `cd ../sn_trader && .venv/bin/python -m pytest -q` (all pass except the 2 sandwich tests).

## The manager UI (trader repo, `snui/`)
`./ui.sh` on the trading machine: a local web board (port 8790) that starts / stops the bots, toggles trades, edits
settings, adds / removes wallets' bots, and keeps the global skip list (`data/skip_netuids.json`, re-read by every bot).

## The link between the two
The monitor serves its news to the bots: `GET http://<monitor host>:8795/news` (SSE, `NEWS_FEED_PORT` /
`NEWS_FEED_TOKEN` in the monitor's `.env`). The bots' `NEWS_FEED_URL` holds this host's IP, so **if the monitor moves to
another host, the bots' `NEWS_FEED_URL` must be changed** (same token).

## News sources
- Subnet X accounts: fxtwitter (one account per request, each ~every 6 s). A private X list read with the user's X
  login (`snmon/xlist.py`, `NEWS_X_LIST`, account @cdylan321) exists but is OFF: X adds posts to a list 6-14 s late
  (median ~9.5 s); fxtwitter was first 10 of 12 times (~5 s), measured 21-22 Oct.
- Subnet sales: X search with that login (`acquisition_loop`, every 6 s, ~12 s index lag): a post that says "acquired"
  and names a subnet → AI check → 🤝 card, `acquisition` event (the bots trade it like a rename), the buyer's account
  becomes the subnet's X account (kept until its identity changes on chain). An identity / owner change on chain also
  re-finds the subnet's X account at once.
- Buybacks (`snmon/buyback.py`, on chain, every block): add_stake_burn (event AddStakeBurn), the owner's coldkey
  buying its own subnet, a named buyback wallet (`NEWS_BUYBACK_WALLETS`), or buy + burn in one block → 💰 card and a
  `buyback` event (the bots' own buyback trade, run like a rename). Also a hidden buyback (user's rule): one wallet's
  buy ≥ 50 τ (new money) paid by the owner (directly / one hop) or an exchange / bridge (named, or 5,000+ transactions
  paying 20+ wallets), by a new quiet wallet (≤ 50 transactions) that did not sell that subnet in 24 h — `snmon/funding.py`,
  transfers and sells kept 7 days in `data/transfers.db`. 6-hour dry run 23 Oct: 7 of 95 big buys (SN63's buyer B ×4).
- Discord: a user account's token (`NEWS_DISCORD_TOKEN`, now vic_x.; the first one was removed from the server on 21 Oct).
- Every post is judged before it goes to the bots (`snmon/judge.py`): gpt-6.1-sol ×2 raced + gpt-6-luna, whose
  "important" goes out at once. const is never left to the AI: everything he writes in a subnet's Discord channel (by his
  user id, `NEWS_CONST_DISCORD_IDS`), his X posts / reposts / replies, and a subnet quoting or sharing him = important.
  A subnet's own X account's post / quote (its owner) is always important too (`NEWS_X_SUBNET_IMPORTANT`).

## Trading rules (the user's decisions; `README.md` and `.env.example` of sn_trader are the reference)
- New subnet (start call): sells everything, stakes all (`LAUNCH_MAX_STAKE_TAO=0`), sent in the block after the start call
  is seen (same block when the start call is visible in the mempool). Exit: no floor; not acted on in the start call's
  block and the next 5 (`LAUNCH_GRACE_BLOCKS`; SN59: ×3, then −21%, −17%, −17%, +3%, +8%); then a trail from the high of
  8% from +6, 5% from +10, 2% from +12 (`LAUNCH_TRAIL_STEPS`); after those 5 blocks, pending unstakes in the mempool that
  would break the trail sell it at once, in the same block (`LAUNCH_MEMPOOL_EXIT`); the owner unstaking any alpha
  sells it at once, every other rule aside (`LAUNCH_OWNER_EXIT`).
- Coldkey swap (subnet sale): all-in like a new subnet (`SWAP_MAX_STAKE_TAO=0`). SPEED (23 Oct): announcements are read
  every 25 blocks and an announced, runnable swap has its sell-and-stake pre-signed every block — a mempool sighting is
  a broadcast alone, NO chain read after it (checked against the announcement in memory; none in memory = traded
  unchecked). SN113: seen 3 s before its block, built on the spot, the stake landed a block late. Exit 1%.
- const post, rename (name change, or a new/changed GitHub link or website — `RENAME_LINKS`; also a subnet SOLD, its
  buyer's X post found by the monitor's X search — news kind `acquisition`, bought like a rename): up to 10 τ from the free
  balance (everything sold first only if short). Exit 1%; also after 20 blocks with no new high (10 once it rose) — every trade has this, swap / new subnet / rename too (23 Oct). A const post older than 15 s is never acted on (old posts came back as new: SN97, 11 trades). const's first 3 blocks after the stake are not acted on (`CONST_GRACE_BLOCKS`).
- Reversal, dump (≥70 τ SOLD in 3 blocks — sales only, other coldkeys' buys don't cancel them, a coldkey's own stake does — moving the price ≥1% by themselves, or ≥5% drop alone; a dump is bought like const — up to 10 τ, stakes the bot did not open sold first when short; a running trade is never sold for it —; dump and pump trades are SHORT (23 Oct): sold at once when up 1% on any pool size (`DUMP_` / `PUMP_TAKE_PROFIT_PCT`), and at the first block without a new high (`DUMP_` / `PUMP_EXIT_BLOCKS` = 1); a CROWD (coldkeys moving ≥5 τ net in one block — 3+ buyers, 2+ sellers): a crowd pump is not bought, a crowd dump is bought once a block passes without a seller (≤10 blocks) — `PUMP_MAX_BUYERS`, `DUMP_MAX_SELLERS`, 23 Oct; a PUMP (≥30 τ BOUGHT in 3 blocks, net per coldkey, +1% by the buys, or +5% alone — `PUMP_*`, 23 Oct) is the mirror: bought right behind the buyer, sized, sold and taking its small-pool profit like a dump (a subnet opened < 1 h ago is not a pump; most stakes are sealed, so most pumps are seen when they land); every trade sells at once when unstakes pending in the open would break its selling level (`MEMPOOL_EXIT`, its unstake lands in the same block), a bigger pool is left to run; into a pool under `SMALL_POOL_TAO` a dump / news / const / rename trade stakes at most 2 τ × pool / 5,500 τ, never under 0.5 τ (`SMALL_POOL_MIN_STAKE_TAO`); also from the mempool), news (from the monitor's feed, incl. `x_subnet`: a subnet's own X post, and `buyback`: a team buying its token back on chain; only news the monitor's
  judge calls important — `snmon/judge.py`, an AI via OpenRouter (or Claude directly) or keyword rules; important news is bought like const — up to 10 τ, stakes the bot did not open sold first when short; a running trade is never sold for it —; news already traded (same X post or same text, same subnet) is not traded again for 24 h; never a share of an X post over 10 min old): free balance; dump, news, const and rename take ≤2 τ in pools
  under 5,500 τ (traded, not skipped; launches and swaps not capped); exit 1% stop, 1% trail, or no new high for 20 blocks (10 once it made a new high — `NEW_HIGH_EXIT_BLOCKS`).
- const: a post naming no subnet but replying to / mentioning someone is matched through that person's X profile (a
  subnet owner's bio names the subnet: @0xarrash → SN8).
- Never buy the 3 subnets the chain would remove first (`PRUNE_RISK_COUNT`). Buys: tolerance 16% → 32%; all-in buys
  `URGENT_TOLERANCE_PCT=1000`; unstakes have no price limit. Tips buy nothing (the chain orders by arrival).

## Chain facts learned the hard way
- MEV shield: the wrapper's era must be anchored ≥2 blocks behind the head, or the author includes it undecrypted.
  Sealed late in a slot (>6 s) → sealed for the block after (a PendingKey miss locks the nonce ~8 blocks).
- Every normal transaction has the same priority; a block orders them by arrival.
- Removal: the non-immune (60 days) subnet with the lowest moving price goes at the next registration (one per 48 h).
- All node requests share one connection: a failover cancels the others' requests — `node._try` retries them, and every
  trader loop is supervised.
