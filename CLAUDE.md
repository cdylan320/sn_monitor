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

## The link between the two
The monitor serves its news to the bots: `GET http://<monitor host>:8795/news` (SSE, `NEWS_FEED_PORT` /
`NEWS_FEED_TOKEN` in the monitor's `.env`). The bots' `NEWS_FEED_URL` holds this host's IP, so **if the monitor moves to
another host, the bots' `NEWS_FEED_URL` must be changed** (same token).

## News sources
- Subnet X accounts: fxtwitter (one account per request) AND a private X list read with the user's X login
  (`X_AUTH_TOKEN` / `X_CT0` = their x.com cookies, account @cdylan321 — the user chose this knowing X may lock it;
  `snmon/xlist.py`, list + members in `data/x_list.json`). If the login is refused, the list stops and fxtwitter goes on.
- Discord: a user account's token (`NEWS_DISCORD_TOKEN`, now vic_x.; the first one was removed from the server on 21 Oct).
- Every post is judged before it goes to the bots (`snmon/judge.py`): gpt-6.1-sol ×2 raced + gpt-6-luna, whose
  "important" goes out at once.

## Trading rules (the user's decisions; `README.md` and `.env.example` of sn_trader are the reference)
- New subnet (start call): sells everything, stakes all (`LAUNCH_MAX_STAKE_TAO=0`), sent in the block after the start call
  is seen (same block when the start call is visible in the mempool). Exit: no floor; not acted on in the start call's
  block and the next 5 (`LAUNCH_GRACE_BLOCKS`; SN59: ×3, then −21%, −17%, −17%, +3%, +8%); then a trail from the high of
  8% from +6, 5% from +10, 2% from +12 (`LAUNCH_TRAIL_STEPS`); after those 5 blocks, pending unstakes in the mempool that
  would break the trail sell it at once, in the same block (`LAUNCH_MEMPOOL_EXIT`); the owner unstaking any alpha
  sells it at once, every other rule aside (`LAUNCH_OWNER_EXIT`).
- Coldkey swap (subnet sale): all-in like a new subnet (`SWAP_MAX_STAKE_TAO=0`); a pending swap is checked against the
  owner's announcement first (a swap the chain will refuse is not traded). Exit 1%.
- const post, rename (name change, or a new/changed GitHub link or website — `RENAME_LINKS`): up to 10 τ from the free
  balance (everything sold first only if short). Exit 1%; const also after 25 blocks with no new high (10 once it rose). A const post older than 15 s is never acted on (old posts came back as new: SN97, 11 trades). const's first 3 blocks after the stake are not acted on (`CONST_GRACE_BLOCKS`).
- Reversal, dump (≥70 τ SOLD in 3 blocks — sales only, other coldkeys' buys don't cancel them, a coldkey's own stake does — moving the price ≥1% by themselves, or ≥5% drop alone; a dump is bought like const — up to 10 τ, stakes the bot did not open sold first when short; a running trade is never sold for it —; also from the mempool; never in a pool under 5,500 τ —
  `DUMP_MIN_POOL_TAO`), news (from the monitor's feed, incl. `x_subnet`: a subnet's own X post; only news the monitor's
  judge calls important — `snmon/judge.py`, an AI via OpenRouter (or Claude directly) or keyword rules; important news is bought like const — up to 10 τ, stakes the bot did not open sold first when short; a running trade is never sold for it —; news already traded (same X post or same text, same subnet) is not traded again for 24 h; never a share of an X post over 10 min old): free balance, ≤5 τ in
  pools under 5,000 τ; exit 1% stop, 1% trail, or no new high for 25 blocks (10 once it made a new high — `NEW_HIGH_EXIT_BLOCKS`).
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
