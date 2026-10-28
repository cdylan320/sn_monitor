"""Is a news post about a subnet important — something that can move its price now?

The trading bots buy on news (sn_trader news trading). 21 Oct: of 21 news trades only 2 made money; most "news" was
routine — answers to users, dev notes, shares of old tweets (SN124: a team member shared a 2-day-old quarterly update;
the bot bought, nothing moved). So each post is judged before it is sent to the bots: important = a product launch, big
progress, funding / a new investor, revenue / customers / contracts, a notable partnership, a listing, a buyback / burn.

An AI reads the post (and the X post it shares): through OpenRouter when OPENROUTER_API_KEY is set in .env (default
model openai/gpt-6.1-sol, low thinking effort: ~$0.003 a post), else Claude directly when ANTHROPIC_API_KEY is set; with
neither, or when it does not answer in NEWS_AI_TIMEOUT seconds, keyword rules decide. The verdict goes to the bots with
the news and onto the card.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field

log = logging.getLogger("snmon.judge")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = {"openrouter": "openai/gpt-6.1-sol", "anthropic": "claude-opus-5-5"}
# asked next to the main model: its "important" goes to the bots at once, its "not important" waits for the main model.
# 21 Oct, 21 real posts × 2: gpt-6-luna without thinking answers in 1.3 s (gpt-6.1-sol 2.5 s, up to 5 s) and agrees on 39
# of 42, but it missed 2 of the 5 important posts — alone it would lose trades. gpt-oss-120b (0.3 s) and Mercury (0.6 s)
# call too much important.
FAST_MODEL = "openai/gpt-6-luna"

CATEGORIES = ["launch", "progress", "funding", "revenue", "partnership", "listing", "tokenomics", "other", "none"]

SYSTEM = """You judge news about one Bittensor subnet for a trading bot. The bot buys the subnet's token the moment \
important news appears and sells within minutes, so "important" means news that would make MANY traders buy this token \
within minutes. That is rare: far fewer than 1 post in 10 is important. When in doubt, it is NOT important.

Important only when it is NEW, CONCRETE and happening NOW (the categories to answer with):
- launch: a new product, app, API, portal or mainnet that is released and live today ("NEW RELEASE", "is live now",
  "launched today") — it counts even when the same post also promotes it (free credits, sign-ups, a demo) — but not
  testnet, not "coming soon", not a date in the future, not a model or code uploaded to a repo, not a new competition
  round or challenge
- funding: money raised with an amount or a named investor, or a grant from a well-known organisation
- revenue: revenue numbers, a named paying customer, or a signed commercial contract
- partnership: a partnership or integration with a WELL-KNOWN company or project (big tech, a major AI lab, a top crypto
  project or exchange) — not small ecosystem, creator or community collaborations. Also a NEW deal between two Bittensor
  subnets: the subnet putting its own product to work on another subnet (e.g. "has launched an open competition on
  @Apex_SN1" to improve its own code, paid in that subnet's alpha) or another subnet starting to use it — only the FIRST
  announcement of the deal (it counts even when it starts within a day), not its later rounds, results or reminders
- listing: listed on a major exchange
- tokenomics: a buyback, a burn, or the owner / team cutting their own emissions or selling less
- progress: only an outstanding first — a record that beats the big labs or the whole industry, confirmed by others
- other: a rare event of the same weight, e.g. the subnet being acquired or sold to a new owner

NOT important, however it is worded:
- marketing or promotion of a product that already exists (no new release in the post): speed, price, quality,
  "faster / cheaper / better than competitors", referral or "earn" programs, side hustles, calls to use or try it
- usage, traffic or growth figures (tokens processed, requests, users, decisions per second), milestones of that kind
- competition and challenge news of the subnet's own miners: new rounds, challenges, scores, champions, benchmarks of
  miners' submissions, rules, evaluation changes, leaderboards (but a deal with another subnet, above, is important)
- testnet releases, plans, roadmaps, upcoming or scheduled launches and changes, teasers, "big things coming"
- research results, papers, model or dataset uploads, demos
- recruiting miners, validators or GPU providers; capacity or demand claims
- replies, support, bug fixes, maintenance, downtime, instructions, routine dev or weekly updates
- reminders, recaps, summaries of known facts, re-shares of older posts, events, AMAs, spaces, podcasts, hiring, memes

If the shared X post is older than a few hours, the market already knows it: not important.
Answer with the category ("none" when it is not important) and a reason of at most 15 words."""

SCHEMA = {
    "type": "object",
    "properties": {
        "important": {"type": "boolean"},
        "category": {"type": "string", "enum": CATEGORIES},
        "reason": {"type": "string"},
    },
    "required": ["important", "category", "reason"],
    "additionalProperties": False,
}

# a subnet sold: found by X search from any account (23 Oct: @KommsApp "has officially acquired SN27!" at 12:00, the pump
# at 12:09, the on-chain rename only at 13:53) — asked only about posts that name a subnet and say "acquired" or the like
ACQ_SYSTEM = """You read one X post. Decide whether it OFFICIALLY ANNOUNCES that a Bittensor subnet (named by its number: \
"SN27", "subnet 27", "netuid 27") now has a new owner — the author's own team or company acquired / bought / took over \
that subnet. NOT an acquisition: rumours, questions, jokes, wishes ("someone should buy SN27"), congratulations or \
commentary by others, an acquisition of something else (a company, a domain), a subnet buying a product, or a subnet \
registering a new slot. Answer the subnet's number (0 when there is none or it is not an acquisition), the new owner's \
name, and a reason of at most 12 words."""
ACQ_SCHEMA = {
    "type": "object",
    "properties": {
        "acquisition": {"type": "boolean"},
        "netuid": {"type": "integer"},
        "acquirer": {"type": "string"},
        "reason": {"type": "string"},
    },
    "required": ["acquisition", "netuid", "acquirer", "reason"],
    "additionalProperties": False,
}

# keyword rules: the fallback when Claude is not set up or does not answer in time
RULES = [
    ("launch", r"\b(launch(ed|es|ing)?|released?|is (now )?live|now live|went live|live now|mainnet|v\d+(\.\d+)* (is )?(out|live)|introducing|now available|shipped)\b"),
    ("funding", r"\b(raised?|raising|funding|funded|seed round|series [a-c]|investment|investors?|backed by|grant)\b"),
    ("revenue", r"\b(revenue|income|paying customers?|customers?|contracts? signed|signed (a |two |three )?(deal|contract)s?|commercial (deal|agreement)s?|arr\b|mrr\b)"),
    ("partnership", r"\b(partner(ship|ed|s)?( with)?|integrat(ed|ion|es) (with|into)|collaborat(ion|ing) with|teamed up)\b"),
    ("listing", r"\b(listed on|listing on|now trading on|exchange listing)\b"),
    ("tokenomics", r"\b(buy ?backs?|burn(ed|ing|s)?|owner (cut|reduc)\w*|emission(s)? (cut|reduc)\w*)\b"),
    ("progress", r"\b(record|milestone|all[- ]time high|beat(s|ing)? (the )?(sota|state of the art|benchmark)|surpass(ed)?|\d+[kmb]?\+? (users|downloads|requests|calls))\b"),
]
NOT_NEWS = re.compile(r"^\s*(@\w+\s+)+|^\s*(yes|yeah|no|thanks|thank you|hi|hey|gm|lol)\b|\b(reminder|recap|ama|spaces?|podcast|hiring|we'?re hiring)\b", re.I)


@dataclass
class Verdict:
    important: bool
    category: str
    reason: str
    by: str              # "ai" | "rules"
    model: str = field(default="", compare=False)   # the AI model whose answer this is

    def event(self) -> dict:
        return {"important": self.important, "category": self.category, "why": self.reason, "judge": self.by}


def _answer(task: asyncio.Task) -> Verdict | None:
    """A finished request's verdict; None if it failed, was cancelled or is still running."""
    if not task.done() or task.cancelled() or task.exception() is not None:
        return None
    return task.result()


def rules(text: str, tweet_age_s: float | None = None) -> Verdict:
    """Keyword rules: a post counts only when it names a category and is not a reply, a reminder or old news."""
    if tweet_age_s is not None and tweet_age_s > 3 * 3600:
        return Verdict(False, "none", f"the shared X post is {tweet_age_s / 3600:.0f} h old", "rules")
    if NOT_NEWS.search(text or ""):
        return Verdict(False, "none", "a reply, reminder or event — not news", "rules")
    for cat, rx in RULES:
        m = re.search(rx, text or "", re.I)
        if m:
            return Verdict(True, cat, f"mentions “{m.group(0).strip()}”", "rules")
    return Verdict(False, "none", "no launch, funding, revenue, partnership or listing in it", "rules")


class NewsJudge:
    """One verdict per post (cached by its text); an AI when one is set up (OpenRouter, or Claude directly), the rules
    otherwise."""

    def __init__(self, model: str = "", timeout: float = 8.0, client=None, session=None, race: int = 1,
                 fast_model: str = "") -> None:
        self.timeout = timeout
        self.race = max(1, race)                 # identical requests sent at once; the first good answer is used
        self.client = client                     # AsyncAnthropic (Claude directly)
        self.session = session                   # aiohttp session (OpenRouter)
        self.openrouter_key = "" if client is not None else (os.getenv("OPENROUTER_API_KEY") or "").strip()
        self.provider = ("openrouter" if self.openrouter_key or session is not None
                         else "anthropic" if client is not None or os.getenv("ANTHROPIC_API_KEY") else "")
        self.model = model or DEFAULT_MODEL.get(self.provider, "")
        self.fast_model = fast_model if self.provider == "openrouter" and fast_model != self.model else ""
        if self.client is None and self.provider == "anthropic":
            try:
                import anthropic
                self.client = anthropic.AsyncAnthropic(max_retries=0)
            except ImportError:
                log.warning("ANTHROPIC_API_KEY is set but the anthropic package is not installed "
                            "(.venv/bin/pip install -r requirements.txt) — judging news by keyword rules")
        self._cache: dict[str, Verdict] = {}
        self.stats = {"ai": 0, "rules": 0, "ai_fail": 0, "cost": 0.0, "fast": 0, "fast_agreed": 0, "fast_overruled": 0}

    @property
    def ready(self) -> bool:
        return self.provider == "openrouter" or self.client is not None

    @property
    def mode(self) -> str:
        if self.provider == "openrouter":
            return (f"{self.model} via OpenRouter" + (f" ({self.race} requests raced)" if self.race > 1 else "")
                    + (f" + {self.fast_model} (its 'important' goes out at once)" if self.fast_model else ""))
        if self.client is not None:
            return f"Claude ({self.model})"
        return "keyword rules (no OPENROUTER_API_KEY / ANTHROPIC_API_KEY)"

    async def judge(self, subnet: str, text: str, tweet_text: str = "", tweet_age_s: float | None = None) -> Verdict:
        key = f"{subnet}\n{text}\n{tweet_text}"
        if key in self._cache:
            return self._cache[key]
        whole = "\n\n".join(x for x in (text, tweet_text) if x)
        v = None
        if self.ready:
            try:
                v = await asyncio.wait_for(self._decide(subnet, text, tweet_text, tweet_age_s), self.timeout)
            except Exception as e:     # timeout, API error, refusal, bad answer: the rules decide
                self.stats["ai_fail"] += 1
                log.warning("news judge: Claude gave no verdict (%s: %s) — using the keyword rules", type(e).__name__, str(e)[:120])
        if v is None:
            v = rules(whole, tweet_age_s)
        self.stats[v.by] += 1
        self._cache[key] = v
        if len(self._cache) > 500:
            self._cache.pop(next(iter(self._cache)))
        return v

    async def _decide(self, *args) -> Verdict:
        """The main model decides — unless the fast model calls the news important first: that goes to the bots at once,
        and the main model's later answer is only compared (logged). The fast model's 'not important' waits for the main
        model, which catches what the fast one misses. If the main model fails, the fast model's answer is used."""
        ask = self._ask_openrouter if self.provider == "openrouter" else self._ask
        if not self.fast_model:
            return await self._first(ask, *args)
        main = asyncio.create_task(asyncio.wait_for(self._first(ask, *args), self.timeout))
        fast = asyncio.create_task(self._ask_openrouter(*args, model=self.fast_model))
        kept = False
        try:
            await asyncio.wait({main, fast}, return_when=asyncio.FIRST_COMPLETED)
            if not main.done():
                quick = _answer(fast)
                if quick is not None and quick.important:
                    self.stats["fast"] += 1
                    main.add_done_callback(lambda t: self._second_opinion(quick, t))
                    kept = True                  # the main model's answer still comes, for the log
                    return quick
            try:
                return await main
            except asyncio.CancelledError:
                raise
            except Exception:
                if not fast.done():
                    await asyncio.wait({fast})
                quick = _answer(fast)
                if quick is None:
                    raise
                return quick
        finally:
            if not fast.done():
                fast.cancel()
            if not kept and not main.done():
                main.cancel()

    def _second_opinion(self, quick: Verdict, task: asyncio.Task) -> None:
        main = _answer(task)
        if main is None:
            return
        if main.important:
            self.stats["fast_agreed"] += 1
        else:
            self.stats["fast_overruled"] += 1
            log.warning("news check: %s called it important (%s), %s says not (%s) — the bots got the fast answer",
                        self.fast_model, quick.reason, self.model, main.reason)

    async def _first(self, ask, *args) -> Verdict:
        """`race` identical requests at once, the first good answer wins and the others are cancelled: one slow answer no
        longer holds the bots up (21 Oct, 6 real posts × 2: median 2.9 → 2.4 s, slowest 5.7 → 4.1 s; SN79's 5.1 s check
        made its stake miss a block)."""
        if self.race <= 1:
            return await ask(*args)
        tasks = [asyncio.create_task(ask(*args)) for _ in range(self.race)]
        try:
            err: Exception | None = None
            for done in asyncio.as_completed(tasks):
                try:
                    return await done
                except asyncio.CancelledError:
                    raise
                except Exception as e:           # this one failed: wait for the next
                    err = e
            raise err or RuntimeError("no answer")
        finally:
            for t in tasks:
                t.cancel()

    @staticmethod
    def _question(subnet: str, text: str, tweet_text: str, tweet_age_s: float | None) -> str:
        parts = [f"Subnet: {subnet}", f"Post:\n{text[:3000]}"]
        if tweet_text:
            age = f" (posted {tweet_age_s / 60:.0f} minutes ago)" if tweet_age_s is not None else ""
            parts.append(f"The X post it shares{age}:\n{tweet_text[:3000]}")
        return "\n\n".join(parts)

    @staticmethod
    def _verdict(answer: str, by: str = "ai") -> Verdict:
        data = json.loads(answer)
        important = bool(data["important"]) and data["category"] != "none"
        return Verdict(important, data["category"], str(data["reason"])[:160], by)

    async def _ask_openrouter(self, subnet: str, text: str, tweet_text: str, tweet_age_s: float | None,
                              model: str = "") -> Verdict:
        """OpenRouter's chat completions API, with a JSON-schema answer; what it cost is added to stats["cost"]."""
        if self.session is None:
            import aiohttp
            self.session = aiohttp.ClientSession()
        model = model or self.model
        body = {
            "model": model,
            "messages": [{"role": "system", "content": SYSTEM},
                         {"role": "user", "content": self._question(subnet, text, tweet_text, tweet_age_s)}],
            "response_format": {"type": "json_schema",
                                "json_schema": {"name": "news_check", "strict": True, "schema": SCHEMA}},
            "max_tokens": 2048,
        }
        if model == self.fast_model:
            body["reasoning"] = {"enabled": False}      # the fast model answers without thinking (with it: 3 s, no better)
            body["provider"] = {"sort": "latency"}
        elif model.startswith("openai/"):
            # a thinking model: a short yes/no needs little of it — low effort keeps the answer fast (the bots wait for it)
            # and leaves room in max_tokens for the answer itself
            body["reasoning"] = {"effort": "low"}
        async with self.session.post(OPENROUTER_URL, json=body,
                                     headers={"Authorization": f"Bearer {self.openrouter_key}"}) as r:
            data = await r.json(content_type=None)
            if r.status != 200 or "error" in data:
                raise RuntimeError(f"OpenRouter {r.status}: {str(data.get('error'))[:120]}")
        self.stats["cost"] += float((data.get("usage") or {}).get("cost") or 0)
        v = self._verdict(data["choices"][0]["message"]["content"])
        v.model = model
        return v

    async def acquisition(self, author: str, text: str) -> dict | None:
        """Does this X post announce that the author's team acquired a subnet? → {"acquisition", "netuid", "acquirer",
        "reason"}, or None when no AI answered (OpenRouter only; the fast model when there is one: ~1.3 s)."""
        if self.provider != "openrouter":
            return None
        if self.session is None:
            import aiohttp
            self.session = aiohttp.ClientSession()
        model = self.fast_model or self.model
        body = {"model": model,
                "messages": [{"role": "system", "content": ACQ_SYSTEM},
                             {"role": "user", "content": f"Author: @{author}\n\nPost:\n{text[:3000]}"}],
                "response_format": {"type": "json_schema",
                                    "json_schema": {"name": "acquisition", "strict": True, "schema": ACQ_SCHEMA}},
                "max_tokens": 1024}
        if model == self.fast_model:
            body["reasoning"], body["provider"] = {"enabled": False}, {"sort": "latency"}
        elif model.startswith("openai/"):
            body["reasoning"] = {"effort": "low"}
        try:
            async with self.session.post(OPENROUTER_URL, json=body,
                                         headers={"Authorization": f"Bearer {self.openrouter_key}"}) as r:
                data = await r.json(content_type=None)
                if r.status != 200 or "error" in data:
                    raise RuntimeError(f"OpenRouter {r.status}: {str(data.get('error'))[:120]}")
            self.stats["cost"] += float((data.get("usage") or {}).get("cost") or 0)
            return json.loads(data["choices"][0]["message"]["content"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("acquisition check failed: %s: %s", type(e).__name__, str(e)[:120])
            return None

    async def _ask(self, subnet: str, text: str, tweet_text: str, tweet_age_s: float | None) -> Verdict:
        kwargs = {}
        if not self.model.startswith("claude-haiku"):          # Haiku has no server-side fallback
            kwargs = {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
        r = await self.client.beta.messages.create(
            model=self.model,
            max_tokens=2048,
            system=SYSTEM,
            messages=[{"role": "user", "content": self._question(subnet, text, tweet_text, tweet_age_s)}],
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}},
            **kwargs,
        )
        if r.stop_reason == "refusal":
            raise RuntimeError("refused")
        return self._verdict(next(b.text for b in r.content if b.type == "text"))
