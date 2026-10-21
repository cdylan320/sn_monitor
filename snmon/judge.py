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
from dataclasses import dataclass

log = logging.getLogger("snmon.judge")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = {"openrouter": "openai/gpt-6.1-sol", "anthropic": "claude-opus-5-5"}

CATEGORIES = ["launch", "progress", "funding", "revenue", "partnership", "listing", "tokenomics", "other", "none"]

SYSTEM = """You judge news about one Bittensor subnet for a trading bot. The bot buys the subnet's token the moment \
important news appears and sells within minutes, so "important" means news that can make buyers act now.

Two rules, both apply:
1. These ALWAYS count as important when they are new and concrete (the categories to answer with):
- launch: a product, app, model, feature or mainnet / major version that is released or live now
- progress: a big measurable milestone — a record, a benchmark beaten, usage or growth numbers
- funding: money raised, a new investor or backer, a grant
- revenue: revenue or income numbers, paying customers, contracts or commercial deals signed
- partnership: a partnership or integration with a notable company or project
- listing: listed on an exchange or a major platform
- tokenomics: a buyback, a burn, or the owner / team cutting their own emissions or selling less
2. Beyond these, use your own judgment: any other NEW, CONCRETE news that can move the token's price now is important
   too — answer "other". Do not miss important news just because it is not in the list above.

Usually NOT important (unless it carries real news like the above):
- replies or answers to users, support, bug reports and fixes, maintenance, downtime
- instructions for miners or validators, competition rules, scores, weights, routine dev or weekly updates
- reminders, recaps, quarterly summaries restating known facts, re-shares of older posts
- events, AMAs, spaces, podcasts, hiring, memes, greetings, vague hype or teasers ("big things coming")

If the shared X post is older than a few hours, the market already knows it: not important.
Answer with the category of the news ("other" for important news outside the examples, "none" when it is not \
important), and a reason of at most 15 words."""

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

    def event(self) -> dict:
        return {"important": self.important, "category": self.category, "why": self.reason, "judge": self.by}


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

    def __init__(self, model: str = "", timeout: float = 8.0, client=None, session=None, race: int = 1) -> None:
        self.timeout = timeout
        self.race = max(1, race)                 # identical requests sent at once; the first good answer is used
        self.client = client                     # AsyncAnthropic (Claude directly)
        self.session = session                   # aiohttp session (OpenRouter)
        self.openrouter_key = "" if client is not None else (os.getenv("OPENROUTER_API_KEY") or "").strip()
        self.provider = ("openrouter" if self.openrouter_key or session is not None
                         else "anthropic" if client is not None or os.getenv("ANTHROPIC_API_KEY") else "")
        self.model = model or DEFAULT_MODEL.get(self.provider, "")
        if self.client is None and self.provider == "anthropic":
            try:
                import anthropic
                self.client = anthropic.AsyncAnthropic(max_retries=0)
            except ImportError:
                log.warning("ANTHROPIC_API_KEY is set but the anthropic package is not installed "
                            "(.venv/bin/pip install -r requirements.txt) — judging news by keyword rules")
        self._cache: dict[str, Verdict] = {}
        self.stats = {"ai": 0, "rules": 0, "ai_fail": 0, "cost": 0.0}

    @property
    def ready(self) -> bool:
        return self.provider == "openrouter" or self.client is not None

    @property
    def mode(self) -> str:
        if self.provider == "openrouter":
            return f"{self.model} via OpenRouter" + (f" ({self.race} requests raced)" if self.race > 1 else "")
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
                ask = self._ask_openrouter if self.provider == "openrouter" else self._ask
                v = await asyncio.wait_for(self._first(ask, subnet, text, tweet_text, tweet_age_s), self.timeout)
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

    async def _ask_openrouter(self, subnet: str, text: str, tweet_text: str, tweet_age_s: float | None) -> Verdict:
        """OpenRouter's chat completions API, with a JSON-schema answer; what it cost is added to stats["cost"]."""
        if self.session is None:
            import aiohttp
            self.session = aiohttp.ClientSession()
        body = {
            "model": self.model,
            "messages": [{"role": "system", "content": SYSTEM},
                         {"role": "user", "content": self._question(subnet, text, tweet_text, tweet_age_s)}],
            "response_format": {"type": "json_schema",
                                "json_schema": {"name": "news_check", "strict": True, "schema": SCHEMA}},
            "max_tokens": 2048,
        }
        if self.model.startswith("openai/"):
            # a thinking model: a short yes/no needs little of it — low effort keeps the answer fast (the bots wait for it)
            # and leaves room in max_tokens for the answer itself
            body["reasoning"] = {"effort": "low"}
        async with self.session.post(OPENROUTER_URL, json=body,
                                     headers={"Authorization": f"Bearer {self.openrouter_key}"}) as r:
            data = await r.json(content_type=None)
            if r.status != 200 or "error" in data:
                raise RuntimeError(f"OpenRouter {r.status}: {str(data.get('error'))[:120]}")
        self.stats["cost"] += float((data.get("usage") or {}).get("cost") or 0)
        return self._verdict(data["choices"][0]["message"]["content"])

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
