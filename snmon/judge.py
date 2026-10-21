"""Is a news post about a subnet important — something that can move its price now?

The trading bots buy on news (sn_trader news trading). 21 Oct: of 21 news trades only 2 made money; most "news" was
routine — answers to users, dev notes, shares of old tweets (SN124: a team member shared a 2-day-old quarterly update;
the bot bought, nothing moved). So each post is judged before it is sent to the bots: important = a product launch, big
progress, funding / a new investor, revenue / customers / contracts, a notable partnership, a listing, a buyback / burn.

Claude reads the post (and the X post it shares) when ANTHROPIC_API_KEY is set in .env; else, or when it does not answer
in NEWS_AI_TIMEOUT seconds, keyword rules decide. The verdict goes to the bots with the news and onto the card.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass

log = logging.getLogger("snmon.judge")

CATEGORIES = ["launch", "progress", "funding", "revenue", "partnership", "listing", "tokenomics", "other", "none"]

SYSTEM = """You judge news about one Bittensor subnet for a trading bot. The bot buys the subnet's token the moment \
important news appears and sells within minutes, so "important" means news that can make buyers act now.

IMPORTANT (any one is enough, and it must be new and concrete):
- launch: a product, app, model, feature or mainnet / major version that is released or live now
- progress: a big measurable milestone — a record, a benchmark beaten, usage or growth numbers
- funding: money raised, a new investor or backer, a grant
- revenue: revenue or income numbers, paying customers, contracts or commercial deals signed
- partnership: a partnership or integration with a notable company or project
- listing: listed on an exchange or a major platform
- tokenomics: a buyback, a burn, or the owner / team cutting their own emissions or selling less

NOT important:
- replies or answers to users, support, bug reports and fixes, maintenance, downtime
- instructions for miners or validators, competition rules, scores, weights, routine dev or weekly updates
- reminders, recaps, quarterly summaries restating known facts, re-shares of older posts
- events, AMAs, spaces, podcasts, hiring, memes, greetings, vague hype or teasers ("big things coming")

If the shared X post is older than a few hours, the market already knows it: not important.
Answer with the category of the news, or "none" when it is not important, and a reason of at most 15 words."""

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
    """One verdict per post (cached by its text); Claude when it is set up, the rules otherwise."""

    def __init__(self, model: str = "claude-opus-5-5", timeout: float = 8.0, client=None) -> None:
        self.model = model
        self.timeout = timeout
        self.client = client
        if self.client is None and os.getenv("ANTHROPIC_API_KEY"):
            try:
                import anthropic
                self.client = anthropic.AsyncAnthropic(max_retries=0)
            except ImportError:
                log.warning("ANTHROPIC_API_KEY is set but the anthropic package is not installed "
                            "(.venv/bin/pip install -r requirements.txt) — judging news by keyword rules")
        self._cache: dict[str, Verdict] = {}
        self.stats = {"ai": 0, "rules": 0, "ai_fail": 0}

    @property
    def mode(self) -> str:
        return f"Claude ({self.model})" if self.client is not None else "keyword rules (no ANTHROPIC_API_KEY)"

    async def judge(self, subnet: str, text: str, tweet_text: str = "", tweet_age_s: float | None = None) -> Verdict:
        key = f"{subnet}\n{text}\n{tweet_text}"
        if key in self._cache:
            return self._cache[key]
        whole = "\n\n".join(x for x in (text, tweet_text) if x)
        v = None
        if self.client is not None:
            try:
                v = await asyncio.wait_for(self._ask(subnet, text, tweet_text, tweet_age_s), self.timeout)
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

    async def _ask(self, subnet: str, text: str, tweet_text: str, tweet_age_s: float | None) -> Verdict:
        parts = [f"Subnet: {subnet}", f"Post:\n{text[:3000]}"]
        if tweet_text:
            age = f" (posted {tweet_age_s / 60:.0f} minutes ago)" if tweet_age_s is not None else ""
            parts.append(f"The X post it shares{age}:\n{tweet_text[:3000]}")
        kwargs = {}
        if not self.model.startswith("claude-haiku"):          # Haiku has no server-side fallback
            kwargs = {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
        r = await self.client.beta.messages.create(
            model=self.model,
            max_tokens=2048,
            system=SYSTEM,
            messages=[{"role": "user", "content": "\n\n".join(parts)}],
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}},
            **kwargs,
        )
        if r.stop_reason == "refusal":
            raise RuntimeError("refused")
        data = json.loads(next(b.text for b in r.content if b.type == "text"))
        important = bool(data["important"]) and data["category"] != "none"
        return Verdict(important, data["category"], str(data["reason"])[:160], "ai")
