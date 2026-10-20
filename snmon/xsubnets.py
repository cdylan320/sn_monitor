"""Each subnet's own X account, found automatically, so the news monitor can watch them all.

No field on chain holds it, so the handle is read from:
  1. the subnet's website — the X account its homepage links most (sites also link founders, partners and const, so
     the most frequent one wins and accounts that are not a subnet's are skipped);
  2. else the X link in its GitHub README;
  3. else an X link in its on-chain identity (description / contact).
A handle is only watched once its X profile points back to the subnet: the profile's website is the subnet's site, or
its bio names it ("SN85" / "subnet 85"), or its name / handle contains the subnet's name. 20 Oct: 70 handles found for
128 subnets, 61 checked out (51 by website); SN21's site linked a marketing person's account — that one is not watched.
NEWS_X_SUBNET_HANDLES adds or corrects any by hand ("72:NATIXNetwork", "21:-" = none).

Kept in data/x_subnets.json and looked at again every DISCOVER_HOURS — sooner for a new subnet, a subnet whose website
or name changed, or a profile that could not be read.
"""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import re
import time

import aiohttp

from .config import ROOT

log = logging.getLogger("snmon.xsubnets")

CACHE_FILE = ROOT / "data" / "x_subnets.json"
DISCOVER_HOURS = 24
RETRY_HOURS = 1            # a profile that could not be read is tried again after this
PROFILE_PACE_S = 1.0       # profile checks one at a time, this far apart (the same free API serves the timelines)
PROFILE_URLS = ("https://api.fxtwitter.com/{}", "https://api.fxtwitter.com/2/profile/{}")
BROWSER = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"}
LINK_RE = re.compile(r"(?:https?:)?//(?:www\.|mobile\.)?(?:x|twitter)\.com/(?:#!/)?@?(\w{1,15})(?=[/\"'?#\s<\\)\]]|$)", re.I)
# X's own pages and accounts that are never one subnet's: Bittensor-wide accounts, const
NOT_SUBNET = frozenset({"intent", "share", "home", "i", "search", "hashtag", "login", "signup", "explore", "settings",
                        "messages", "notifications", "privacy", "tos", "x", "twitter", "elonmusk", "opentensor",
                        "bittensor", "const_reborn"})
GITHUB_RE = re.compile(r"github\.com/([^/\s]+/[^/\s#?]+)", re.I)


def handles_in(text: str, skip: frozenset[str] | set[str] = frozenset()) -> collections.Counter:
    """X accounts linked in a page / README (x.com/<handle>, twitter.com/<handle>, also inside a status link)."""
    return collections.Counter(h for h in LINK_RE.findall(text or "")
                               if h.lower() not in NOT_SUBNET and h.lower() not in skip)


def _domain(url: str) -> str:
    m = re.match(r"^(?:[a-z][\w+.-]*://)?(?:www\.)?([^/:?#\s]+)", (url or "").strip().lower())
    return m.group(1) if m else ""


def _same_domain(a: str, b: str) -> bool:
    return bool(a) and bool(b) and (a == b or a.endswith("." + b) or b.endswith("." + a))


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def checks_out(netuid: int, name: str, url: str, handle: str, user: dict) -> str:
    """Why `handle`'s X profile is the subnet's own ("website" / "bio" / "name"), or "" when nothing says so."""
    site = user.get("website") if isinstance(user.get("website"), dict) else {}
    if _same_domain(_domain(site.get("url") or site.get("display_url") or ""), _domain(url)):
        return "website"
    about = f"{user.get('description') or ''} {user.get('name') or ''}"
    if re.search(rf"\b(?:sn|subnet)\s?#?{netuid}\b", about, re.I):
        return "bio"
    key = _norm(name)
    if len(key) >= 4 and (key in _norm(about) or key in _norm(handle)):
        return "name"
    return ""


async def _get(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, headers=BROWSER, allow_redirects=True) as r:
            return (await r.text(errors="ignore"))[:3_000_000] if r.status == 200 else ""
    except Exception as e:
        log.debug("GET %s failed: %s", url, e)
        return ""


async def read_profile(session: aiohttp.ClientSession, handle: str) -> dict | None:
    """The account's X profile via fxtwitter (either endpoint; one answers "404" now and then), None if unreadable."""
    for url in PROFILE_URLS:
        try:
            async with session.get(url.format(handle)) as r:
                user = (await r.json(content_type=None)).get("user") if r.status == 200 else None
        except Exception as e:
            log.debug("X profile %s failed: %s", handle, e)
            user = None
        if isinstance(user, dict):
            return user
    return None


class SubnetAccounts:
    def __init__(self, overrides: dict[int, str] | None = None, skip: set[str] | None = None) -> None:
        self.overrides = overrides or {}        # netuid → handle ("" = none), from NEWS_X_SUBNET_HANDLES
        self.skip = frozenset(s.lower() for s in (skip or ()))   # accounts watched for themselves (const)
        self.found: dict[int, dict] = {}        # netuid → {"handle", "why", "candidate", "src", "url", "name", "at"}
        self.busy = False
        try:
            self.found = {int(n): e for n, e in json.loads(CACHE_FILE.read_text()).get("found", {}).items()}
        except (OSError, ValueError):
            pass

    def save(self) -> None:
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CACHE_FILE.write_text(json.dumps({"found": self.found}, indent=1))
        except OSError:
            pass

    def by_netuid(self) -> dict[int, str]:
        out = {n: e["handle"] for n, e in self.found.items() if e.get("handle")}
        for n, h in self.overrides.items():
            if h:
                out[n] = h
            else:
                out.pop(n, None)
        return out

    def accounts(self) -> dict[str, tuple[str, set[int]]]:
        """handle (lowercase) → (handle as written, the subnets it posts for — one, unless a team runs several)."""
        out: dict[str, tuple[str, set[int]]] = {}
        for n, h in self.by_netuid().items():
            out.setdefault(h.lower(), (h, set()))[1].add(n)
        return out

    def due(self, subnets: dict) -> list[int]:
        """Subnets to look at now: new, website / name changed, not looked at for DISCOVER_HOURS (an unreadable
        profile: RETRY_HOURS)."""
        now, out = time.time(), []
        for n, info in subnets.items():
            if not n or n in self.overrides:
                continue
            e = self.found.get(n)
            wait = (RETRY_HOURS if e and e.get("why") == "unreadable" else DISCOVER_HOURS) * 3600
            if e is None or e.get("url") != (info.url or "") or e.get("name") != info.name or now - e.get("at", 0) >= wait:
                out.append(n)
        return out

    async def _candidate(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, info) -> tuple[str, str]:
        ident = info.identity or {}
        async with sem:
            if info.url:
                c = handles_in(await _get(session, info.url), self.skip)
                if c:
                    return c.most_common(1)[0][0], "website"
            gh = GITHUB_RE.search(ident.get("github") or "")
            if gh:
                repo = gh.group(1).removesuffix(".git")
                c = handles_in(await _get(session, f"https://raw.githubusercontent.com/{repo}/HEAD/README.md"), self.skip)
                if c:
                    return c.most_common(1)[0][0], "github"
        c = handles_in(" ".join(str(v) for v in ident.values()), self.skip)
        return (c.most_common(1)[0][0], "identity") if c else ("", "")

    async def refresh(self, session: aiohttp.ClientSession, subnets: dict, profile=read_profile) -> int:
        """Find and check the handle of every subnet that is due; → how many were looked at."""
        if self.busy:
            return 0
        self.busy = True
        try:
            due = self.due(subnets)
            sem = asyncio.Semaphore(8)
            found = await asyncio.gather(*(self._candidate(session, sem, subnets[n]) for n in due))
            for n, (handle, src) in zip(due, found):
                info = subnets[n]
                e = {"handle": "", "why": "", "candidate": handle, "src": src, "url": info.url or "",
                     "name": info.name, "at": time.time()}
                if handle:
                    user = await profile(session, handle)
                    if user is None:
                        e["why"] = "unreadable"
                    else:
                        why = checks_out(n, info.name, info.url or "", handle, user)
                        e.update(handle=handle if why else "", why=why or "not the subnet's")
                    await asyncio.sleep(PROFILE_PACE_S)
                self.found[n] = e
            for n in [n for n in self.found if n not in subnets]:
                del self.found[n]
            if due:
                self.save()
            return len(due)
        finally:
            self.busy = False
