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

Founders and owners too (the user, 10 Nov: SN71's CEO @gavinzaentz posted, the monitor saw it 11 minutes later when the
team shared it in Discord): the accounts a subnet's own account reposts, quotes, replies to or mentions, and the other X
accounts on its website, are candidates; one whose profile has a role word (founder, CEO, CTO, owner, building …) AND
names the subnet (its @account, "SN71", its name, or the same website) is watched for it too — at most FOUNDERS_MAX a
subnet, checked again every FOUNDER_RECHECK_DAYS. NEWS_X_FOUNDER_HANDLES adds any by hand ("71:gavinzaentz").

Kept in data/x_subnets.json and looked at again every DISCOVER_HOURS — sooner for a new subnet, a subnet whose website
or name changed, or a profile that could not be read; at once (not in the hourly round) when a subnet's identity or owner
changes on chain — a sold subnet's new owner posts from a new account (23 Oct: SN27 → Komms, @KommsApp).
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
# X's own pages and accounts that are never one subnet's: Bittensor-wide accounts, const, and news / analytics accounts
# that report on every subnet (4 Nov: @subnetradarcom's report of an SN16 owner change was taken for a sale — its posts
# about all subnets went out as SN16's news for days)
NOT_SUBNET = frozenset({"intent", "share", "home", "i", "search", "hashtag", "login", "signup", "explore", "settings",
                        "messages", "notifications", "privacy", "tos", "x", "twitter", "elonmusk", "opentensor",
                        "bittensor", "const_reborn", "subnetradarcom", "tao_planet_"})
GITHUB_RE = re.compile(r"github\.com/([^/\s]+/[^/\s#?]+)", re.I)
ROLE_RE = re.compile(r"\b(co-?founders?|founders?|founding|ceo|cto|coo|cpo|chief \w+ officer|owner|creator|building|"
                     r"built|head of|lead(?:ing)?|core (?:dev|team|contributor))\b", re.I)
FOUNDERS_MAX = 3            # founder / owner accounts watched per subnet
FOUNDER_RECHECK_DAYS = 7    # a candidate's profile is read again after this (a founder still one, a no still a no)
FOUNDER_CHECKS = 40         # profiles read per round at most (the same free API serves the timelines)


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


def founder_of(netuid: int, name: str, url: str, official: str, user: dict) -> str:
    """Why this X profile is a founder's / owner's of the subnet — a role word AND the subnet named — or ""."""
    bio = user.get("description") or ""
    role = ROLE_RE.search(bio)
    if not role:
        return ""
    # an account that names another subnet's number is that one's (10 Nov trial: @IOTA_SN9 — the account of SN9 — was
    # taken for SN1 and SN13, which the same team runs; @QuantumSN48 for SN63)
    named = ({int(k) for k in re.findall(r"\b(?:sn|subnet)\s?#?(\d{1,3})\b", bio, re.I)}
             | {int(k) for k in re.findall(r"sn_?(\d{1,3})(?!\d)", user.get("screen_name") or "", re.I)})
    if named and netuid not in named:
        return ""
    site = user.get("website") if isinstance(user.get("website"), dict) else {}
    if official and re.search(rf"@{re.escape(official)}\b", bio, re.I):
        ref = f"@{official}"
    elif re.search(rf"\b(?:sn|subnet)\s?#?{netuid}\b", bio, re.I):
        ref = f"SN{netuid}"
    elif _same_domain(_domain(site.get("url") or site.get("display_url") or ""), _domain(url)):
        ref = "the subnet's website"
    elif len(_norm(name)) >= 5 and re.search(rf"\b{re.escape(name)}\b", bio, re.I):
        ref = name
    else:
        return ""
    return f"{role.group(0)} · {ref}"


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
    def __init__(self, overrides: dict[int, str] | None = None, skip: set[str] | None = None,
                 founders: dict[int, list[str]] | None = None) -> None:
        self.overrides = overrides or {}        # netuid → handle ("" = none), from NEWS_X_SUBNET_HANDLES
        self.founder_overrides = founders or {}  # netuid → [handle], from NEWS_X_FOUNDER_HANDLES
        self.founders: dict[int, dict[str, dict]] = {}   # netuid → {handle lowercase: {"handle", "why", "at"}}
        self.checked: dict[str, list] = {}      # handle lowercase → [when its profile was read, why / "" = no]
        self.candidates: dict[int, collections.Counter] = {}   # netuid → accounts its own account talks to
        self.skip = frozenset(s.lower() for s in (skip or ()))   # accounts watched for themselves (const)
        self.found: dict[int, dict] = {}        # netuid → {"handle", "why", "candidate", "src", "url", "name", "at"}
        self.forced: set[int] = set()           # looked at again on the next refresh, whatever changed (a new owner)
        self.busy = False
        try:
            d = json.loads(CACHE_FILE.read_text())
            self.found = {int(n): e for n, e in d.get("found", {}).items()}
            self.founders = {int(n): v for n, v in (d.get("founders") or {}).items() if isinstance(v, dict)}
            self.checked = {h: v for h, v in (d.get("checked") or {}).items() if isinstance(v, list)}
        except (OSError, ValueError):
            pass
        wrong = [n for n, e in self.found.items() if self._never((e or {}).get("handle") or "")]
        for n in wrong:                         # found or claimed before that account was known not to be a subnet's
            del self.found[n]
        if wrong:
            self.save()

    def save(self) -> None:
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CACHE_FILE.write_text(json.dumps({"found": self.found, "founders": self.founders, "checked": self.checked},
                                             indent=1))
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

    def founder_handles(self) -> dict[int, list[str]]:
        """netuid → its founders' / owners' handles: found, and added by hand."""
        out = {n: [e["handle"] for e in v.values()] for n, v in self.founders.items() if v}
        for n, hs in self.founder_overrides.items():
            out[n] = list(dict.fromkeys(out.get(n, []) + list(hs)))
        return out

    def accounts(self) -> dict[str, tuple[str, set[int]]]:
        """handle (lowercase) → (handle as written, the subnets it posts for — one, unless a team runs several) — the
        subnets' own accounts and their founders'."""
        out: dict[str, tuple[str, set[int]]] = {}
        for n, h in self.by_netuid().items():
            out.setdefault(h.lower(), (h, set()))[1].add(n)
        for n, hs in self.founder_handles().items():
            for h in hs:
                if not self._never(h):
                    out.setdefault(h.lower(), (h, set()))[1].add(n)
        return out

    def is_founder(self, key: str) -> bool:
        """A founder's account, not a subnet's own (its looks are fewer)."""
        key = key.lower()
        return (key not in {h.lower() for h in self.by_netuid().values()}
                and any(key in {h.lower() for h in hs} for hs in self.founder_handles().values()))

    def founder_why(self, key: str) -> str:
        for v in self.founders.values():
            if key.lower() in v:
                return v[key.lower()].get("why", "")
        return "added by hand" if any(key.lower() in {h.lower() for h in hs} for hs in self.founder_overrides.values()) else ""

    def note(self, netuids, handles) -> None:
        """Accounts a subnet's own account reposted, quoted, replied to or mentioned: founder candidates."""
        for n in netuids:
            c = self.candidates.setdefault(n, collections.Counter())
            c.update(h for h in handles if h and not self._never(h))

    async def check_founders(self, session: aiohttp.ClientSession, subnets: dict, profile=None, most: int = FOUNDER_CHECKS) -> int:
        """Read the profiles of the founder candidates not read for FOUNDER_RECHECK_DAYS (the most talked-to first) and
        keep the founders / owners among them, FOUNDERS_MAX a subnet → how many profiles were read."""
        profile = profile or read_profile
        official = self.by_netuid()
        now, wait = time.time(), FOUNDER_RECHECK_DAYS * 86400
        todo = []
        for n, c in self.candidates.items():
            info = subnets.get(n)
            if info is None or not n:
                continue
            own = {h.lower() for h in official.values()}
            for h, k in c.most_common(10):
                low = h.lower()
                if low in own or self._never(h) or now - (self.checked.get(low) or [0])[0] < wait:
                    continue
                todo.append((k, n, h))
        for n, v in self.founders.items():          # the founders kept: read again once a week
            for low, e in v.items():
                if now - (self.checked.get(low) or [0])[0] >= wait:
                    todo.append((99, n, e["handle"]))
        read = 0
        for _k, n, h in sorted(todo, key=lambda t: -t[0])[:most]:
            info = subnets.get(n)
            if info is None:
                continue
            user = await profile(session, h)
            read += 1
            if user is None:
                continue
            why = founder_of(n, info.name, info.url or "", official.get(n, ""), user)
            self.checked[h.lower()] = [now, why]
            have = self.founders.setdefault(n, {})
            if why and (h.lower() in have or len(have) < FOUNDERS_MAX):
                have[h.lower()] = {"handle": user.get("screen_name") or h, "why": why, "at": now}
            elif not why:
                have.pop(h.lower(), None)
            await asyncio.sleep(PROFILE_PACE_S)
        if read:
            self.founders = {n: v for n, v in self.founders.items() if v and n in subnets}
            self.save()
        return read

    def due(self, subnets: dict) -> list[int]:
        """Subnets to look at now: new, website / name changed, not looked at for DISCOVER_HOURS (an unreadable
        profile: RETRY_HOURS)."""
        now, out = time.time(), []
        for n, info in subnets.items():
            if not n or n in self.overrides:
                continue
            e = self.found.get(n)
            wait = (RETRY_HOURS if e and e.get("why") == "unreadable" else DISCOVER_HOURS) * 3600
            if (e is None or n in self.forced or e.get("url") != (info.url or "") or e.get("name") != info.name
                    or now - e.get("at", 0) >= wait):
                out.append(n)
        return out

    def recheck(self, n: int) -> None:
        self.forced.add(n)

    def _never(self, handle: str) -> bool:
        return handle.lower() in NOT_SUBNET or handle.lower() in self.skip

    def claim(self, n: int, handle: str, info) -> bool:
        """The buyer's X account, from its post announcing it acquired the subnet: watched at once, and kept until the
        subnet's identity changes on chain (then its website says which account is the subnet's). False: an account
        that is never one subnet's (a news account reporting the sale)."""
        if self._never(handle):
            return False
        self.found[n] = {"handle": handle, "why": "acquisition", "candidate": handle, "src": "acquisition post",
                         "url": info.url or "", "name": info.name, "at": time.time()}
        self.forced.discard(n)
        self.save()
        return True

    async def _candidate(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, info) -> tuple[str, str]:
        ident = info.identity or {}
        async with sem:
            if info.url:
                c = handles_in(await _get(session, info.url), self.skip)
                if c:
                    self.note([info.netuid], [h for h, _ in c.most_common(6)[1:]])   # founders are linked there too
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
                old = self.found.get(n) or {}
                if old.get("why") == "acquisition" and old.get("url") == (info.url or "") and old.get("name") == info.name:
                    self.found[n] = {**old, "at": time.time()}   # the identity still shows the old team: the buyer stays
                    continue
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
            self.forced -= set(due)
            for n in [n for n in self.found if n not in subnets]:
                del self.found[n]
            if due:
                self.save()
            return len(due)
        finally:
            self.busy = False
