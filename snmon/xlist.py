"""Every subnet's X account in ONE request: a private X list, read with the user's X login (its browser cookies).

fxtwitter reads one account per request (~2.5 s each, ~40% fail), so each of ~62 accounts was seen only every ~6-13 s
(21 Oct: @yanez__ai's post reached the bots 12.9 s old). X's own web API returns the newest posts of all the list's
members in one request (~0.6 s); X allows 500 of those per 15 minutes, so the list is read about every 2 s.

The login is X_AUTH_TOKEN + X_CT0 (the `auth_token` and `ct0` cookies of x.com, copied from the browser). X does not
allow automated reading this way and may lock the account — the user chose to accept that (21 Oct, @cdylan321). If the
login stops working, the list is not read any more and fxtwitter goes on alone.

The list (private: only the account sees it, nobody is notified) and its members are kept in data/x_list.json; a
subnet account found later is added to it. X's web app names its requests by ids that change when X ships a new app:
when one stops working, the current ids are read again from x.com's own scripts (also kept in data/x_list.json).
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import time

import aiohttp

from .config import ROOT

log = logging.getLogger("snmon.xlist")

DATA_FILE = ROOT / "data" / "x_list.json"
API = "https://x.com/i/api/graphql/{}/{}"
# the web app's own public token (in x.com's JavaScript) — the login itself is the cookies
BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
OPS = ("ListLatestTweetsTimeline", "ListAddMember", "CreateList", "Viewer", "SearchTimeline")
POSTED = ("CreateList", "ListAddMember", "SearchTimeline")   # sent as POST (search answers a GET with 404)
REDISCOVER_S = 1800        # read the request ids from x.com's scripts at most this often
ADD_PACE_S = 2.0           # members are added one at a time, this far apart
EPOCH_MS = 1288834974657   # X's ids start here: (id >> 22) + EPOCH_MS = when it was posted, in ms


class XLoginError(Exception):
    """The login does not work (expired, logged out, or the account locked)."""


def _tweet(r: dict | None) -> dict | None:
    r = r or {}
    if r.get("__typename") == "TweetWithVisibilityResults":
        r = r.get("tweet") or {}
    return r if r.get("legacy") and r.get("rest_id") else None


def _user(t: dict) -> dict:
    u = ((t.get("core") or {}).get("user_results") or {}).get("result") or {}
    core, legacy = u.get("core") or {}, u.get("legacy") or {}
    return {"screen_name": core.get("screen_name") or legacy.get("screen_name") or "",
            "name": core.get("name") or legacy.get("name") or "",
            "avatar_url": (u.get("avatar") or {}).get("image_url") or legacy.get("profile_image_url_https"),
            "followers": (u.get("relationship_counts") or {}).get("followers", legacy.get("followers_count")),
            "joined": core.get("created_at") or legacy.get("created_at") or ""}


def _text(t: dict) -> str:
    """The whole text (a long post keeps it in note_tweet), its t.co links expanded, the media link dropped."""
    lg = t["legacy"]
    note = ((t.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result") or {}
    text = note.get("text") or lg.get("full_text") or ""
    urls = ((note.get("entity_set") or {}) if note.get("text") else {}).get("urls") or (lg.get("entities") or {}).get("urls") or []
    for u in urls:
        if u.get("url") and u.get("expanded_url"):
            text = text.replace(u["url"], u["expanded_url"])
    for m in (lg.get("entities") or {}).get("media") or []:
        if m.get("url"):
            text = text.replace(m["url"], "")
    return html.unescape(text).strip()


def posted_at(tweet_id: str | int) -> float:
    return ((int(tweet_id) >> 22) + EPOCH_MS) / 1000


def to_item(result: dict) -> dict | None:
    """One post from X's web API in the shape the monitor reads from fxtwitter (id, author, text, url,
    created_timestamp, replying_to, quote, reposted_by, media, likes / reposts / replies)."""
    t = _tweet(result)
    if t is None:
        return None
    lg = t["legacy"]
    original = _tweet((lg.get("retweeted_status_result") or {}).get("result"))
    if original is not None:            # a repost: the original post, marked as reposted by the member (as fxtwitter)
        item = to_item(original)
        if item is not None:
            by = _user(t)
            item["reposted_by"] = {"screen_name": by["screen_name"], "name": by["name"]}
        return item
    a = _user(t)
    tid = str(t["rest_id"])
    item = {"id": tid, "url": f"https://x.com/{a['screen_name']}/status/{tid}", "text": _text(t),
            "created_timestamp": posted_at(tid), "author": a, "likes": lg.get("favorite_count", 0),
            "reposts": lg.get("retweet_count", 0), "replies": lg.get("reply_count", 0)}
    if lg.get("in_reply_to_status_id_str"):
        item["replying_to"] = {"screen_name": lg.get("in_reply_to_screen_name") or "", "status": lg["in_reply_to_status_id_str"]}
    q = _tweet((t.get("quoted_status_result") or {}).get("result"))
    if q is not None:
        qa = _user(q)
        item["quote"] = {"id": str(q["rest_id"]), "text": _text(q), "author": qa,
                         "url": f"https://x.com/{qa['screen_name']}/status/{q['rest_id']}"}
    photos = [{"url": m["media_url_https"]} for m in (lg.get("extended_entities") or {}).get("media") or []
              if m.get("type") == "photo" and m.get("media_url_https")]
    if photos:
        item["media"] = {"photos": photos}
    return item


def timeline_items(data: dict) -> list[dict]:
    """Every post in a timeline answer, newest first (a post shown twice — alone and in its thread — once)."""
    found: dict[str, dict] = {}

    def walk(o):
        if isinstance(o, dict):
            if o.get("__typename") in ("Tweet", "TweetWithVisibilityResults"):
                item = to_item(o)
                if item is not None:
                    found.setdefault(item["id"] + ("r" if item.get("reposted_by") else ""), item)
                return                  # its quote / original is part of it
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(data)
    return sorted(found.values(), key=lambda i: -int(i["id"]))


class XList:
    def __init__(self, auth_token: str, ct0: str, session: aiohttp.ClientSession | None = None, path=DATA_FILE) -> None:
        self.auth_token, self.ct0 = auth_token, ct0
        self.session = session
        self.path = path
        self.list_id = ""
        self.members: dict[str, str] = {}        # handle (lowercase) → X user id
        self.ops: dict[str, dict] = {}            # request name → {"id", "features"}
        self.who = ""                             # the account's @handle
        self.left: int | None = None              # list reads left in this 15-minute window, and when it resets
        self.reset = 0.0
        self.limits: dict[str, tuple[int, float]] = {}   # request → (left in this window, when it resets)
        self.pause_until = 0.0
        self._discovered = 0.0
        try:
            d = json.loads(self.path.read_text())
            self.list_id, self.members, self.ops = d.get("list_id", ""), d.get("members", {}), d.get("ops", {})
        except (OSError, ValueError):
            pass

    def _save(self) -> None:
        try:
            self.path.write_text(json.dumps({"list_id": self.list_id, "members": self.members, "ops": self.ops}, indent=1))
        except OSError:
            pass

    def _headers(self) -> dict:
        return {"authorization": f"Bearer {BEARER}", "x-csrf-token": self.ct0, "cookie": f"auth_token={self.auth_token}; ct0={self.ct0}",
                "x-twitter-auth-type": "OAuth2Session", "x-twitter-active-user": "yes", "x-twitter-client-language": "en",
                "content-type": "application/json", "referer": "https://x.com/", "user-agent": UA}

    async def start(self) -> None:
        """Who the login is, the request ids, and the list (made if there is none). Raises XLoginError."""
        await self.login()
        if not self.list_id:
            d = await self._call("CreateList", {"isPrivate": True, "name": "subnets", "description": ""})
            self.list_id = ((d.get("data") or {}).get("list") or {}).get("id_str") or ""
            if not self.list_id:
                raise RuntimeError(f"the private list could not be made: {str(d.get('errors'))[:160]}")
            self.members = {}
            self._save()

    async def login(self) -> None:
        """Who the login is, and the request ids (read from x.com's scripts when missing). Raises XLoginError."""
        if self.session is None:
            # no cookie jar: the login goes only where it is written in (x.com), and X's own cookies are not mixed in
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), cookie_jar=aiohttp.DummyCookieJar(),
                                                 max_line_size=65536, max_field_size=65536)
        if any(op not in self.ops for op in OPS):
            await self.discover()
        d = await self._call("Viewer", {"withCommunitiesMemberships": False})
        u = (((d.get("data") or {}).get("viewer") or {}).get("user_results") or {}).get("result") or {}
        self.who = (u.get("core") or u.get("legacy") or {}).get("screen_name") or ""
        if not self.who:
            raise XLoginError("X did not say who is logged in")

    async def _call(self, op: str, variables: dict) -> dict:
        """One request to X's web API. 401/403 → XLoginError; a 429 sets a pause; an unknown request id (X shipped a
        new app) → the ids are read again and it is tried once more."""
        for attempt in range(2):
            o = self.ops.get(op)
            if o is None:
                raise RuntimeError(f"{op}: no request id")
            feats = json.dumps({f: True for f in o.get("features", [])}, separators=(",", ":"))
            url = API.format(o["id"], op)
            if op in POSTED:
                req = self.session.post(url, headers=self._headers(),
                                        json={"variables": variables, "features": json.loads(feats), "queryId": o["id"]})
            else:
                req = self.session.get(url, headers=self._headers(),
                                       params={"variables": json.dumps(variables, separators=(",", ":")), "features": feats})
            async with req as r:
                self._limits(op, r)
                new_ct0 = r.cookies.get("ct0")
                if new_ct0 is not None and new_ct0.value and new_ct0.value != self.ct0:
                    self.ct0 = new_ct0.value          # X renewed the session's ct0
                if r.status in (401, 403):
                    raise XLoginError(f"X refused the login (HTTP {r.status}) — copy fresh auth_token / ct0 cookies")
                if r.status == 429:
                    self.pause_until = max(self.pause_until, self.reset or time.time() + 60)
                    raise RuntimeError("X: too many requests (429)")
                data = await r.json(content_type=None) if r.status in (200, 400, 404) else {}
            if r.status == 200 and data.get("data") is not None:
                return data
            if attempt == 0 and r.status in (400, 404) and time.time() - self._discovered > REDISCOVER_S:
                log.warning("X: %s answered %d — reading X's request ids again", op, r.status)
                await self.discover()
                continue
            raise RuntimeError(f"X {op}: HTTP {r.status} {str(data.get('errors'))[:160]}")
        raise RuntimeError(f"X {op}: failed")

    def _limits(self, op: str, r) -> None:
        try:
            left, reset = int(r.headers["x-rate-limit-remaining"]), float(r.headers["x-rate-limit-reset"])
        except (KeyError, ValueError):
            return
        self.limits[op] = (left, reset)
        if op == "ListLatestTweetsTimeline":
            self.left, self.reset = left, reset

    def wait(self, every: float, op: str = "ListLatestTweetsTimeline") -> float:
        """Seconds until the next `op` (a list read, a search): `every`, or longer when that would use up X's allowance
        for it before the window resets."""
        now = time.time()
        if now < self.pause_until:
            return self.pause_until - now
        left, reset = self.limits.get(op, (None, 0.0))
        if op == "ListLatestTweetsTimeline" and self.left is not None:
            left, reset = self.left, self.reset
        if left is not None and reset > now:
            return max(every, (reset - now) / max(left - 2, 1))
        return every

    async def search(self, query: str, count: int = 40) -> list[dict]:
        """X's search, newest first ("Latest"). 23 Oct, measured: a post shows up ~12 s after it was posted (90% within
        17 s); X allows 187 searches per 15 minutes."""
        d = await self._call("SearchTimeline", {"rawQuery": query, "count": count, "querySource": "typed_query",
                                                "product": "Latest"})
        return timeline_items(d)

    async def timeline(self, count: int = 40) -> list[dict]:
        """The list's newest posts, newest first."""
        d = await self._call("ListLatestTweetsTimeline", {"listId": self.list_id, "count": count})
        return timeline_items(d)

    async def sync(self, handles: dict[str, str], user_id) -> int:
        """Add the accounts the list does not have yet (handle lowercase → as written); `user_id(handle)` gives the X
        user id. Returns how many were added."""
        added = 0
        for low, handle in handles.items():
            if low in self.members:
                continue
            uid = await user_id(handle)
            if not uid:
                continue
            try:
                d = await self._call("ListAddMember", {"listId": self.list_id, "userId": str(uid)})
            except XLoginError:
                raise
            except Exception as e:
                log.warning("X list: adding @%s failed: %s", handle, e)
                continue
            if (d.get("data") or {}).get("list") is not None:
                self.members[low] = str(uid)
                added += 1
                self._save()
            await asyncio.sleep(ADD_PACE_S)
        return added

    async def discover(self) -> None:
        """Read the current request ids (and their feature names) from x.com's own scripts: the logged-in page names a
        chunk map (number → name, hash); the list requests sit in chunks loaded later, so they are searched too."""
        self._discovered = time.time()
        async with self.session.get("https://x.com/home", headers={"cookie": f"auth_token={self.auth_token}; ct0={self.ct0}",
                                                                   "user-agent": UA}) as r:
            page = await r.text()
        scripts = re.findall(r'https://abs\.twimg\.com/responsive-web/client-web/main\.[0-9a-f]+\.js', page)
        m = re.search(r'\.u=e=>\(\(\{(.*?)\}\)\[e\]\|\|e\)\+"\."\+\(\{(.*?)\}\)\[e\]\+"a\.js"', page, re.S)
        base = re.search(r'\.p="(https://abs\.twimg\.com/[^"]+)"', page)
        if m and base:
            names = dict(re.findall(r'(\d+):"([^"]+)"', m.group(1)))
            hashes = dict(re.findall(r'(\d+):"([0-9a-f]+)"', m.group(2)))
            skip = re.compile(r"emoji|i18n|countries|icons")
            scripts += [f"{base.group(1)}{names.get(c, c)}.{h}a.js" for c, h in hashes.items() if not skip.search(names.get(c, ""))]
        found: dict[str, dict] = {}
        sem = asyncio.Semaphore(16)

        async def read(url: str) -> None:
            if all(op in found for op in OPS):
                return
            async with sem:
                try:
                    async with self.session.get(url, headers={"user-agent": UA}) as r:
                        src = await r.text() if r.status == 200 else ""
                except Exception:
                    return
            for q in re.finditer(r'queryId:"([\w-]{10,})",operationName:"(\w+)",operationType:"\w+",metadata:\{featureSwitches:\[([^\]]*)\]', src):
                if q.group(2) in OPS:
                    found.setdefault(q.group(2), {"id": q.group(1), "features": re.findall(r'"([^"]+)"', q.group(3))})
        await asyncio.gather(*(read(u) for u in scripts))
        if not found:
            raise RuntimeError("X's request ids could not be read from its scripts")
        self.ops.update(found)
        self._save()
        log.info("X: request ids read (%s)", ", ".join(sorted(found)))
