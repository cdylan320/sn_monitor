"""News monitor: subnet channels on the Bittensor Discord → news webhook.

What counts as news (everything else in the channels is ignored):
  📢 announcement   a post that pings @everyone / @here
  🐦 X post         a link to an x.com / twitter.com post, by anyone (team shares are labelled)
  📰 team update    a post by the subnet's team or server staff that carries a link, or is a
                    real post (≥ NEWS_MIN_TEAM_CHARS characters, or has announcement words) —
                    short replies and support chatter are skipped

Who is "the team" is read from Discord itself: users and roles that the channel's permission
overwrites give moderation rights (manage messages / channels / threads, mention everyone) —
that is how subnet owners are set up in their own channels — plus server-wide staff roles.

Real time: messages arrive over the gateway (MESSAGE_CREATE, or PASSIVE_UPDATE for large guilds
followed by a REST fetch). A light sweep of channel last-message ids every 20s catches anything
the gateway didn't deliver. After a restart, messages from the last 30 minutes are caught up.

Each card shows the subnet's price at the time of the post, then is edited at +5m / +15m / +1h
with the market's reaction.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time

import aiohttp
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import alerts, fmt
from .config import ROOT
from .gateway import DiscordUser

log = logging.getLogger("snmon.news")

STATE_FILE = ROOT / "data" / "news_state.json"
CATCHUP_SECONDS = 30 * 60
REACTION_MINUTES = (5, 15, 60)

# moderation rights that mark a channel's team in its permission overwrites
TEAM_BITS = (1 << 4) | (1 << 13) | (1 << 17) | (1 << 29) | (1 << 34)  # channels, messages, @everyone, webhooks, threads
ADMIN_BIT = 1 << 3

URL_RE = re.compile(r"https?://[^\s<>()\]]+")
X_RE = re.compile(r"https?://(?:www\.|mobile\.)?(?:x|twitter|fxtwitter|vxtwitter|fixupx)\.com/(\w+)/status/(\d+)", re.I)
NEWS_WORDS = re.compile(
    r"\b(announc\w*|releas\w*|launch\w*|live now|is live|now live|upgrade|update[ds]?|mainnet|testnet|"
    r"deadline|enrol+ment|registration|migrat\w*|deprecat\w*|breaking|incentive|emission|burn|buyback|"
    r"partnership|partner|listing|roadmap|competition|challenge|season|leaderboard|v\d+(\.\d+)*|"
    r"hard ?fork|patch|maintenance|downtime|airdrop|reward)\b", re.I)
SUBNET_NAME_RE = re.compile(r"^\D{0,4}?(\d{1,3})(?!\d)")

# Scoring a team post (≥ NEWS_SCORE → news). Tuned on SN78's real channel: announcements like
# "ok folks, enrollment for C5 is open … <link>" score high; answers like "yeah C5 is not ready yet …"
# or "no it will have emission as long as …" score low.
STRONG_WORDS = re.compile(
    r"\b(announc\w*|releas\w*|launch\w*|(is|are|now|went) (live|open|available|published|out)|now (reaching|paying)|"
    r"results?|published|rewards? (are|is|have|will)|enrol+(ment|ing)|registration|upgrade (script|instructions?|"
    r"required|now)|deadline|mainnet|testnet|migrat\w*|hard ?fork|maintenance|downtime|deprecat\w*|breaking change|"
    r"partnership|listing|roadmap|airdrop|buyback|burn(ing|ed)?|split|new (version|release|model|competition|cohort|"
    r"season|round)|v\d+\.\d+|now fixed|has been fixed|are fixed|patch(ed)?)\b", re.I)
AUDIENCE = re.compile(r"\b(folks|everyone|every ?one|all miners|all validators|miners|validators|guys|"
                      r"heads[- ]up|psa|reminder|attention|community)\b|^\s*(update|important|note|news)\s*[:!-]", re.I)
STRUCTURE = re.compile(r"(^|\n)\s*(#{1,3} |[-*•] |\d[.)] )|\b1\) ", re.M)
CHATTY = re.compile(r"^\s*(yes|yeah|yea|yep|yup|no|nope|nah|sure|lol|lmao|haha|hah|i think|i guess|probably|"
                    r"idk|not sure|correct|right|exactly|that'?s|it'?s|its|when it|because|cuz|you can|u can|"
                    r"you should|if you|did you|have you|thanks|thank you|np|sorry|hm+|oh|ah)\b", re.I)
FIGURES = re.compile(r"\b\d+(\.\d+)?\s?%|\b\d+/\d+\b|\b\d{1,2}:\d{2}\b|\butc\b|\b(mon|tues|wednes|thurs|fri|satur|sun)day\b", re.I)
NEWS_SCORE = 3
STAFF_SCORE = 5   # server moderators post mostly moderation notices — they need a stronger signal
MEDIA = re.compile(r"https?://(?:[\w-]+\.)*(tenor|giphy|klipy|gfycat|imgur|media\.discordapp|cdn\.discordapp)\.\w+\S*", re.I)
ANSWER_WINDOW = 600  # seconds: a team post soon after a community question is probably the answer
SS58 = re.compile(r"\b5[1-9A-HJ-NP-Za-km-z]{47}\b")  # a wallet address → support for one person
GREETING = re.compile(r"^\s*(hi|hey|hello|gm|yo)\b(?!.*\b(all|everyone|folks|guys|miners|validators)\b)", re.I)

ANNOUNCE, XPOST, TEAM = "announcement", "x_post", "team"
LABEL = {ANNOUNCE: "📢 Announcement", XPOST: "🐦 X post", TEAM: "📰 Team update"}
COLOR = {ANNOUNCE: 0xF59E0B, XPOST: 0x1D9BF0, TEAM: 0x8B5CF6}


@dataclass
class Channel:
    id: str
    name: str
    netuid: int | None
    team_users: set[str] = field(default_factory=set)
    team_roles: set[str] = field(default_factory=set)
    last_id: int = 0


@dataclass
class Card:
    netuid: int | None
    msg_id: str | None
    payload: dict
    price0: int | None
    posted: float


class NewsMonitor:
    def __init__(self, token: str, discord, meta, price_now, change, guild_hint: str = "",
                 staff_words: str = "admin|moderator|\\bmods?\\b|staff|opentensor|foundation",
                 min_team_chars: int = 120, dry_run: bool = False) -> None:
        self.client = DiscordUser(token)
        self.discord = discord
        self.meta = meta
        self.price_now = price_now
        self.change = change
        self.guild_hint = guild_hint.strip()
        self.staff_re = re.compile(staff_words, re.I)
        self.min_team_chars = min_team_chars
        self.dry_run = dry_run
        self.guild_id: str | None = None
        self.guild_name = ""
        self.channels: dict[str, Channel] = {}
        self.roles: dict[str, str] = {}          # role id → name
        self.staff_roles: set[str] = set()
        self.member_roles: dict[str, list[str]] = {}
        self._seen: dict[str, None] = {}
        self._x_seen: dict[str, float] = {}
        self._fetching: set[str] = set()
        self._member_lookup_off = False
        self._recent: dict[str, list[tuple[float, str, bool]]] = {}  # channel → (time, author, asked a question)
        self.stats: dict[str, int] = {}
        self.posted = 0
        self.ready = False

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.client.start()
        await self.load_structure()
        self._restore_state()
        self.client.on_event = self._on_event
        self.client.on_ready = self._on_gateway_ready
        await self._catch_up()
        self.ready = True
        asyncio.create_task(self._sweep_loop())
        asyncio.create_task(self._structure_loop())

    async def load_structure(self) -> None:
        guilds = await self.client.get("/users/@me/guilds")
        g = None
        for cand in guilds:
            if self.guild_hint and (cand["id"] == self.guild_hint or cand["name"].lower() == self.guild_hint.lower()):
                g = cand
                break
        if g is None:
            named = [c for c in guilds if "bittensor" in c["name"].lower()]
            g = min(named, key=lambda c: len(c["name"])) if named else None
        if g is None:
            raise RuntimeError("the news account is not in a Bittensor server — join it, or set NEWS_GUILD_ID")
        self.guild_id, self.guild_name = g["id"], g["name"]
        guild = await self.client.get(f"/guilds/{self.guild_id}")
        self.roles = {r["id"]: r["name"] for r in guild.get("roles", [])}
        self.staff_roles = {
            r["id"] for r in guild.get("roles", [])
            if r["id"] != self.guild_id and (int(r.get("permissions", 0)) & ADMIN_BIT or self.staff_re.search(r["name"]))
        }
        chans = await self.client.get(f"/guilds/{self.guild_id}/channels")
        fresh: dict[str, Channel] = {}
        for c in chans:
            if c.get("type") not in (0, 5):  # text / announcement
                continue
            m = SUBNET_NAME_RE.match(c.get("name", ""))
            netuid = int(m.group(1)) if m else None
            if netuid is None:
                continue
            ch = Channel(c["id"], c["name"], netuid)
            for ow in c.get("permission_overwrites", []):
                if int(ow.get("allow", 0)) & TEAM_BITS and ow["id"] != self.guild_id:
                    (ch.team_users if ow.get("type") == 1 else ch.team_roles).add(ow["id"])
            old = self.channels.get(c["id"])
            ch.last_id = old.last_id if old else int(c.get("last_message_id") or 0)
            fresh[c["id"]] = ch
        self.channels = fresh

    async def _structure_loop(self) -> None:
        while True:
            await asyncio.sleep(1800)
            try:
                await self.load_structure()
            except Exception as e:
                log.warning("news: structure refresh failed: %s", e)

    async def _on_gateway_ready(self) -> None:
        if self.guild_id:
            await self.client.subscribe_guild(self.guild_id)

    # ── intake ───────────────────────────────────────────────────────────

    def _on_event(self, t: str, d: dict):
        if not isinstance(d, dict) or d.get("guild_id") != self.guild_id:
            return None
        if t in ("MESSAGE_CREATE", "PASSIVE_UPDATE_V1", "PASSIVE_UPDATE_V2"):
            self.stats[t] = self.stats.get(t, 0) + 1
        if t == "MESSAGE_CREATE":
            if d.get("channel_id") in self.channels:
                member = d.get("member") or {}
                if member.get("roles") is not None:
                    self.member_roles[d["author"]["id"]] = member["roles"]
                return self._handle(d)
        elif t in ("PASSIVE_UPDATE_V1", "PASSIVE_UPDATE_V2"):
            for c in d.get("channels") or []:
                ch = self.channels.get(c.get("id"))
                if ch and int(c.get("last_message_id") or 0) > ch.last_id:
                    asyncio.create_task(self._fetch(ch))
        return None

    async def _sweep_loop(self) -> None:
        """Safety net: one request lists every channel's last message id; fetch where it moved."""
        while True:
            await asyncio.sleep(20)
            try:
                chans = await self.client.get(f"/guilds/{self.guild_id}/channels")
            except Exception as e:
                log.debug("news sweep failed: %s", e)
                continue
            for c in chans:
                ch = self.channels.get(c["id"])
                if ch and int(c.get("last_message_id") or 0) > ch.last_id:
                    self.stats["sweep"] = self.stats.get("sweep", 0) + 1
                    asyncio.create_task(self._fetch(ch))

    async def _fetch(self, ch: Channel) -> None:
        if ch.id in self._fetching:
            return
        self._fetching.add(ch.id)
        try:
            msgs = await self.client.get(f"/channels/{ch.id}/messages", after=str(ch.last_id), limit=50)
            for m in sorted(msgs, key=lambda m: int(m["id"])):
                m.setdefault("guild_id", self.guild_id)
                await self._handle(m)
        except Exception as e:
            log.debug("news fetch #%s failed: %s", ch.name, e)
        finally:
            self._fetching.discard(ch.id)

    async def _catch_up(self) -> None:
        """After a restart, process what was posted while we were down (≤30 min ago)."""
        cutoff = time.time() - CATCHUP_SECONDS
        for ch in self.channels.values():
            if not ch.last_id:
                continue
            try:
                msgs = await self.client.get(f"/channels/{ch.id}/messages", after=str(ch.last_id), limit=50)
            except Exception:
                continue
            for m in sorted(msgs, key=lambda m: int(m["id"])):
                m.setdefault("guild_id", self.guild_id)
                if _ts(m) >= cutoff:
                    await self._handle(m)
                else:
                    ch.last_id = max(ch.last_id, int(m["id"]))
            await asyncio.sleep(0.3)
        self._save_state()

    # ── classification ───────────────────────────────────────────────────

    async def _ensure_roles(self, m: dict) -> None:
        """Messages fetched over REST don't carry the author's roles; look them up once per user."""
        uid = m["author"]["id"]
        member = m.get("member") or {}
        if member.get("roles") is not None:
            self.member_roles[uid] = member["roles"]
            return
        if uid in self.member_roles or self._member_lookup_off:
            return
        try:
            mem = await self.client.get(f"/guilds/{self.guild_id}/members/{uid}")
            self.member_roles[uid] = mem.get("roles", [])
        except PermissionError:
            self._member_lookup_off = True  # this account can't read members; rely on the gateway
        except Exception:
            self.member_roles[uid] = []

    def is_team(self, author_id: str, ch: Channel) -> str | None:
        """'team' (this subnet's own people), 'staff' (server moderators / Opentensor) or None."""
        if author_id in ch.team_users:
            return "team"
        roles = set(self.member_roles.get(author_id, ()))
        if roles & (ch.team_roles - self.staff_roles):
            return "team"
        if roles & (self.staff_roles | ch.team_roles):
            return "staff"
        return None

    def score(self, m: dict, ch: Channel) -> int:
        """How much a team post looks like news rather than conversation."""
        text = MEDIA.sub("", m.get("content") or "")  # GIFs and image hosts aren't links to news
        plain = URL_RE.sub("", text).strip()
        s = 0
        if URL_RE.search(text) or any(not (a.get("content_type") or "").endswith("gif") for a in m.get("attachments") or []):
            s += 3
        if STRUCTURE.search(text):
            s += 3
        if STRONG_WORDS.search(plain):
            s += 2
        if AUDIENCE.search(plain):
            s += 2
        if FIGURES.search(plain):
            s += 1  # splits, percentages, times, dates — policy changes and schedules
        if len(plain) >= 300:
            s += 2
        elif len(plain) >= self.min_team_chars:
            s += 1
        if CHATTY.search(plain) or GREETING.search(plain):
            s -= 3
        if SS58.search(plain):
            s -= 3
        if m.get("type") == 19 or any(u["id"] != m["author"]["id"] for u in m.get("mentions") or []):
            s -= 3  # directed at someone
        t = _ts(m)
        if any(who != m["author"]["id"] and asked and 0 <= t - ts <= ANSWER_WINDOW
               for ts, who, asked in self._recent.get(ch.id, [])[-6:]):
            s -= 2  # probably answering a question just asked
        if plain.endswith("?"):
            s -= 2
        return s

    def classify(self, m: dict, ch: Channel) -> tuple[str, str | None] | None:
        """→ (kind, who) or None. `who` is 'team' / 'staff' / None."""
        if m.get("type") not in (0, 19):  # normal message / reply
            return None
        content = m.get("content") or ""
        bot = m["author"].get("bot")
        who = None if bot else self.is_team(m["author"]["id"], ch)
        if m.get("mention_everyone") and not bot:
            return ANNOUNCE, who
        just_link = len(URL_RE.sub("", content).strip()) < 25
        bar = NEWS_SCORE if who == "team" else STAFF_SCORE
        if who and not (X_RE.search(content) and just_link) and self.score(m, ch) >= bar:
            return TEAM, who
        if X_RE.search(content):
            return XPOST, who
        return None

    def _remember(self, m: dict, ch: Channel) -> None:
        """Keep a little channel context (who just asked what) for the answer heuristic."""
        lst = self._recent.setdefault(ch.id, [])
        lst.append((_ts(m), m["author"]["id"], "?" in (m.get("content") or "")))
        del lst[:-12]

    async def _handle(self, m: dict) -> None:
        mid = m["id"]
        ch = self.channels.get(m.get("channel_id"))
        if ch is None or mid in self._seen:
            return
        self._seen[mid] = None
        if len(self._seen) > 5000:
            for k in list(self._seen)[:1000]:
                del self._seen[k]
        ch.last_id = max(ch.last_id, int(mid))
        if m.get("author", {}).get("id") == (self.client.user or {}).get("id"):
            return
        content = m.get("content") or ""
        maybe = (m.get("mention_everyone") or URL_RE.search(content) or m.get("attachments")
                 or len(content) >= self.min_team_chars or NEWS_WORDS.search(content))
        if maybe:
            await self._ensure_roles(m)
        verdict = self.classify(m, ch)
        self._remember(m, ch)
        if verdict is None:
            return
        kind, who = verdict
        if kind == XPOST and not who:
            xs = [s for _, s in X_RE.findall(m.get("content") or "")]
            now = time.time()
            if xs and all(now - self._x_seen.get(s, 0) < 12 * 3600 for s in xs):
                return  # this X post was already reported
            for s in xs:
                self._x_seen[s] = now
        self._save_state()
        await self._post(m, ch, kind, who)

    # ── cards ────────────────────────────────────────────────────────────

    def _clean(self, m: dict) -> str:
        text = m.get("content") or ""
        users = {u["id"]: (u.get("global_name") or u.get("username") or "user") for u in m.get("mentions", [])}
        text = re.sub(r"<@!?(\d+)>", lambda x: "@" + users.get(x.group(1), "user"), text)
        text = re.sub(r"<@&(\d+)>", lambda x: "@" + self.roles.get(x.group(1), "role"), text)
        text = re.sub(r"<#(\d+)>", lambda x: "#" + (self.channels[x.group(1)].name if x.group(1) in self.channels else "channel"), text)
        text = re.sub(r"<a?:(\w+):\d+>", r":\1:", text)
        return text.strip()

    def build(self, m: dict, ch: Channel, kind: str, who: str | None) -> tuple[dict, int | None]:
        a = m["author"]
        name = (m.get("member") or {}).get("nick") or a.get("global_name") or a.get("username") or "unknown"
        badge = {"team": " · subnet team", "staff": " · server staff"}.get(who or "", "")
        avatar = (f"https://cdn.discordapp.com/avatars/{a['id']}/{a['avatar']}.png?size=128" if a.get("avatar")
                  else "https://cdn.discordapp.com/embed/avatars/0.png")
        jump = f"https://discord.com/channels/{self.guild_id}/{ch.id}/{m['id']}"
        text = self._clean(m)
        info = self.meta.info(ch.netuid) if ch.netuid is not None else None
        sub = f"SN{ch.netuid} {info.name}".strip() if info else f"#{ch.name}"
        label = LABEL[kind] + (" by the team" if kind == XPOST and who else "")

        links = list(dict.fromkeys(URL_RE.findall(m.get("content") or "")))
        words = URL_RE.sub("", text).strip()
        if not words and links:  # the post is just a link: say what it is, Discord previews it below
            text = "shared an X post" if X_RE.match(links[0]) else "shared a link"
            text = f"*{text}*"
        preview = text.replace("\n", " ")
        preview = preview if len(preview) <= 140 else preview[:137].rstrip() + "…"
        lines = [f"{LABEL[kind].split()[0]} **{sub}** · {name}{badge}: {preview}"]
        unfurl = [u for u in links if X_RE.match(u)][:1] or links[:1]  # Discord renders a native preview

        p0 = self.price_now(ch.netuid) if ch.netuid is not None else None
        fields = [{"name": "Posted by", "value": f"**{name}**{badge.replace(' · ', chr(10))}", "inline": True},
                  {"name": "Channel", "value": f"[#{ch.name}]({jump})", "inline": True}]
        if p0:
            ch1h = self.change(ch.netuid, 300)
            fields.append({"name": "Price at post", "value": f"**{fmt.price(p0)} τ**"
                           + (f"\n1h {fmt.pct(ch1h)}" if ch1h is not None else ""), "inline": True})
        if links:
            fields.append({"name": "Links", "value": "\n".join(f"<{u}>" for u in links[:5])[:1024], "inline": False})

        embed = {
            "author": alerts._author(info) if info else {"name": f"#{ch.name}"},
            "title": label,
            "url": jump,
            "description": (text[:1800] + ("…" if len(text) > 1800 else "")) or "*(attachment)*",
            "color": COLOR[kind],
            "fields": fields,
            "thumbnail": {"url": avatar},
            "footer": {"text": f"{self.guild_name} · #{ch.name} · open the message with the title link"},
            "timestamp": m.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        }
        img = next((x.get("url") for x in m.get("attachments", [])
                    if (x.get("content_type") or "").startswith("image/")), None)
        if img:
            embed["image"] = {"url": img}
        payload = {"content": alerts.content(lines + unfurl), "embeds": [embed], "allowed_mentions": {"parse": []}}
        return payload, p0

    async def tweet(self, url: str) -> dict | None:
        """The tweet behind an x.com link (author, text, image) via the public fxtwitter API — X often
        blocks Discord's own link preview, so the card carries the tweet itself."""
        mt = X_RE.search(url)
        if not mt:
            return None
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as s:
                async with s.get(f"https://api.fxtwitter.com/{mt.group(1)}/status/{mt.group(2)}") as r:
                    t = (await r.json(content_type=None)).get("tweet") if r.status == 200 else None
        except Exception as e:
            log.debug("tweet fetch failed: %s", e)
            return None
        if not t:
            return None
        a = t.get("author") or {}
        emb = {
            "author": {"name": f"{a.get('name', '')} (@{a.get('screen_name', '')})",
                       "url": f"https://x.com/{a.get('screen_name', '')}", "icon_url": a.get("avatar_url")},
            "description": (t.get("text") or "")[:1500],
            "url": t.get("url") or url,
            "color": COLOR[XPOST],
            "footer": {"text": f"X · ♥ {t.get('likes', 0)} · ⟲ {t.get('retweets', 0)} · 💬 {t.get('replies', 0)}"},
        }
        photos = (t.get("media") or {}).get("photos") or []
        if photos:
            emb["image"] = {"url": photos[0].get("url")}
        if t.get("created_timestamp"):
            emb["timestamp"] = datetime.fromtimestamp(t["created_timestamp"], timezone.utc).isoformat()
        return emb

    async def _post(self, m: dict, ch: Channel, kind: str, who: str | None) -> None:
        payload, p0 = self.build(m, ch, kind, who)
        x = X_RE.search(m.get("content") or "")
        if x:
            tw = await self.tweet(x.group(0))
            lines = payload["content"].split("\n")
            lines = [ln for ln in lines if not X_RE.fullmatch(ln.strip())]  # drop the bare link line
            if tw:
                payload["embeds"].append(tw)
                handle = tw["author"]["name"].split("(")[-1].rstrip(")")
                quote = tw["description"].replace("\n", " ")
                quote = quote if len(quote) <= 120 else quote[:117].rstrip() + "…"
                lines.append(f"> {handle}: {quote}")
            else:  # fall back to a link Discord can preview
                lines.append(f"https://fxtwitter.com/{x.group(1)}/status/{x.group(2)}")
            payload["content"] = "\n".join(lines)
        head = payload["content"].strip().splitlines()[0]
        print(fmt.byellow(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {head[:160]}"))
        self.posted += 1
        if self.dry_run:
            return
        msg_id = await self.discord.send(payload)
        if msg_id and p0:
            asyncio.create_task(self._reaction(Card(ch.netuid, msg_id, payload, p0, time.time())))

    async def _reaction(self, card: Card) -> None:
        """Edit the card at +5m / +15m / +1h with how the price moved since the post."""
        marks = []
        for minutes in REACTION_MINUTES:
            await asyncio.sleep(max(0, card.posted + minutes * 60 - time.time()))
            now = self.price_now(card.netuid)
            if not now:
                continue
            a, b, pc = fmt.move(card.price0, now)
            marks.append(f"+{minutes}m **{fmt.pct(pc)}**" if minutes < 60 else f"+1h **{fmt.pct(pc)}**")
            emb = card.payload["embeds"][0]
            field = {"name": "Market reaction", "value": " · ".join(marks) + f"\n{a} → {b} τ", "inline": False}
            emb["fields"] = [f for f in emb["fields"] if f["name"] != "Market reaction"] + [field]
            self.discord.edit_later(card.msg_id, {"embeds": card.payload["embeds"]})

    # ── persistence ──────────────────────────────────────────────────────

    def _restore_state(self) -> None:
        try:
            state = json.loads(STATE_FILE.read_text())
        except (OSError, ValueError):
            return
        for cid, last in state.get("last", {}).items():
            if cid in self.channels:
                self.channels[cid].last_id = max(int(last), 0) or self.channels[cid].last_id
        self._x_seen = {k: v for k, v in state.get("x", {}).items() if time.time() - v < 12 * 3600}

    def _save_state(self) -> None:
        try:
            STATE_FILE.write_text(json.dumps({"last": {c.id: c.last_id for c in self.channels.values()},
                                              "x": self._x_seen}))
        except OSError:
            pass


def _ts(m: dict) -> float:
    try:
        return datetime.fromisoformat(m["timestamp"]).timestamp()
    except (KeyError, ValueError):
        return time.time()
