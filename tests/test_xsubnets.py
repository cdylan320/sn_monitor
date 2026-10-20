"""Run: .venv/bin/python tests/test_xsubnets.py — every subnet's own X account: found, checked, watched."""
import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon import xsubnets  # noqa: E402
from snmon.meta import SubnetInfo  # noqa: E402
from snmon.news import XPOST, Channel, NewsMonitor, x_own_post  # noqa: E402

xsubnets.CACHE_FILE = Path(tempfile.mkdtemp()) / "x_subnets.json"
xsubnets.PROFILE_PACE_S = 0


def sub(n, name, url, github=""):
    return SubnetInfo(n, name, "", 10_000.0, 1, None, url, identity={"name": name, "url": url, "github": github})


SUBNETS = {85: sub(85, "Vidaio", "https://vidaio.io/", "https://github.com/vidaio-subnet/vidaio-subnet"),
           8: sub(8, "Vanta", "https://www.vantanetwork.io/"),
           21: sub(21, "Any", "https://any.example"),
           64: sub(64, "Chutes", "https://chutes.ai", "https://github.com/chutesai/chutes")}
SITES = {
    # the real SN85 homepage links the subnet twice and two founders once each
    "https://vidaio.io/": '<a href="https://x.com/vidaio_">X</a> <a href="https://twitter.com/vidaio_/">t</a> '
                          '<a href="https://x.com/ArpanTripathi20">f</a> <a href="https://x.com/IchekeChinaza">f</a> '
                          '<a href="https://twitter.com/intent/tweet?text=hi">share</a> <a href="https://x.com/const_reborn">c</a>',
    "https://www.vantanetwork.io/": '<a href="https://x.com/VantaTrading">X</a>',
    "https://any.example": '<a href="https://x.com/ppcrebel">X</a>',
    "https://chutes.ai": "<html>a page built in JavaScript: no X link here</html>",
    "https://raw.githubusercontent.com/chutesai/chutes/HEAD/README.md": "[![X](badge)](https://x.com/chutes_ai)",
}
PROFILES = {
    "vidaio_": {"name": "Vidaio", "description": "AI video", "website": {"url": "https://vidaio.io"}},
    "VantaTrading": {"name": "Vanta", "description": "Prop trading on Bittensor (SN8)", "website": {"url": "https://vantatrading.io"}},
    "ppcrebel": {"name": "Pavel", "description": "Group Head of Digital Marketing at @heurekacz"},
    "chutes_ai": {"name": "Chutes", "description": "The most secure AI inference", "website": {"url": "http://chutes.ai"}},
}


class _Web:
    """A pretend web: GET a page → its HTML (404 when unknown)."""
    def __init__(self):
        self.gets = []

    def get(self, url, **kw):
        web = self

        class R:
            async def __aenter__(self):
                web.gets.append(url)
                self.status = 200 if url in SITES else 404
                return self

            async def __aexit__(self, *a):
                return False

            async def text(self, errors=None):
                return SITES.get(url, "")
        return R()


def profiles(answers=PROFILES):
    asked = []

    async def profile(session, handle):
        asked.append(handle)
        return answers.get(handle)
    profile.asked = asked
    return profile


def test_the_account_a_subnet_site_links_most_is_its_candidate_and_x_pages_and_const_are_skipped():
    c = xsubnets.handles_in(SITES["https://vidaio.io/"])
    assert c.most_common(1)[0] == ("vidaio_", 2) and "intent" not in c and "const_reborn" not in c


def test_a_handle_counts_only_when_its_profile_points_back_to_the_subnet():
    ok = xsubnets.checks_out
    assert ok(85, "Vidaio", "https://vidaio.io/", "vidaio_", PROFILES["vidaio_"]) == "website"
    assert ok(8, "Vanta", "https://www.vantanetwork.io/", "VantaTrading", PROFILES["VantaTrading"]) == "bio"   # SN8 in the bio
    assert ok(9, "iota", "https://iota.macrocosmos.ai/", "macrocosmosai", {"name": "Macrocosmos"}) == ""   # "iota" is 4 letters but not there
    assert ok(81, "Reliquary", "https://reliquary.ai", "reliquary_ai", {"name": "x"}) == "name"           # the handle names it
    assert ok(21, "Any", "https://any.example", "ppcrebel", PROFILES["ppcrebel"]) == ""                     # SN21's real case
    assert ok(8, "Vanta", "", "x", {"description": "building on SN85"}) == ""                              # SN85 is not SN8


def test_discovery_finds_checks_and_keeps_the_accounts():
    async def go():
        acc = xsubnets.SubnetAccounts(skip={"const_reborn"})
        prof = profiles()
        assert await acc.refresh(_Web(), SUBNETS, prof) == 4
        assert acc.by_netuid() == {85: "vidaio_", 8: "VantaTrading", 64: "chutes_ai"}      # SN21: not the subnet's
        assert acc.found[64]["src"] == "github" and acc.found[21]["why"] == "not the subnet's"
        assert await acc.refresh(_Web(), SUBNETS, prof) == 0                                # nothing due: kept
        again = xsubnets.SubnetAccounts(skip={"const_reborn"})                               # after a restart: from the file
        assert again.by_netuid() == acc.by_netuid()
        moved = {**SUBNETS, 8: sub(8, "Vanta", "https://www.vantatrading.io/")}            # its website changed: looked at again
        assert again.due(moved) == [8]
    asyncio.run(go())


def test_an_unreadable_profile_is_retried_after_an_hour_and_handles_can_be_set_by_hand():
    async def go():
        xsubnets.CACHE_FILE.unlink(missing_ok=True)
        acc = xsubnets.SubnetAccounts({21: "", 72: "NATIXNetwork"})
        await acc.refresh(_Web(), {85: SUBNETS[85]}, profiles({}))
        assert acc.found[85]["why"] == "unreadable" and acc.due({85: SUBNETS[85]}) == []
        acc.found[85]["at"] -= 3600
        assert acc.due({85: SUBNETS[85]}) == [85]
        assert acc.by_netuid() == {72: "NATIXNetwork"} and acc.due({21: SUBNETS[21]}) == []   # by hand: never looked up
        acc.overrides[64], acc.overrides[8] = "chutes_ai", "chutes_ai"                       # one team, two subnets
        assert acc.accounts()["chutes_ai"] == ("chutes_ai", {8, 64})
    asyncio.run(go())


class _Meta:
    subnets = {85: SUBNETS[85]}

    def info(self, n):
        return SUBNETS.get(n)


def nm():
    m = NewsMonitor("x", None, _Meta(), price_now=lambda n: 5_380_000, change=lambda n, b: 1.99)
    m.guild_id = "G"
    m._save_state = lambda: None
    return m


def tweet(i, author="vidaio_", age=5.0, **kw):
    return {"id": str(i), "author": {"screen_name": author}, "text": f"post {i}", "url": f"https://x.com/{author}/status/{i}",
            "created_timestamp": time.time() - age, **kw}


class _X:
    """A pretend fxtwitter timeline, newest first."""
    def __init__(self, items):
        self.items = items

    def get(self, url, **kw):
        x = self

        class R:
            status, headers = 200, {}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self, content_type=None):
                return {"results": list(x.items)}
        return R()


def test_a_subnet_accounts_own_new_post_is_news_for_its_subnet_at_once():
    async def go():
        m = nm()
        posted = []

        async def post_x(t, h, parent=None, feed_kind="x_account", netuid=None):
            posted.append((t["id"], feed_kind, netuid))
        m._post_x = post_x
        x = _X([tweet(1, age=3000)])
        await m._subnet_x_look(x, "vidaio_", {85})                          # first look: only recorded
        assert posted == []
        x.items = [tweet(6), tweet(5, replying_to={"screen_name": "someone"}),       # newest first
                   tweet(4, author="partner", reposted_by={"screen_name": "vidaio_"}),
                   tweet(3, age=3600), tweet(2, quote={"text": "q"}), tweet(1, age=3000)]
        await m._subnet_x_look(x, "vidaio_", {85})
        assert posted == [("2", "x_subnet", 85), ("6", "x_subnet", 85)]      # the quote and the post; no reply, repost or old post
        assert "2" in m._x_sub and "6" in m._x_sub
        await m._subnet_x_look(x, "vidaio_", {85})                          # a later look: nothing twice
        assert len(posted) == 2
    asyncio.run(go())


def test_the_subnet_post_reaches_the_bots_as_x_subnet():
    m = nm()
    sent = []

    class Feed:
        def publish(self, ev):
            sent.append(ev)
            return 1
    m.news_feed = Feed()
    m.discord = None
    m.dry_run = True
    asyncio.run(m._post_x(tweet(7), "vidaio_", feed_kind="x_subnet", netuid=85))
    assert [(e["netuid"], e["kind"], e["who"]) for e in sent] == [(85, "x_subnet", "vidaio_")]


def test_a_discord_share_of_the_subnets_own_post_is_not_reported_again():
    async def go():
        m = nm()
        ch = Channel("C85", "85・vidaio", 85, team_users={"HELIOS"})
        m.channels = {"C85": ch}
        m.client.user = {"id": "ME"}
        posts = []

        async def no_roles(msg):
            return None

        async def post(msg, c, kind, who):
            posts.append((msg["id"], kind, who))
        m._ensure_roles, m._post = no_roles, post
        share = lambda i, s: {"id": i, "channel_id": "C85", "type": 0, "content": f"https://x.com/vidaio_/status/{s}",
                              "author": {"id": "HELIOS", "username": "helios"}, "mentions": [], "attachments": [],
                              "timestamp": "2026-10-20T13:04:00+00:00"}
        m._x_sub["2112620822057881829"] = time.time() - 60                  # seen on @vidaio_ a minute ago
        await m._handle(share("101", "2112620822057881829"))
        await m._handle(share("102", "999"))                                 # a tweet the watcher did not see
        assert posts == [("102", XPOST, "team")]
    asyncio.run(go())


def test_own_post_means_written_by_the_account_not_a_reply_or_repost():
    assert x_own_post(tweet(1), "Vidaio_") and x_own_post(tweet(2, quote={"text": "q"}), "vidaio_")
    assert not x_own_post(tweet(3, replying_to={"screen_name": "vidaio_"}), "vidaio_")     # its thread's next part
    assert not x_own_post(tweet(4, author="p", reposted_by={"screen_name": "vidaio_"}), "vidaio_")


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} subnet X account tests passed")
