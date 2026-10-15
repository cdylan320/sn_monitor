"""Run: .venv/bin/python tests/test_news.py"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.news import ANNOUNCE, TEAM, XPOST, Channel, NewsMonitor, subnet_named, x_by, x_new  # noqa: E402


class _Meta:
    def info(self, n):
        from snmon.meta import SubnetInfo
        return SubnetInfo(n, "umi", "", 1393.0, 1, None, None)


def nm():
    m = NewsMonitor("x", None, _Meta(), price_now=lambda n: 3_340_000, change=lambda n, b: 1.5)
    m.guild_id, m.guild_name = "G", "Bittensor"
    m.roles = {"R_OWNER": "SN78 Owner", "R_MOD": "Moderator"}
    m.staff_roles = {"R_MOD"}
    return m


CH = Channel("C78", "78・umi・و", 78, team_users={"SAM"}, team_roles={"R_OWNER"})


def msg(author, content, **kw):
    return {"id": kw.pop("id", "100"), "channel_id": "C78", "guild_id": "G", "type": kw.pop("type", 0),
            "author": {"id": author, "username": author.lower(), "global_name": author, "avatar": None},
            "content": content, "mentions": [], "attachments": [], "timestamp": "2026-10-01T11:18:00+00:00", **kw}


def test_owner_announcement_with_link_is_team_news():
    m = nm()
    v = m.classify(msg("SAM", "ok folks, enrollment for C5 is open, feel free to start enrolling your miners for C5 "
                              "using the following upgrade instructions: https://github.com/Umi-BitSign/umi/blob/main/"
                              "docs/miners/connection.md"), CH)
    assert v == (TEAM, "team")


def test_x_link_by_community_member():
    m = nm()
    assert m.classify(msg("HELIOS", "https://x.com/markjeffrey/status/2105702079847387489"), CH) == (XPOST, None)


def test_everyone_ping_is_announcement():
    m = nm()
    assert m.classify(msg("SAM", "@everyone maintenance in 10 min", mention_everyone=True), CH)[0] == ANNOUNCE


def test_team_short_reply_is_not_news():
    m = nm()
    assert m.classify(msg("SAM", "yes that's expected, restart your miner", type=19), CH) is None
    assert m.classify(msg("SAM", "thanks!"), CH) is None


def test_role_based_team_and_staff():
    m = nm()
    m.member_roles = {"BOB": ["R_OWNER"], "MOD": ["R_MOD"]}
    assert m.classify(msg("BOB", "New release v1.4.0 is live, please update your validators"), CH) == (TEAM, "team")
    assert m.classify(msg("MOD", "Heads up everyone: the next round of the subnet competition starts today "
                                 "https://bittensor.com/competitions"), CH) == (TEAM, "staff")


def test_real_sn78_team_chatter_is_ignored():
    # actual sam0x17 answers from #78 — conversation, not news
    m = nm()
    for text in ("yes, dealing with some bs, the server is too slow to complete the validation within the allotted time "
                 "so I'm moving it to a bigger box tonight",
                 "no it will have emission as long as I keep renewing",
                 "when it is ready, It's going to run automatically for C5-C10 but I'm still building the new pipeline "
                 "so it will take a few more days",
                 "yeah C5 is not ready yet. There are many, many changes and improvements we are making based on C4"):
        assert m.classify(msg("SAM", text), CH) is None, text


def test_real_sn78_team_news_is_kept():
    m = nm()
    for text in ("C4 rewards are now reaching miners! We confirmed on-chain payments to all 106 eligible recipients.",
                 "update: someone gamed the IP stuff again in the C4 rewards. Going to do two things: 1) ASN checks 2) bans",
                 "the miner upgrade script had several issues that are now fixed, should get confirmation that all miners "
                 "are upgraded soon",
                 "C5+ will be 50/50 split instead of 70/30 split for the publicly released model track because more "
                 "teams are competing"):
        assert m.classify(msg("SAM", text), CH) == (TEAM, "team"), text


def test_staff_moderation_and_gifs_are_ignored():
    m = nm()
    m.member_roles = {"MOD": ["R_MOD"]}
    assert m.classify(msg("MOD", "Hi. Just a quick reminder for everyone: the subnet owners aren't allowed to discuss "
                                 "the token price in this channel, please keep it on topic"), CH) is None
    assert m.classify(msg("MOD", "https://klipy.com/gifs/wish-you-7"), CH) is None
    assert m.classify(msg("SAM", "https://tenor.com/view/party-gif-123"), CH) is None


def test_wallet_support_reply_is_ignored():
    m = nm()
    assert m.classify(msg("SAM", "Hi! I checked hotkey `5Fgbg98sArvah9HJBEAkdQvukiWPRSeovnC8xN3oUSwD3wjz`: it is registered "
                                 "on SN78 and enrolled for C4, results for it are now published in the dashboard, "
                                 "rewards will follow once the payout job runs tonight"), CH) is None


def test_answer_right_after_a_question_is_ignored():
    m = nm()
    m._remember(msg("RANDO", "is C5 going to have the same reward split as C4?", id="99"), CH)
    assert m.classify(msg("SAM", "the split changes a bit, we will post the details for the next cohort later this week "
                                 "once the numbers are final"), CH) is None


def test_community_chatter_is_ignored():
    m = nm()
    assert m.classify(msg("RANDO", "wen moon? price is pumping lol https://taostats.io/subnets/78"), CH) is None
    assert m.classify(msg("RANDO", "gm"), CH) is None


class _Feed:
    def __init__(self):
        self.events = []

    def publish(self, ev):
        self.events.append(ev)
        return 1


def test_news_reaches_the_trading_bots_before_the_card_with_its_kind():
    m = nm()
    m.dry_run = True
    m.news_feed = _Feed()
    m.channels = {"C78": CH}

    async def no_tweet(url):
        return None
    m.tweet = no_tweet
    sent = []

    async def send(payload, netuid, p0):
        sent.append(len(m.news_feed.events))                     # how many events were out when the card went
    m._send = send
    asyncio.run(m._post(msg("SAM", "@everyone mainnet launch today", mention_everyone=True), CH, ANNOUNCE, "team"))
    asyncio.run(m._post(msg("HELIOS", "https://x.com/a/status/1", id="101"), CH, XPOST, None))
    asyncio.run(m._post(msg("SAM", "https://x.com/a/status/2", id="102"), CH, XPOST, "team"))
    ev = m.news_feed.events
    assert [e["kind"] for e in ev] == ["announcement", "x_other", "x_team"] and sent == [1, 2, 3]
    assert ev[0]["netuid"] == 78 and ev[0]["price"] == 3_340_000 and ev[0]["who"] == "team"
    assert ev[0]["url"].endswith("/C78/100") and abs(ev[0]["posted"] - 1790853480.0) < 1e5


def test_no_feed_no_events_and_a_post_without_a_subnet_is_not_sent():
    m = nm()
    m._publish(78, "team", "team", "discord", "x", "", 0.0)    # no feed: nothing happens
    m.news_feed = _Feed()
    m._publish(None, "team", "team", "discord", "x", "", 0.0)
    assert m.news_feed.events == []


def test_card_layout():
    m = nm()
    payload, p0 = m.build(msg("SAM", "enrollment for C5 is open: https://github.com/Umi-BitSign/umi <@123>",
                              mentions=[{"id": "123", "username": "helios"}]), CH, TEAM, "team")
    e = payload["embeds"][0]
    assert payload["content"].startswith("​\n📰 **SN78 umi** · SAM · subnet team: enrollment for C5 is open")
    assert "https://github.com/Umi-BitSign/umi" in payload["content"]   # unfurled by Discord
    assert e["url"] == "https://discord.com/channels/G/C78/100"
    assert "@helios" in e["description"] and p0 == 3_340_000
    assert [f["name"] for f in e["fields"]] == ["Posted by", "Channel", "Price at post", "Links"]


NOW = 1_800_000_000


def _tw(i, age_h, repost=False):
    t = {"id": str(i), "created_timestamp": NOW - age_h * 3600}
    if repost:
        t["reposted_by"] = {"screen_name": "const_reborn"}
    return t


def test_a_subnet_named_in_a_post_is_found_by_its_name():
    names = {114: "SOMA", 64: "Chutes", 44: "Score", 16: "kenju", 2: "DSperse"}
    assert subnet_named("Why is SOMA such a great compressor in the #Bittensor ecosystem?", names) == 114
    assert subnet_named("Soma is great", names) == 114 and subnet_named("Bullish on CHUTES", names) == 64
    assert subnet_named("check out Kenju", names) == 16
    assert subnet_named("soma is great", names) is None                          # a plain lowercase word
    assert subnet_named("The Score is high", names) is None                       # an ordinary-word name
    assert subnet_named("SOMA and Chutes both shipped", names) is None            # two subnets: ambiguous
    assert subnet_named("see https://example.com/SOMA/page", names) is None       # inside a link


def test_x_repost_of_an_old_tweet_is_news():
    # 2026-10-03: const reposted a tweet written 4 days earlier; the API dates it by the original
    timeline = [_tw(9, 98.8, repost=True), _tw(8, 18), _tw(7, 21), _tw(6, 43)]
    assert [t["id"] for t in x_new(timeline, {"8", "7", "6"}, NOW)] == ["9"]


def test_x_older_tweets_scrolling_into_view_are_not_news():
    timeline = [_tw(8, 18), _tw(7, 21), _tw(6, 43), _tw(5, 70, repost=True), _tw(4, 90)]
    assert x_new(timeline, {"8", "7", "6"}, NOW) == []


def test_x_new_items_come_oldest_first_and_stale_own_posts_are_skipped():
    timeline = [_tw(12, 0.1), _tw(11, 0.5, repost=True), _tw(10, 30), _tw(8, 40)]
    assert [t["id"] for t in x_new(timeline, {"8"}, NOW)] == ["11", "12"]   # 10 is his own, 30h old


def test_x_quote_and_reply_missing_from_the_plain_timeline_are_caught():
    # 2026-10-04: a quote-post and a reply to another user only show in the with_replies timeline
    timeline = [_tw(20, 12.1), {**_tw(21, 12.0), "replying_to": {"screen_name": "KeithSingery"}},
                _tw(15, 37, repost=True), _tw(14, 80, repost=True), _tw(13, 58)]
    got = x_new(timeline, {"15", "14"}, NOW)
    assert [t["id"] for t in got] == ["20", "21"]   # written 20 then 21; item 13 is his own and 58h old → not news


def test_x_only_the_accounts_own_actions_count():
    # the with-replies timeline lists the tweet he replied to (someone else's) right above his reply
    theirs = {"id": "20", "author": {"screen_name": "KeithSingery"}, "quote": {"text": "x"}}
    reply = {"id": "21", "author": {"screen_name": "const_reborn"}, "replying_to": {"screen_name": "KeithSingery", "status": "20"}}
    repost = {"id": "15", "author": {"screen_name": "jon_durbin"}, "reposted_by": {"screen_name": "Const_Reborn"}}
    assert not x_by(theirs, "const_reborn")
    assert x_by(reply, "const_reborn") and x_by(repost, "const_reborn")


class _Timeline:
    """A pretend fxtwitter: every request takes `latency` seconds; const's tweet "p2" shows up `appear_at` seconds after start."""
    def __init__(self, latency, appear_at, first_status=200, retry_after=None):
        self.t0, self.latency, self.appear_at = time.time(), latency, appear_at
        self.requests = self.inflight = self.peak = 0
        self.first_status, self.retry_after, self.times = first_status, retry_after, []

    def get(self, url):
        tl = self

        class R:
            status = 200
            headers = {}

            async def __aenter__(self):
                tl.requests += 1; tl.inflight += 1; tl.peak = max(tl.peak, tl.inflight); tl.times.append(time.time() - tl.t0)
                if tl.requests == 1 and tl.first_status != 200:
                    self.status = tl.first_status
                    self.headers = {"Retry-After": str(tl.retry_after)} if tl.retry_after else {}
                await asyncio.sleep(tl.latency)
                tl.inflight -= 1
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self, content_type=None):
                items = [{"id": "p1", "author": {"screen_name": "const_reborn"}, "text": "gm"}]
                if time.time() - tl.t0 >= tl.appear_at:
                    items.insert(0, {"id": "p2", "author": {"screen_name": "const_reborn"}, "text": "SN46 is up"})
                return {"results": items}
        return R()


def _run_x_loop(tl, seconds, post_delay=0.0, every=0.05):
    async def go():
        import snmon.news as N
        m = nm()
        posted = []

        async def post_x(t, h, parent=None):
            posted.append((t["id"], time.time() - tl.t0))
            await asyncio.sleep(post_delay)          # posting the Discord card takes a moment
        m._post_x, m._save_state = post_x, lambda: None
        task = asyncio.create_task(m._x_poll_loop(tl, ["const_reborn"], every))
        await asyncio.sleep(seconds)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return posted
    return asyncio.run(go())


def test_x_overlapping_looks_post_a_new_tweet_once_even_while_its_card_is_still_being_posted():
    tl = _Timeline(latency=0.4, appear_at=1.0)
    posted = _run_x_loop(tl, 2.4, post_delay=0.6)          # the card takes longer than a look: other looks come back meanwhile
    assert [p[0] for p in posted] == ["p2"], posted        # exactly once (p1 was only recorded by the first look)
    assert posted[0][1] - 1.0 <= 0.4 + 0.25               # about one answer time after the tweet
    assert tl.peak >= 3                                    # the looks really overlapped


def test_x_a_429_pauses_the_looks_for_the_time_the_server_asks():
    tl = _Timeline(latency=0.2, appear_at=99, first_status=429, retry_after=1)
    _run_x_loop(tl, 2.2)
    assert len([t for t in tl.times if t < 0.9]) == 1, tl.times     # slow start: one look, throttled, nothing else is sent
    assert len([t for t in tl.times if t >= 1.0]) >= 3, tl.times    # after the pause the looks go on


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} news tests passed")
