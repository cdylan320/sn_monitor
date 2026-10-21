"""Run: .venv/bin/python tests/test_xlist.py — every subnet X account in one request: the private X list."""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.news import NewsMonitor, x_own_post  # noqa: E402
from snmon.xlist import XList, XLoginError, posted_at, timeline_items, to_item  # noqa: E402

# 21 Oct, read from the list (public post fields only): a post, a reply, a quote and a repost by subnet accounts
SAMPLES = json.load(open(os.path.join(os.path.dirname(__file__), "x_list_samples.json")))


def test_x_posts_become_the_items_the_monitor_reads_from_fxtwitter():
    post, reply, quote, repost = (to_item(SAMPLES[k]) for k in ("post", "reply", "quote", "repost"))
    assert post["id"] == "2112978075311014316" and post["author"]["screen_name"] == "xeveryframe"
    assert post["url"] == "https://x.com/xeveryframe/status/2112978075311014316"
    assert post["text"].startswith("👀\n\nNow live on EveryFrame: FLUX 3 MAX") and "t.co" not in post["text"]
    assert abs(post["created_timestamp"] - posted_at(post["id"])) < 1e-6
    assert time.strftime("%H:%M:%S", time.gmtime(post["created_timestamp"])) == "18:43:25"     # = created_at
    assert reply["replying_to"] == {"screen_name": "yanez__ai", "status": "2112960651329224953"}
    assert reply["text"] == "Can this robot Pulse too?" and len(reply["media"]["photos"]) == 2   # the photo link dropped
    assert quote["quote"]["author"]["screen_name"] == "IOTA_SN9" and quote["quote"]["text"].startswith("The swarm just got faster")
    assert repost["reposted_by"]["screen_name"] == "TPN_Labs" and repost["author"]["screen_name"] == "MuffinDannyH"
    # what counts for a subnet account: its own posts and quotes, not replies or reposts
    assert x_own_post(post, "xeveryframe") and x_own_post(quote, "MacrocosmosAI")
    assert not x_own_post(reply, "yanez__ai") and not x_own_post(repost, "TPN_Labs")


def test_a_timeline_answer_gives_each_post_once_newest_first():
    entry = lambda r: {"content": {"itemContent": {"tweet_results": {"result": r}}}}
    data = {"data": {"list": {"tweets_timeline": {"timeline": {"instructions": [{"entries": [
        entry(SAMPLES["reply"]), entry(SAMPLES["post"]), entry(SAMPLES["quote"]),
        {"content": {"items": [{"item": {"itemContent": {"tweet_results": {"result": SAMPLES["post"]}}}}]}},   # in a thread too
        entry({"__typename": "TweetWithVisibilityResults", "tweet": SAMPLES["repost"]})]}]}}}}}
    items = timeline_items(data)
    assert [i["id"] for i in items] == sorted({i["id"] for i in items}, key=int, reverse=True)
    assert len(items) == 4 and sum(i["id"] == "2112978075311014316" for i in items) == 1


def nm():
    from snmon.meta import SubnetInfo

    class Meta:
        subnets = {54: SubnetInfo(54, "Yanez", "", 5000.0, 1, None, None)}

        def info(self, n):
            return self.subnets.get(n)
    m = NewsMonitor("x", None, Meta(), price_now=lambda n: 5_000_000, change=lambda n, b: 0.0)
    m._save_state = lambda: None
    return m


def post(i, author="yanez__ai", age=3.0, **kw):
    return {"id": str(i), "author": {"screen_name": author}, "text": f"post {i}", "url": f"https://x.com/{author}/status/{i}",
            "created_timestamp": time.time() - age, **kw}


def test_from_the_list_a_new_own_post_is_sent_once_and_fxtwitter_does_not_send_it_again():
    async def go():
        m = nm()
        sent = []

        async def post_x(t, h, parent=None, feed_kind="x_account", netuid=None):
            sent.append((t["id"], feed_kind, netuid))
        m._post_x = post_x
        await m._subnet_x_items("yanez__ai", {54}, [post(1, age=4000)], by_position=False, source="xl")   # first look: recorded
        assert sent == []
        # the list holds only the account's last few posts — none of them known: still new (SN54, 21 Oct)
        items = [post(5, replying_to={"screen_name": "yanez__ai", "status": "4"}), post(4),
                 post(3, author="other", reposted_by={"screen_name": "yanez__ai"}), post(2, age=900)]
        await m._subnet_x_items("yanez__ai", {54}, items, by_position=False, source="xl")
        assert sent == [("4", "x_subnet", 54)]                       # not the reply, the repost, or the 15-min-old post
        assert m.stats["xl_first"] == 1
        await m._subnet_x_items("yanez__ai", {54}, items, by_position=False, source="xl")
        await m._subnet_x_items("yanez__ai", {54}, [post(5), post(4), post(1, age=4000)])   # fxtwitter sees it later
        assert sent == [("4", "x_subnet", 54)]
    asyncio.run(go())


class _R:
    def __init__(self, status, data=None, headers=None):
        self.status, self.data, self.headers, self.cookies = status, data or {}, headers or {}, {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        return self.data


class _X:
    """A pretend x.com: answers each request with the next of `answers`."""
    def __init__(self, answers):
        self.answers, self.calls = list(answers), []

    def get(self, url, **kw):
        self.calls.append(url)
        return self.answers.pop(0)

    post = get


def xlist(answers, tmp="/tmp/claude-x-list-test.json"):
    x = XList("tok", "csrf", session=_X(answers), path=__import__("pathlib").Path(tmp))
    x.list_id = "1"
    x.ops = {op: {"id": "q" + op, "features": ["f"]} for op in ("ListLatestTweetsTimeline", "ListAddMember", "CreateList", "Viewer")}
    return x


def test_a_refused_login_stops_a_429_pauses_and_the_reads_are_paced_to_xs_allowance():
    async def go():
        ok = {"data": {"list": {}}}
        reset = time.time() + 600
        x = xlist([_R(200, ok, {"x-rate-limit-remaining": "300", "x-rate-limit-reset": str(reset)})])
        assert await x.timeline() == []
        assert 1.9 < x.wait(2.0) <= 2.1                              # 300 left for 600 s: every 2 s is fine
        x.left = 52
        assert 11.5 < x.wait(2.0) < 12.5                             # only 52 left: spread over what remains
        locked = xlist([_R(403)])
        try:
            await locked.timeline()
            assert False, "a refused login must stop the list"
        except XLoginError:
            pass
        busy = xlist([_R(429, {}, {"x-rate-limit-remaining": "0", "x-rate-limit-reset": str(reset)})])
        try:
            await busy.timeline()
        except RuntimeError:
            pass
        assert busy.wait(2.0) > 590                                  # no read before X's window resets
        stale = xlist([_R(404, {"errors": [{"message": "not found"}]}), _R(200, ok)])
        found = []

        async def discover():
            found.append(1)
            stale.ops["ListLatestTweetsTimeline"] = {"id": "new", "features": []}
        stale.discover = discover
        assert await stale.timeline() == [] and found == [1]         # X shipped a new app: the ids are read again
        assert "/new/" in stale.session.calls[-1]
    asyncio.run(go())


def test_the_list_loop_sends_new_posts_and_stops_when_the_login_is_refused():
    async def go():
        m = nm()
        sent = []

        async def post_x(t, h, parent=None, feed_kind="x_account", netuid=None):
            sent.append(t["id"])
        m._post_x = post_x
        m._x_ids["yanez__ai"] = ["1"]

        class Accounts:
            def accounts(self):
                return {"yanez__ai": ("yanez__ai", {54})}

        class FakeList:
            who, members, reads = "me", {"yanez__ai": "9"}, 0

            async def start(self):
                pass

            async def sync(self, handles, user_id):
                return 0

            async def timeline(self):
                self.reads += 1
                if self.reads == 1:
                    return [post(7), post(8, author="not_watched")]
                raise XLoginError("X refused the login (HTTP 403)")

            def wait(self, every):
                return 0
        await asyncio.wait_for(m.subnet_x_list_loop(FakeList(), Accounts()), 2)
        assert sent == ["7"] and m.stats["xl_polls"] == 1 and m.stats["xl_off"] == 1
    asyncio.run(go())


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} X list tests passed")
