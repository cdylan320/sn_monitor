"""Run: .venv/bin/python tests/test_judge.py — is a news post important? Claude decides, the keyword rules back it up."""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.judge import OPENROUTER_URL, NewsJudge, Verdict, rules  # noqa: E402
from snmon.news import XPOST, Channel, NewsMonitor, x_posted  # noqa: E402


class _Block:
    def __init__(self, text):
        self.type, self.text = "text", text


class _Reply:
    def __init__(self, data=None, stop="end_turn"):
        self.stop_reason = stop
        self.content = [_Block(json.dumps(data))] if data is not None else []


class _Client:
    """A pretend AsyncAnthropic: records each request, answers with `reply` after `delay` seconds."""
    def __init__(self, reply, delay=0.0):
        self.calls, self.reply, self.delay = [], reply, delay
        client = self

        class Messages:
            async def create(self, **kw):
                client.calls.append(kw)
                await asyncio.sleep(client.delay)
                return client.reply

        class Beta:
            messages = Messages()
        self.beta = Beta()


FUNDING = {"important": True, "category": "funding", "reason": "raised $5M led by a known fund"}


def test_claude_judges_with_a_json_answer_low_effort_and_fallbacks_and_the_verdict_is_kept():
    async def go():
        c = _Client(_Reply(FUNDING))
        j = NewsJudge("claude-opus-5-5", client=c)
        v = await j.judge("SN85 Vidaio", "We raised $5M", "", None)
        assert v == Verdict(True, "funding", "raised $5M led by a known fund", "ai")
        kw = c.calls[0]
        assert kw["model"] == "claude-opus-5-5" and kw["output_config"]["effort"] == "low"
        assert kw["output_config"]["format"]["type"] == "json_schema"
        assert kw["fallbacks"] == "default" and kw["betas"] == ["server-side-fallback-2026-07-01"]
        assert await j.judge("SN85 Vidaio", "We raised $5M", "", None) == v and len(c.calls) == 1   # kept: asked once
        h = NewsJudge("claude-haiku-5-5", client=_Client(_Reply(FUNDING)))
        await h.judge("SN85", "x")
        assert "fallbacks" not in h.client.calls[0]                                              # Haiku has none
    asyncio.run(go())


def test_a_refusal_a_slow_answer_or_no_key_falls_back_to_the_keyword_rules():
    async def go():
        refused = NewsJudge(client=_Client(_Reply(stop="refusal")))
        assert (await refused.judge("SN1", "Introducing our new app, live now")).by == "rules"
        slow = NewsJudge(timeout=0.05, client=_Client(_Reply(FUNDING), delay=1.0))
        v = await slow.judge("SN1", "gm")
        assert v.by == "rules" and not v.important and slow.stats["ai_fail"] == 1
        os.environ.pop("ANTHROPIC_API_KEY", None)
        none = NewsJudge()
        assert none.client is None and "keyword rules" in none.mode
    asyncio.run(go())


def test_the_keyword_rules_on_real_posts():
    # 21 Oct, the bots' news trades: only real news counts
    assert rules("SWARM quarterly update. The thesis now has customers. Two contracts signed.", 2 * 86400).important is False
    assert rules("@lemcrarz you read it right, thanks. The validator's clock for each miner starts before it").important is False
    assert rules("Yeah your latest review was recorded as nonresponse.").important is False
    assert rules("**Ditto Ecosystem Update | October 16** This week's merged work gives miners clearer submissions").important is False
    assert rules("Reminder: AMA on X spaces tomorrow").important is False
    assert rules("We just killed cold outreach. Introducing Leadpoet: the AI sales agent").category == "launch"
    assert rules("Vidaio raised $5M from Polychain").category == "funding"
    assert rules("Today we welcome our newest sales partner: Tobruk Protective Security Solutions.").category == "partnership"


def test_an_x_post_id_tells_when_it_was_posted():
    assert time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(x_posted("2112159070706667726"))) == "2026-10-19 12:28:59"


class _Meta:
    def info(self, n):
        from snmon.meta import SubnetInfo
        return SubnetInfo(n, "Swarm", "", 5000.0, 1, None, None)


def test_a_discord_share_is_judged_with_the_shared_x_post_and_the_verdict_goes_to_the_bots_and_the_card():
    async def go():
        m = NewsMonitor("x", None, _Meta(), price_now=lambda n: 9_555_000, change=lambda n, b: 0.0, dry_run=True)
        m.guild_id = "G"
        sent, cards = [], []

        class Feed:
            def publish(self, ev):
                sent.append(ev)
                return 1
        m.news_feed = Feed()
        c = _Client(_Reply({"important": False, "category": "none", "reason": "a 2-day-old quarterly update"}))
        m.judge = NewsJudge(client=c)

        async def tweet(url):
            return {"author": {"name": "SWARM (@SwarmSubnet)"}, "description": "SWARM quarterly update to stakeholders.",
                    "url": url, "fields": []}
        m.tweet = tweet

        async def send(payload, netuid, p0):
            cards.append(payload)
        m._send = send
        ch = Channel("C124", "124・swarm", 124, team_users={"TEAM"})
        msg = {"id": "101", "channel_id": "C124", "type": 0, "author": {"id": "TEAM", "username": "team"},
               "content": "https://x.com/SwarmSubnet/status/2112159070706667726 Hi, You can find here where Swarm stands now",
               "mentions": [], "attachments": [], "timestamp": "2026-10-21T12:49:12+00:00"}
        await m._post(msg, ch, XPOST, "team")
        ask = c.calls[0]["messages"][0]["content"]
        assert "SWARM quarterly update" in ask and "minutes ago" in ask        # the shared post and its age are judged
        ev = sent[0]
        assert ev["important"] is False and ev["judge"] == "ai" and ev["why"] == "a 2-day-old quarterly update"
        assert any(f["name"] == "News check (AI)" and "Not important" in f["value"] for f in cards[0]["embeds"][0]["fields"])
    asyncio.run(go())


class _Session:
    """A pretend aiohttp session for OpenRouter: records each POST, answers `status` / `data`."""
    def __init__(self, status, data):
        self.calls, self.status, self.data = [], status, data

    def post(self, url, json=None, headers=None):
        sess = self

        class R:
            status = sess.status

            async def __aenter__(self):
                sess.calls.append((url, json, headers))
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self, content_type=None):
                return sess.data
        return R()


def test_with_an_openrouter_key_the_judge_asks_openrouter_for_a_json_answer_and_counts_the_cost():
    async def go():
        os.environ["OPENROUTER_API_KEY"] = "sk-or-test"
        try:
            sess = _Session(200, {"choices": [{"message": {"content": json.dumps(FUNDING)}}],
                                  "usage": {"prompt_tokens": 950, "completion_tokens": 40, "cost": 0.000115}})
            j = NewsJudge(session=sess)
            assert j.provider == "openrouter" and j.model == "openai/gpt-6.1-sol"
            assert j.mode == "openai/gpt-6.1-sol via OpenRouter"
            v = await j.judge("SN85 Vidaio", "We raised $5M")
            assert v == Verdict(True, "funding", "raised $5M led by a known fund", "ai")
            url, body, headers = sess.calls[0]
            assert url == OPENROUTER_URL and headers["Authorization"] == "Bearer sk-or-test"
            assert body["model"] == "openai/gpt-6.1-sol" and body["reasoning"] == {"effort": "low"} and body["messages"][0]["role"] == "system"
            fmt = body["response_format"]
            assert fmt["type"] == "json_schema" and fmt["json_schema"]["strict"] is True
            assert abs(j.stats["cost"] - 0.000115) < 1e-12
            broke = NewsJudge(session=_Session(402, {"error": {"message": "Insufficient credits", "code": 402}}))
            w = await broke.judge("SN85 Vidaio", "Introducing our app, live now")
            assert w.by == "rules" and w.important and broke.stats["ai_fail"] == 1   # out of credits: the rules decide
        finally:
            os.environ.pop("OPENROUTER_API_KEY", None)
    asyncio.run(go())



def test_the_typical_news_always_counts_and_the_model_judges_anything_else_too():
    from snmon.judge import CATEGORIES, SYSTEM
    assert "ALWAYS count as important" in SYSTEM and "use your own judgment" in SYSTEM and '"other"' in SYSTEM
    assert "Do not miss important news just because it is not in the list" in SYSTEM
    assert "other" in CATEGORIES
    v = NewsJudge._verdict(json.dumps({"important": True, "category": "other", "reason": "the founder joins OpenAI"}))
    assert v.important and v.category == "other"                  # important news outside the examples is traded


def test_raced_requests_the_first_good_answer_is_used_and_the_slow_one_is_dropped():
    # 21 Oct: SN79's check took 5.1 s and its stake missed a block; 2 requests at once cut the slow answers
    async def go():
        os.environ["OPENROUTER_API_KEY"] = "sk-or-test"
        try:
            calls = []

            class Slow(_Session):
                def post(self, url, json=None, headers=None):
                    inner, n = super().post(url, json, headers), len(calls)
                    calls.append(n)

                    class Late:
                        async def __aenter__(self):
                            if n == 0:
                                await asyncio.sleep(2)          # the first request is the slow one
                            return await inner.__aenter__()

                        async def __aexit__(self, *a):
                            return False
                    return Late()
            j = NewsJudge(session=Slow(200, {"choices": [{"message": {"content": json.dumps(FUNDING)}}], "usage": {}}),
                          race=2)
            assert "2 requests raced" in j.mode
            t = time.perf_counter()
            v = await j.judge("SN85 Vidaio", "We raised $5M")
            assert v.by == "ai" and v.category == "funding" and time.perf_counter() - t < 1.0 and len(calls) == 2
            bad = NewsJudge(session=_Session(402, {"error": {"message": "no credits"}}), race=2)
            assert (await bad.judge("SN85", "Introducing our app, live now")).by == "rules"   # both fail: the rules
        finally:
            os.environ.pop("OPENROUTER_API_KEY", None)
    asyncio.run(go())

if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} news judge tests passed")
