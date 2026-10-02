"""Run: .venv/bin/python tests/test_news.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.news import ANNOUNCE, TEAM, XPOST, Channel, NewsMonitor  # noqa: E402


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


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} news tests passed")
