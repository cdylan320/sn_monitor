"""Run: .venv/bin/python tests/test_dmlog.py"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from snmon.dmlog import DmLog, line  # noqa: E402
from snmon.news import NewsMonitor  # noqa: E402

ME, T = "1562473619932586110", "777"
CHAT = "D1"
MID = str(1438000000000000000)   # a DM message id (its time is in the snowflake)


def rows(p: Path):
    return [json.loads(x) for x in p.read_text().splitlines()]


def fresh():
    p = Path(tempfile.mkdtemp()) / "dms.jsonl"
    d = DmLog(p)
    d.on_event("READY", {"user": {"id": ME, "username": "vic_x."},
                         "users": [{"id": T, "username": "1_t_t_t_1", "global_name": "T"}],
                         "private_channels": [{"id": CHAT, "type": 1, "recipient_ids": [T]}]})
    return d, p


def test_new_edit_delete_kept():
    d, p = fresh()
    d.on_event("MESSAGE_CREATE", {"id": MID, "channel_id": CHAT, "content": "sorry, I blocked you because",
                                  "author": {"id": T, "username": "1_t_t_t_1", "global_name": "T"}})
    d.on_event("MESSAGE_UPDATE", {"id": MID, "channel_id": CHAT, "embeds": []})          # a preview only: not an edit
    d.on_event("MESSAGE_UPDATE", {"id": MID, "channel_id": CHAT, "content": "never mind"})
    d.on_event("MESSAGE_DELETE", {"id": MID, "channel_id": CHAT})
    r = rows(p)
    assert [e["event"] for e in r] == ["new", "edited", "deleted"]
    assert r[0]["chat"] == "1_t_t_t_1 (T)" and r[0]["by"] == "1_t_t_t_1 (T)"
    assert r[1]["before"] == "sorry, I blocked you because" and r[1]["text"] == "never mind"
    assert r[2]["text"] == "never mind" and r[2]["by"] == "1_t_t_t_1 (T)"           # the deleted text is kept
    assert "DELETED by 1_t_t_t_1 (T)" in line(r[2]) and "never mind" in line(r[2])


def test_delete_after_restart_and_unknown():
    d, p = fresh()
    d.on_event("MESSAGE_CREATE", {"id": MID, "channel_id": CHAT, "content": "hi", "author": {"id": T, "username": "t"}})
    d2 = DmLog(p)                                      # the monitor restarted: the saved messages are read back
    d2.on_event("MESSAGE_DELETE", {"id": MID, "channel_id": CHAT})
    assert rows(p)[-1]["text"] == "hi" and rows(p)[-1]["chat"] == "1_t_t_t_1 (T)"
    d2.on_event("MESSAGE_DELETE", {"id": "1438000000000000999", "channel_id": CHAT})   # sent before the log
    assert rows(p)[-1]["text"] is None and "text unknown" in line(rows(p)[-1])


def test_own_message_reaction_and_file():
    d, p = fresh()
    d.on_event("MESSAGE_CREATE", {"id": MID, "channel_id": CHAT, "content": "react only!",
                                  "author": {"id": ME, "username": "vic_x."}})
    d.on_event("MESSAGE_REACTION_ADD", {"user_id": T, "channel_id": CHAT, "message_id": MID, "emoji": {"name": "👍"}})
    r = rows(p)
    assert r[0]["by"] == "YOU"
    assert r[1]["event"] == "reaction" and r[1]["by"] == "1_t_t_t_1 (T)" and r[1]["on_by"] == "YOU"
    assert "reacted 👍 on YOU's message 'react only!'" in line(r[1])
    job = d.on_event("MESSAGE_CREATE", {"id": "1438000000000000002", "channel_id": CHAT, "content": "",
                                        "author": {"id": T, "username": "1_t_t_t_1"},
                                        "attachments": [{"filename": "proof 1.png", "url": "", "size": 10}]})
    assert rows(p)[-1]["files"] == ["1438000000000000002_proof_1.png"]
    job.close()                                        # (the download itself is not run here)


def test_news_routes_only_dms():
    m = NewsMonitor("x", None, None, price_now=lambda n: None, change=lambda n, b: None)
    m.guild_id = "G"
    seen = []

    class Log:
        def on_event(self, t, d):
            seen.append(t)
    m.dm_log = Log()
    m._on_event("MESSAGE_CREATE", {"id": "1", "channel_id": CHAT, "content": "x", "author": {"id": T}})
    m._on_event("MESSAGE_DELETE", {"id": "1", "channel_id": "C78", "guild_id": "G"})       # the server: not a DM
    assert seen == ["MESSAGE_CREATE"]


if __name__ == "__main__":
    n = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            n += 1
    print(f"{n} DM log tests passed")
