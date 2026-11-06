"""Keeps every DM of the news account: new, edited and deleted messages, and reactions, in data/dms.jsonl.

A message the sender deletes is still here (Discord keeps no copy we can read; 6 Nov, 1_t_t_t_1 sent 2 and deleted
them). Files / images are saved to data/dm_files/ at once: Discord removes them with the message. data/ is never pushed.

Read it:  ./dms.sh            the last 50 events
          ./dms.sh deleted    only deleted and edited messages
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from .config import ROOT

log = logging.getLogger("snmon.dmlog")

DM_FILE = ROOT / "data" / "dms.jsonl"
MAX_FILE_BYTES = 25 * 1024 * 1024        # a bigger attachment: its name and link only
EPOCH_MS = 1420070400000


def user_name(u: dict) -> str:
    n = u.get("username") or u.get("id") or "?"
    g = u.get("global_name")
    return f"{n} ({g})" if g and g != n else n


def sent_at(snowflake: str) -> float:
    return ((int(snowflake) >> 22) + EPOCH_MS) / 1000


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds")


class DmLog:
    def __init__(self, path: Path = DM_FILE) -> None:
        self.path = path
        self.files_dir = path.parent / "dm_files"
        self.me_id: str | None = None
        self.msgs: dict[str, dict] = {}     # message id → {"by", "text", "files", "sent"} (for edits / deletes / reactions)
        self.users: dict[str, str] = {}     # user id → name
        self.chats: dict[str, str] = {}     # DM channel id → who the chat is with
        self._load()

    def _load(self) -> None:
        """The messages saved before (a restart): a delete after it still shows what was written."""
        try:
            f = open(self.path, encoding="utf-8")
        except FileNotFoundError:
            return
        with f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("chat") and e.get("channel_id"):
                    self.chats.setdefault(e["channel_id"], e["chat"])
                if e.get("event") == "new":
                    self.msgs[e["id"]] = {k: e.get(k) for k in ("by", "text", "files", "sent")}
                elif e.get("event") == "edited" and e.get("id") in self.msgs:
                    self.msgs[e["id"]]["text"] = e.get("text")

    async def start(self, client) -> None:
        """Who this account is and who each DM chat is with (one request, as the Discord app does at start)."""
        if client.user:
            self.me_id = client.user["id"]
        for c in await client.get("/users/@me/channels"):
            self._chat(c)

    def _chat(self, c: dict) -> None:
        people = c.get("recipients") or []
        for u in people:
            self.users[u["id"]] = user_name(u)
        if people:
            self.chats[c["id"]] = c.get("name") or ", ".join(user_name(u) for u in people)

    # ── gateway events (only those without a guild_id: DMs) ──────────────

    def on_event(self, t: str, d: dict):
        if t == "READY":
            self.me_id = (d.get("user") or {}).get("id") or self.me_id
            for u in d.get("users") or []:
                self.users[u["id"]] = user_name(u)
            for c in d.get("private_channels") or []:
                if not c.get("recipients") and c.get("recipient_ids"):
                    c = dict(c, recipients=[{"id": i, "username": self.users.get(i, i)} for i in c["recipient_ids"]])
                self._chat(c)
        elif t == "CHANNEL_CREATE":
            self._chat(d)
        elif t == "MESSAGE_CREATE":
            return self._new(d)
        elif t == "MESSAGE_UPDATE":
            self._edited(d)
        elif t == "MESSAGE_DELETE":
            self._deleted(d)
        elif t in ("MESSAGE_REACTION_ADD", "MESSAGE_REACTION_REMOVE"):
            self._reaction(d, t == "MESSAGE_REACTION_ADD")
        return None

    def _by(self, user_id: str | None) -> str:
        if user_id and user_id == self.me_id:
            return "YOU"
        return self.users.get(user_id or "", user_id or "?")

    def _new(self, d: dict):
        a = d.get("author") or {}
        if a.get("id"):
            self.users[a["id"]] = user_name(a)
        cid = d.get("channel_id")
        if cid not in self.chats and a.get("id") and a["id"] != self.me_id:
            self.chats[cid] = user_name(a)
        text = d.get("content") or ""
        stickers = [s.get("name", "?") for s in d.get("sticker_items") or d.get("stickers") or []]
        if stickers:
            text = (text + " " if text else "") + " ".join(f"[sticker {s}]" for s in stickers)
        if not text and d.get("embeds"):
            text = "[embed]"
        atts = d.get("attachments") or []
        files = [f"{d['id']}_{_safe(x.get('filename') or 'file')}" for x in atts]
        sent = iso(sent_at(d["id"]))
        self.msgs[d["id"]] = {"by": self._by(a.get("id")), "text": text, "files": files, "sent": sent}
        ref = (d.get("message_reference") or {}).get("message_id")
        self._write({"t": sent, "event": "new", "chat": self.chats.get(cid, "?"), "channel_id": cid, "id": d["id"],
                     "by": self._by(a.get("id")), "text": text, "files": files,
                     **({"reply_to": ref} if ref else {})})
        if atts:
            return self._save_files(atts, files)
        return None

    def _edited(self, d: dict) -> None:
        if "content" not in d:                  # a link preview added: not an edit
            return
        old = self.msgs.get(d.get("id"))
        if old is not None and old.get("text") == d["content"]:
            return
        by = self._by((d.get("author") or {}).get("id")) if d.get("author") else (old or {}).get("by", "?")
        self._write({"t": iso(time.time()), "event": "edited", "chat": self.chats.get(d.get("channel_id"), "?"),
                     "channel_id": d.get("channel_id"), "id": d.get("id"), "by": by,
                     "before": (old or {}).get("text"), "text": d["content"]})
        if old is not None:
            old["text"] = d["content"]

    def _deleted(self, d: dict) -> None:
        old = self.msgs.get(d.get("id")) or {}
        self._write({"t": iso(time.time()), "event": "deleted", "chat": self.chats.get(d.get("channel_id"), "?"),
                     "channel_id": d.get("channel_id"), "id": d.get("id"), "by": old.get("by", "?"),
                     "sent": old.get("sent") or (iso(sent_at(d["id"])) if d.get("id") else None),
                     "text": old.get("text"), "files": old.get("files") or []})
        log.info("DM deleted in the chat with %s (sent by %s) — kept in %s",
                 self.chats.get(d.get("channel_id"), "?"), old.get("by", "?"), self.path.name)

    def _reaction(self, d: dict, added: bool) -> None:
        e = d.get("emoji") or {}
        on = self.msgs.get(d.get("message_id")) or {}
        self._write({"t": iso(time.time()), "event": "reaction" if added else "reaction removed",
                     "chat": self.chats.get(d.get("channel_id"), "?"), "channel_id": d.get("channel_id"),
                     "id": d.get("message_id"), "by": self._by(d.get("user_id")), "emoji": e.get("name") or "?",
                     "on_by": on.get("by"), "on_text": on.get("text")})

    def _write(self, rec: dict) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except OSError:
            log.exception("DM log: could not write %s", self.path)

    async def _save_files(self, atts: list[dict], names: list[str]) -> None:
        self.files_dir.mkdir(parents=True, exist_ok=True)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as s:   # no token: Discord's CDN
            for a, name in zip(atts, names):
                if not a.get("url") or (a.get("size") or 0) > MAX_FILE_BYTES:
                    continue
                try:
                    async with s.get(a["url"]) as r:
                        r.raise_for_status()
                        (self.files_dir / name).write_bytes(await r.content.read(MAX_FILE_BYTES))
                except Exception as ex:   # noqa: BLE001 — a lost file must not stop the log
                    log.warning("DM log: could not save the file %s (%s)", name, ex)


def _safe(name: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", name)[:80] or "file"


# ── reading it: ./dms.sh ─────────────────────────────────────────────────

def _short(t: str | None) -> str:
    return (t or "")[5:16].replace("T", " ")


def line(e: dict) -> str:
    when, chat, by = _short(e.get("t")), e.get("chat", "?"), e.get("by", "?")
    text = e.get("text")
    files = "".join(f"  [file: data/dm_files/{f}]" for f in e.get("files") or [])
    ev = e.get("event")
    if ev == "new":
        return f"{when}  [{chat}]  {by}: {text}{files}"
    if ev == "edited":
        return f"{when}  [{chat}]  EDITED by {by}: {e.get('before')!r} -> {text!r}"
    if ev == "deleted":
        what = repr(text) if text is not None else "(sent before the log started: text unknown)"
        return f"{when}  [{chat}]  DELETED by {by} (sent {_short(e.get('sent'))}): {what}{files}"
    if ev in ("reaction", "reaction removed"):
        on = f" on {e['on_by']}'s message {e.get('on_text')!r}" if e.get("on_by") else ""
        verb = "reacted" if ev == "reaction" else "removed reaction"
        return f"{when}  [{chat}]  {by} {verb} {e.get('emoji')}{on}"
    return f"{when}  [{chat}]  {ev}: {e}"


def main(argv: list[str]) -> None:
    only_changed = "deleted" in argv
    n = next((int(a) for a in argv if a.isdigit()), 50)
    try:
        rows = [json.loads(x) for x in DM_FILE.read_text(encoding="utf-8").splitlines() if x.strip()]
    except FileNotFoundError:
        print("No DMs saved yet (data/dms.jsonl). They are saved from when the monitor runs with this feature.")
        return
    if only_changed:
        rows = [e for e in rows if e.get("event") in ("deleted", "edited")]
    print("Times are UTC.")
    for e in rows[-n:]:
        print(line(e))
    if not rows:
        print("Nothing yet.")


if __name__ == "__main__":
    main(sys.argv[1:])
