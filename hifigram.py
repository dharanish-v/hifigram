"""hifigram: find the best-quality copy of any song across your Telegram channels.

Usage:
  hifigram.py                                   interactive search + pick
  hifigram.py login                             one-time Telegram login
  hifigram.py sync                              index audio from all chats (resumable; later runs fetch only new posts)
  hifigram.py stats                             what's in the index
  hifigram.py search "q1" "q2" [--json]         fuzzy search the index, grouped by song, best copy first
  hifigram.py top 5 ["query"] [--chat NAME]     highest-quality unique songs
  hifigram.py verify CHAT:MSG ...               spectrum check for fake lossless (~4 MB each); also --verify on search/top/artist
  hifigram.py channels "tamil flac"             find public channels by name (nothing joined)
  hifigram.py scan @channel                     index a public channel without joining
  hifigram.py get CHAT:MSG [CHAT:MSG ...]       download specific files
  hifigram.py artist "name" "variant" --plan    best copy of every unique song -> plans/<artist>.json
  hifigram.py artist "name" --download          download that plan (resumable, skips what you have)
"""
import argparse
import asyncio
import concurrent.futures
import contextlib
import fcntl
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import unicodedata
from pathlib import Path

import mutagen
from mutagen.mp3 import BitrateMode, MPEGInfo
from rapidfuzz import fuzz, process
from rich.console import Console
from rich.table import Table
from telethon import TelegramClient
from telethon.errors import PhoneNumberInvalidError, RPCError
from telethon.tl.types import (
    DocumentAttributeAudio,
    InputMessagesFilterDocument,
    InputMessagesFilterMusic,
)

HERE = Path(__file__).parent
CONFIG = HERE / "config.json"
DOWNLOADS = HERE / "downloads"
PLANS = HERE / "plans"
DB = HERE / "index.db"

LOSSLESS = {"flac", "alac", "wav", "aiff", "aif", "ape", "wv"}
AUDIO_EXTS = LOSSLESS | {"mp3", "m4a", "aac", "ogg", "opus", "oga", "wma"}
# rough perceptual weight vs MP3 at the same bitrate
CODEC_WEIGHT = {"mp3": 1.0, "aac": 1.25, "m4a": 1.25, "opus": 1.6, "ogg": 1.2, "oga": 1.2, "wma": 0.9}
PUNCT = r"[_\-–—.,!?'\"’:;|/\\&+*#~]+"
VERSION_WORDS = ("remix", "reprise", "unplugged", "live", "instrumental", "karaoke", "acoustic", "cover", "slowed", "lofi")

BLOCK = 128 * 1024  # partial-download block for header probes
PROBE_CAP = 4 * 1024 * 1024  # give up probing a file after this many bytes
PROBE_CONCURRENCY = 8
DOWNLOAD_CONCURRENCY = 3

out = Console()
err = Console(stderr=True)


# ---------- config / client ----------

def load_config(interactive):
    if CONFIG.exists():
        return json.loads(CONFIG.read_text())
    if not interactive:
        sys.exit("No config.json. Run: hifigram.py login")
    err.print("Get api_id / api_hash at https://my.telegram.org → API development tools")
    cfg = {"api_id": int(input("api_id: ")), "api_hash": input("api_hash: ").strip()}
    CONFIG.write_text(json.dumps(cfg))
    return cfg


async def connect(interactive):
    cfg = load_config(interactive)
    client = TelegramClient(str(HERE / "session"), cfg["api_id"], cfg["api_hash"], flood_sleep_threshold=300)
    if interactive:
        while True:
            try:
                await client.start(phone=lambda: input("Phone, international format (e.g. +919876543210): ").replace(" ", ""))
                break
            except PhoneNumberInvalidError:
                err.print("[red]Invalid number.[/] Use + and country code, no leading 0 or spaces.")
    else:
        await client.connect()
        if not await client.is_user_authorized():
            sys.exit("Not logged in. Run: hifigram.py login")
    return client


# ---------- partial-download file for header probes ----------

class ProbeCapReached(Exception):
    pass


# network trouble: never cache as a result, retry on a later run
TRANSIENT = (TimeoutError, ConnectionError, concurrent.futures.TimeoutError, RPCError)


class RemoteFile:
    """Seekable read-only file over a Telegram document; fetches only the blocks that get read."""

    def __init__(self, client, loop, doc, name):
        self.client, self.loop, self.doc, self.name = client, loop, doc, name
        self.size, self.pos, self.blocks, self.fetched = doc.size, 0, {}, 0

    async def _fetch(self, i):
        async for chunk in self.client.iter_download(
            self.doc, offset=i * BLOCK, request_size=BLOCK, limit=1, file_size=self.size
        ):
            return bytes(chunk)
        return b""

    def _block(self, i):
        if i not in self.blocks:
            if self.fetched >= PROBE_CAP:
                raise ProbeCapReached()
            data = asyncio.run_coroutine_threadsafe(self._fetch(i), self.loop).result(timeout=60)
            self.blocks[i] = data
            self.fetched += len(data)
        return self.blocks[i]

    def read(self, n=-1):
        end = self.size if n is None or n < 0 else min(self.size, self.pos + n)
        parts = []
        while self.pos < end:
            i, off = divmod(self.pos, BLOCK)
            data = self._block(i)[off:off + end - self.pos]
            if not data:
                break
            parts.append(data)
            self.pos += len(data)
        return b"".join(parts)

    def seek(self, offset, whence=0):
        self.pos = max(0, [offset, self.pos + offset, self.size + offset][whence])
        return self.pos

    def tell(self):
        return self.pos

    def seekable(self):
        return True

    def readable(self):
        return True


def probe_flac(f):
    if f.read(4) != b"fLaC":
        return None
    info, pos = None, 4
    for _ in range(128):
        hdr = f.read(4)
        if len(hdr) < 4:
            return None
        last, btype, length = hdr[0] & 0x80, hdr[0] & 0x7F, int.from_bytes(hdr[1:4], "big")
        if btype == 0:
            x = int.from_bytes(f.read(length)[10:18], "big")
            sr, bits, total = x >> 44, ((x >> 36) & 0x1F) + 1, x & 0xFFFFFFFFF
            info = {"sample_rate": sr, "bits": bits, "length": total / sr if sr else 0, "channels": ((x >> 41) & 7) + 1}
        else:
            f.seek(length, 1)
        pos += 4 + length
        if last:
            break
    if info and info["length"]:
        info["kbps"] = round((f.size - pos) * 8 / info["length"] / 1000)  # audio only, no cover art
    return info


def probe_mp3(f):
    h = f.read(10)
    offset = 0
    if h[:3] == b"ID3":
        size = (h[6] << 21) | (h[7] << 14) | (h[8] << 7) | h[9]
        offset = 10 + size + (10 if h[5] & 0x10 else 0)  # skip tag (cover art) without reading it
    info = MPEGInfo(f, offset)
    return {"kbps": round(info.bitrate / 1000), "sample_rate": info.sample_rate, "length": info.length,
            "vbr": info.bitrate_mode in (BitrateMode.VBR, BitrateMode.ABR), "channels": info.channels}


def mp4_atoms(f, start, end):
    pos = start
    while pos + 8 <= end:
        f.seek(pos)
        h = f.read(8)
        if len(h) < 8:
            return
        size, typ, hdr = int.from_bytes(h[:4], "big"), h[4:8], 8
        if size == 1:
            size, hdr = int.from_bytes(f.read(8), "big"), 16
        elif size == 0:
            size = end - pos
        if size < hdr:
            return
        yield typ, pos + hdr, pos + size
        pos += size


def mp4_child(f, start, end, *path):
    for name in path:
        found = next(((s, e) for t, s, e in mp4_atoms(f, start, end) if t == name), None)
        if not found:
            return None
        start, end = found
    return start, end


def probe_mp4(f):
    """Walk atoms by header only, so cover art in moov/udta is never downloaded."""
    top = {t: (s, e) for t, s, e in mp4_atoms(f, 0, f.size)}
    if b"moov" not in top:
        return None
    for t, s, e in mp4_atoms(f, *top[b"moov"]):
        if t != b"trak":
            continue
        mdhd = mp4_child(f, s, e, b"mdia", b"mdhd")
        stsd = mp4_child(f, s, e, b"mdia", b"minf", b"stbl", b"stsd")
        if not mdhd or not stsd:
            continue
        f.seek(mdhd[0])
        d = f.read(32)
        if d[0] == 1:
            scale, dur = int.from_bytes(d[20:24], "big"), int.from_bytes(d[24:32], "big")
        else:
            scale, dur = int.from_bytes(d[12:16], "big"), int.from_bytes(d[16:20], "big")
        f.seek(stsd[0] + 8)  # version/flags + entry count
        entry = f.read(36 + 36)
        fmt = entry[4:8]
        if fmt not in (b"alac", b"mp4a"):
            continue
        bits, sr = int.from_bytes(entry[26:28], "big"), int.from_bytes(entry[32:34], "big")
        channels = int.from_bytes(entry[24:26], "big")
        if fmt == b"alac" and entry[40:44] == b"alac":  # ALACSpecificConfig: real depth + rate (>65535 Hz)
            cfg = entry[48:]
            bits, sr, channels = cfg[5], int.from_bytes(cfg[20:24], "big"), cfg[9]
        length = dur / scale if scale else 0
        mdat = top.get(b"mdat")
        kbps = round((mdat[1] - mdat[0]) * 8 / length / 1000) if mdat and length else 0
        return {"kbps": kbps, "sample_rate": sr, "bits": bits, "length": length, "alac": fmt == b"alac",
                "channels": channels}
    return None


def probe_wav(f):
    if f.read(4) != b"RIFF" or (f.read(4), f.read(4))[1] != b"WAVE":
        return None
    pos, fmt = 12, None
    for _ in range(64):
        f.seek(pos)
        h = f.read(8)
        if len(h) < 8:
            return None
        cid, size = h[:4], int.from_bytes(h[4:8], "little")
        if cid == b"fmt ":
            d = f.read(16)
            fmt = {"sample_rate": int.from_bytes(d[4:8], "little"), "byte_rate": int.from_bytes(d[8:12], "little"),
                   "bits": int.from_bytes(d[14:16], "little"), "channels": int.from_bytes(d[2:4], "little")}
        elif cid == b"data" and fmt and fmt["byte_rate"]:
            length = size / fmt["byte_rate"]
            return {"kbps": round(fmt["byte_rate"] * 8 / 1000), "sample_rate": fmt["sample_rate"],
                    "bits": fmt["bits"], "length": length, "channels": fmt["channels"]}
        pos += 8 + size + (size & 1)
    return None


ADTS_RATES = [96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350]


def probe_adts(f):
    """Raw .aac has no duration: average the frame sizes in the first block."""
    data = f.read(BLOCK)
    pos, frames, total, sr = 0, 0, 0, 0
    while pos + 7 <= len(data) and data[pos] == 0xFF and data[pos + 1] & 0xF0 == 0xF0:
        idx = (data[pos + 2] >> 2) & 0xF
        if idx >= len(ADTS_RATES):
            return None
        sr = ADTS_RATES[idx]
        ch = ((data[pos + 2] & 1) << 2) | (data[pos + 3] >> 6)
        n = ((data[pos + 3] & 3) << 11) | (data[pos + 4] << 3) | (data[pos + 5] >> 5)
        if n < 7:
            return None
        frames, total, pos = frames + 1, total + n, pos + n
    if frames < 10:
        return None
    kbps = total / frames * 8 * sr / 1024 / 1000
    return {"kbps": round(kbps), "sample_rate": sr, "length": f.size * 8 / (kbps * 1000), "estimated": True,
            "channels": ch}


def probe_other(f):
    m = mutagen.File(f)
    if not m:
        return None
    i = m.info
    codec = getattr(i, "codec", "") or ""
    return {"kbps": round((getattr(i, "bitrate", 0) or 0) / 1000), "sample_rate": getattr(i, "sample_rate", 0),
            "bits": getattr(i, "bits_per_sample", 0), "length": getattr(i, "length", 0),
            "alac": codec.startswith("alac"), "channels": getattr(i, "channels", 0)}


def probe(f, ext):
    f.seek(0)
    if ext == "flac":
        return probe_flac(f)
    if ext == "mp3":
        return probe_mp3(f)
    head = f.read(12)
    f.seek(0)
    if head[4:8] == b"ftyp":
        return probe_mp4(f)
    if head[:4] == b"RIFF":
        return probe_wav(f)
    if head[0] == 0xFF and head[1] & 0xF0 == 0xF0:
        return probe_adts(f)
    return probe_other(f)



# ---------- local index ----------
# Everything Telegram returns is kept here, so nothing is ever fetched twice:
#   files     one row per audio post (any chat), refreshed whenever a message is seen
#   chats     chat titles + the newest message id seen (unchanged chats cost zero calls on sync)
#   scans     per chat+filter scan checkpoints: resumable backfill, then incremental via min_id
#   probes    header-probe result per document id (a file reposted in 10 chats is probed once)
#   downloads per document id (never downloaded twice) + song key for artist upgrades

SCHEMA = """
CREATE TABLE IF NOT EXISTS files(chat_id INTEGER, msg_id INTEGER, doc_id INTEGER, size INTEGER, duration REAL,
  mime TEXT, ext TEXT, title TEXT, performer TEXT, file_name TEXT, caption TEXT, date INTEGER,
  PRIMARY KEY(chat_id, msg_id));
CREATE INDEX IF NOT EXISTS files_doc ON files(doc_id);
CREATE TABLE IF NOT EXISTS chats(chat_id INTEGER PRIMARY KEY, title TEXT, username TEXT, kind TEXT, top_id INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS scans(chat_id INTEGER, filt TEXT, newest INTEGER DEFAULT 0, oldest INTEGER DEFAULT 0,
  done INTEGER DEFAULT 0, PRIMARY KEY(chat_id, filt));
CREATE TABLE IF NOT EXISTS probes(doc_id INTEGER PRIMARY KEY, info TEXT, version INTEGER, at INTEGER);
CREATE TABLE IF NOT EXISTS downloads(doc_id INTEGER PRIMARY KEY, path TEXT, song_key TEXT, score REAL, at INTEGER);
CREATE INDEX IF NOT EXISTS downloads_song ON downloads(song_key);
CREATE TABLE IF NOT EXISTS extra_chats(chat_id INTEGER PRIMARY KEY, username TEXT, added INTEGER);
CREATE TABLE IF NOT EXISTS remote_queries(kind TEXT, q TEXT, at INTEGER, hits INTEGER, result TEXT, PRIMARY KEY(kind, q));
CREATE TABLE IF NOT EXISTS verify(doc_id INTEGER PRIMARY KEY, result TEXT, version INTEGER, at INTEGER);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""
PROBE_VERSION = 2  # bump when probe code improves: failed probes from older versions get retried
FILTERS = {"music": InputMessagesFilterMusic, "files": InputMessagesFilterDocument}


def open_db():
    db = sqlite3.connect(DB, timeout=60)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")  # scheduled sync + manual searches can share the index
    except sqlite3.OperationalError:
        pass
    db.executescript(SCHEMA)
    return db


def ext_of(msg):
    name = msg.file.name or ""
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else (
        (msg.file.ext or "").lstrip(".").lower() or (msg.file.mime_type or "").split("/")[-1])
    return "flac" if ext == "x-flac" else ext


def is_audio(msg):
    if not msg.document:
        return False
    audio = next((a for a in msg.document.attributes if isinstance(a, DocumentAttributeAudio)), None)
    if audio and audio.voice:
        return False
    return (msg.file.mime_type or "").startswith("audio/") or ext_of(msg) in AUDIO_EXTS


def chat_kind(chat):
    if getattr(chat, "broadcast", False):
        return "channel"
    if getattr(chat, "megagroup", False) or getattr(chat, "title", None):
        return "group"
    return "user"


def remember_chat(db, chat_id, chat):
    title = getattr(chat, "title", None) or " ".join(filter(None, [getattr(chat, "first_name", None),
                                                                    getattr(chat, "last_name", None)])) or "?"
    db.execute("INSERT INTO chats(chat_id, title, username, kind) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET "
               "title=excluded.title, username=excluded.username, kind=excluded.kind",
               (chat_id, title, getattr(chat, "username", None), chat_kind(chat)))


def remember(db, msgs):
    """Store every audio message we see, from any command."""
    n = 0
    for msg in msgs:
        if not is_audio(msg):
            continue
        doc = msg.document
        audio = next((a for a in doc.attributes if isinstance(a, DocumentAttributeAudio)), None)
        if msg.chat:
            remember_chat(db, msg.chat_id, msg.chat)
        db.execute("INSERT OR REPLACE INTO files VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
            msg.chat_id, msg.id, doc.id, doc.size, (audio.duration if audio else 0) or 0, doc.mime_type or "",
            ext_of(msg), (audio.title if audio else None) or "", (audio.performer if audio else None) or "",
            msg.file.name or "", msg.message or "", int(msg.date.timestamp()) if msg.date else 0))
        n += 1
    return n


ROW_SQL = ("SELECT f.*, c.title AS chat_title, c.username, c.kind FROM files f "
           "LEFT JOIN chats c ON c.chat_id = f.chat_id")


def item_from_row(r):
    link = None
    if r["username"]:
        link = f"https://t.me/{r['username']}/{r['msg_id']}"
    elif r["kind"] in ("channel", "group") and str(r["chat_id"]).startswith("-100"):
        link = f"https://t.me/c/{str(r['chat_id'])[4:]}/{r['msg_id']}"
    return {
        "ref": f"{r['chat_id']}:{r['msg_id']}", "doc_id": r["doc_id"], "chat": r["chat_title"] or "?", "link": link,
        "title": r["title"], "performer": r["performer"], "file_name": r["file_name"], "caption": r["caption"][:300],
        "duration": r["duration"], "size": r["size"], "mime": r["mime"], "ext": r["ext"], "probed": False,
    }


def load_items(db, chat=None):
    sql, params = ROW_SQL, ()
    if chat:
        sql += " WHERE c.title LIKE ?"
        params = (f"%{chat}%",)
    return [item_from_row(r) for r in db.execute(sql, params)]


def apply_probes(db, items):
    """Assess every item using cached probe results (or the size/duration estimate)."""
    cache = {r["doc_id"]: r for r in db.execute("SELECT doc_id, info FROM probes")}
    vers = {r["doc_id"]: json.loads(r["result"]) for r in db.execute("SELECT doc_id, result FROM verify WHERE result IS NOT NULL")}
    for it in items:
        row = cache.get(it["doc_id"])
        assess(it, json.loads(row["info"]) if row and row["info"] else None, vers.get(it["doc_id"]))
    return items


# ---------- sync ----------

async def scan_chat(client, db, entity, chat_id, name, top, scans, label):
    """Backfill a chat once (checkpointed, resumable), afterwards fetch only posts newer than the last seen."""
    fetched = 0
    for fname, flt in FILTERS.items():
        st = scans.get((chat_id, fname)) or {"chat_id": chat_id, "filt": fname, "newest": top, "oldest": 0, "done": 0}
        tag = f"{label} {name[:30]} ({fname})"
        n = 0
        with err.status(f"{tag}…") as status:
            if not st["done"]:
                batch = []
                async for m in client.iter_messages(entity, filter=flt, offset_id=st["oldest"]):
                    batch.append(m)
                    st["oldest"] = m.id
                    if len(batch) >= 200:
                        n += remember(db, batch)
                        batch = []
                        save_scan(db, st)
                        status.update(f"{tag}: {n} audio…")
                n += remember(db, batch)
                st["done"] = 1
            else:
                msgs = [m async for m in client.iter_messages(entity, filter=flt, min_id=st["newest"])]
                n += remember(db, msgs)
                st["newest"] = max([st["newest"], top] + [m.id for m in msgs])
            save_scan(db, st)
        fetched += n
        if n:
            err.print(f"{tag}: +{n} audio")
    db.execute("UPDATE chats SET top_id=? WHERE chat_id=?", (top, chat_id))
    db.commit()
    return fetched


async def sync(client, db):
    """Scan every joined chat + every scanned public channel for audio; unchanged chats cost zero calls."""
    with err.status("Listing chats…"):
        dialogs = await client.get_dialogs()
    known = {r["chat_id"]: r["top_id"] for r in db.execute("SELECT chat_id, top_id FROM chats")}
    scans = {(r["chat_id"], r["filt"]): dict(r) for r in db.execute("SELECT * FROM scans")}
    done_all = lambda cid: all((scans.get((cid, f)) or {}).get("done") for f in FILTERS)
    fetched = skipped = 0
    for i, d in enumerate(dialogs, 1):
        top = d.message.id if d.message else 0
        remember_chat(db, d.id, d.entity)
        if known.get(d.id) == top and done_all(d.id):
            skipped += 1
            continue
        fetched += await scan_chat(client, db, d.entity, d.id, d.name, top, scans, f"[{i}/{len(dialogs)}]")
    joined = {d.id for d in dialogs}
    extra = [r for r in db.execute("SELECT chat_id, username FROM extra_chats") if r["chat_id"] not in joined]
    for r in extra:  # public channels indexed without joining: 1 call to see the newest post id
        entity = await client.get_entity(r["username"])
        latest = await client.get_messages(entity, limit=1)
        top = latest[0].id if latest else 0
        remember_chat(db, r["chat_id"], entity)
        if known.get(r["chat_id"]) == top and done_all(r["chat_id"]):
            skipped += 1
            continue
        fetched += await scan_chat(client, db, entity, r["chat_id"], getattr(entity, "title", r["username"]), top,
                                   scans, "[public]")
    db.execute("INSERT OR REPLACE INTO meta VALUES('last_sync', ?)", (str(int(time.time())),))
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    err.print(f"[green]Synced[/] {len(dialogs) + len(extra)} chats ({skipped} unchanged, skipped) · +{fetched} new"
              f" · {total} audio indexed")


@contextlib.contextmanager
def client_lock():
    """One Telegram connection at a time: a scheduled sync and a manual command must not share the session at once."""
    with open(HERE / ".tg.lock", "w") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            err.print("[yellow]Another hifigram command is using Telegram; waiting for it to finish…[/]")
            fcntl.flock(fh, fcntl.LOCK_EX)
        yield


def save_scan(db, st):
    db.execute("INSERT OR REPLACE INTO scans VALUES(:chat_id, :filt, :newest, :oldest, :done)", st)
    db.commit()


def synced_once(db):
    return db.execute("SELECT COUNT(*) FROM scans WHERE done=1").fetchone()[0] > 0


# ---------- quality ----------

def assess(item, info, ver=None):
    """Fill label/kbps/score. Lossless beats any lossy; then bit depth, then sample rate.
    A spectrum verify result (ver) overrides what the header claims."""
    ext, size = item["ext"], item["size"]
    item["probed"] = bool(info)
    item["channels"] = (info or {}).get("channels") or (ver or {}).get("channels")
    if info and info.get("length") and not (info.get("estimated") and item["duration"]):
        item["duration"] = round(info["length"])
    dur = item["duration"]
    kbps = (info or {}).get("kbps") or (round(size * 8 / dur / 1000) if dur else 0)
    lossless = ext in LOSSLESS or bool((info or {}).get("alac"))
    item["kbps"], item["lossless"] = kbps, lossless
    if lossless:
        fmt = "ALAC" if (info or {}).get("alac") else ext.upper()
        bits, sr = (info or {}).get("bits") or 0, (info or {}).get("sample_rate") or 0
        if bits and sr:
            item["label"] = f"{fmt} {bits}/{sr / 1000:g}"
            item["score"] = 10000 + min(bits, 24) * 100 + sr / 1000  # >24 bits: no real recording has them
        else:
            item["label"] = f"{fmt} ~{kbps}k"
            item["score"] = 10000 + kbps / 100
        item["tier"] = "hires" if item["score"] >= 12000 or sr > 48000 else "lossless"
        if ver:
            item["verified"] = ver["verdict"]
            if ver["verdict"] == "lossy-source":
                item["score"] = ver["eff_kbps"]
                item["tier"] = "320" if ver["eff_kbps"] >= 300 else "256" if ver["eff_kbps"] >= 240 else "low"
                item["label"] += f" ⚠ fake: lossy source ~{ver['eff_kbps']}k"
            elif ver.get("real_bits", bits) < bits or ver.get("real_rate", sr) < sr:
                rb, rsr = ver["real_bits"], ver["real_rate"]
                item["score"] = 10000 + min(rb, 24) * 100 + rsr / 1000
                item["tier"] = "hires" if rb > 16 and rsr > 48000 else "lossless"
                item["label"] += f" ⚠ really {rb}/{rsr / 1000:g}"
            else:
                item["label"] += " ? maybe 320k source" if ver["verdict"] == "suspect" else " ✓"
                item["score"] += 0.5 if ver["verdict"] == "genuine" else 0  # proven beats merely claimed
    else:
        fmt = {"m4a": "AAC", "oga": "OGG"}.get(ext, ext.upper())
        vbr = "V" if (info or {}).get("vbr") else ""
        item["label"] = f"{fmt} {vbr}{kbps}" if item["probed"] else f"{fmt} ~{kbps}"
        eff = kbps * CODEC_WEIGHT.get(ext, 1.0)
        item["score"] = eff
        item["tier"] = "320" if eff >= 300 else "256" if eff >= 240 else "192" if eff >= 180 else "low"
    ch = item["channels"]
    if ch and ch != 2:
        item["label"] += " mono" if ch == 1 else f" {ch - 1}.1" if ch in (6, 8) else f" {ch}ch"
    return item


def stereo_only(items):
    """Keep mono/stereo; drop surround (5.1 etc). Unknown channel count is kept."""
    return [it for it in items if (it.get("channels") or 2) <= 2]


async def ensure_probed(client, db, items, need_channels=False):
    """Header-probe only documents never probed before (or failed with older probe code); cache every result.
    need_channels: also re-read the header of older probes that lack the channel count (--stereo)."""
    if client is None:  # --offline: cached results only
        return apply_probes(db, items)
    have = {r["doc_id"]: r for r in db.execute("SELECT doc_id, info, version FROM probes")}
    vers = {r["doc_id"]: r["result"] for r in db.execute("SELECT doc_id, result FROM verify")}
    need = {}
    for it in items:
        row = have.get(it["doc_id"])
        if row and need_channels and row["info"] and "channels" not in json.loads(row["info"]):
            if not json.loads(vers.get(it["doc_id"]) or "{}").get("channels"):
                need.setdefault(it["doc_id"], it)
            continue
        if row and (row["info"] or row["version"] >= PROBE_VERSION):
            continue
        need.setdefault(it["doc_id"], it)
    if need:
        msgs = await fetch_messages(client, db, [it["ref"] for it in need.values()])
        loop = asyncio.get_running_loop()
        sem = asyncio.Semaphore(PROBE_CONCURRENCY)
        done = 0

        async def one(it):
            nonlocal done
            msg = msgs.get(it["ref"])
            if not msg:
                return
            async with sem:
                f = RemoteFile(client, loop, msg.document, it["file_name"])
                try:
                    info = await asyncio.to_thread(probe, f, it["ext"])
                except TRANSIENT as e:
                    err.print(f"[yellow]network trouble, will retry next run[/] {it['ref']}: {e}")
                    return
                except Exception:
                    info = None
            db.execute("INSERT OR REPLACE INTO probes VALUES(?,?,?,?)",
                       (it["doc_id"], json.dumps(info) if info else None, PROBE_VERSION, int(time.time())))
            db.commit()
            done += 1
            status.update(f"Checking real quality {done}/{len(need)} (new files only)…")

        with err.status(f"Checking real quality 0/{len(need)} (new files only)…") as status:
            await asyncio.gather(*(one(it) for it in need.values()))
    return apply_probes(db, items)


# ---------- fake detection (spectrum verify) ----------
# Decodes ~3.5 s at two points (35% and 65%) of the song. Only those two slices are fetched, sized from the bitrate
# (~0.5-2 MB for typical files, never the whole file), and analysed together:
#   - brick-wall cutoff: MP3/AAC encoders cut at 16-20 kHz
#   - empty ultrasonics: upsampling leaves nothing above the source's Nyquist (22.05/24 kHz)
#   - padded bits: "24/32-bit" samples whose low 8 or 16 bits are always zero (also catches 32-bit float made
#     from 24/16-bit audio: those values sit exactly on the 24/16-bit grid)
# A spectrogram PNG is saved for every check (spectrograms/<doc_id>.png). Results are cached per document;
# downloaded files are checked locally for free.

VERIFY_VERSION = 2
SAMPLE_POINTS = (0.35, 0.65)
SAMPLE_SECONDS = 4.5  # fetched per point; 3.5 s of it is decoded
SPECTRO_DIR = HERE / "spectrograms"


def audio_layout(f, ext):
    """(audio_start, audio_end, extra_ranges) of the encoded audio, reading headers only."""
    f.seek(0)
    head = f.read(12)
    if head[:4] == b"fLaC":
        f.seek(4)
        pos = 4
        for _ in range(128):
            h = f.read(4)
            length = int.from_bytes(h[1:4], "big")
            f.seek(length, 1)
            pos += 4 + length
            if h[0] & 0x80:
                break
        return pos, f.size, []
    if head[:4] == b"RIFF":
        pos = 12
        for _ in range(64):
            f.seek(pos)
            h = f.read(8)
            size = int.from_bytes(h[4:8], "little")
            if h[:4] == b"data":
                return pos + 8, min(f.size, pos + 8 + size), []
            pos += 8 + size + (size & 1)
    if head[4:8] == b"ftyp":
        top = {t: (s, e) for t, s, e in mp4_atoms(f, 0, f.size)}
        if b"mdat" in top and b"moov" in top:
            ms, me = top[b"moov"]
            extra = [(ms - 16, me - ms + 16)] if ms > top[b"mdat"][0] else []  # moov after mdat
            return top[b"mdat"][0], top[b"mdat"][1], extra
    return None


async def fetch_range(client, doc, offset, length):
    start = offset // BLOCK * BLOCK
    blocks = -(-(offset + length - start) // BLOCK)
    data = b"".join([bytes(c) async for c in client.iter_download(
        doc, offset=start, request_size=BLOCK, limit=blocks, file_size=doc.size)])
    return data[offset - start:offset - start + length]


async def decode(path, seek, seconds=3.5):
    """-> (pcm s32le bytes, channels). Never downmix: mixing would put fake data in the low bits."""
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", *(["-ss", f"{seek:.2f}"] if seek else []), "-i", str(path), "-t", str(seconds),
        "-f", "s32le", "-", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    pcm, log = await proc.communicate()
    m = re.search(r"Audio: .*?, \d+ Hz, (mono|stereo|(\d+) channels|(\d)\.(\d))", log.decode(errors="ignore"))
    ch = 2 if not m else 1 if m[1] == "mono" else 2 if m[1] == "stereo" else int(m[2]) if m[2] else int(m[3]) + int(m[4])
    return pcm, ch


def spectrum_verdict(samples, sr, claimed_bits):
    """samples: [(pcm, channels)]. Averages the spectrum of all samples, checks the low bits of all samples."""
    import numpy as np
    xs = [np.frombuffer(p[:len(p) // (4 * c) * 4 * c], dtype="<i4").reshape(-1, c) for p, c in samples if p]
    xs = [x for x in xs if len(x)]
    if not xs:
        return None
    allx = np.concatenate([x.ravel() for x in xs])
    thr = len(allx) * 0.002
    real_bits = 16 if np.count_nonzero(allx & 0xFFFF) < thr else 24 if np.count_nonzero(allx & 0xFF) < thr else 32
    real_bits = min(real_bits, claimed_bits)
    n = 8192
    win = np.hanning(n)
    spectra = []
    for x in xs:
        mono = x.astype(np.float64).mean(axis=1) / 2 ** 31
        spectra += [np.abs(np.fft.rfft(mono[i:i + n] * win)) ** 2 for i in range(0, len(mono) - n, n // 2)]
    if len(spectra) < 8:
        return None
    db = 10 * np.log10(np.mean(spectra, axis=0) + 1e-30)
    freqs = np.fft.rfftfreq(n, 1 / sr)
    nyq = sr / 2

    def band(lo, hi):
        sel = (freqs >= lo) & (freqs < hi)
        return float(np.median(db[sel])) if sel.any() else None

    # lossy encoders leave a brick wall at 16-20.5 kHz: find the sharpest drop below 21.5 kHz
    cutoff, best = None, 30.0
    for f in np.arange(11000, min(nyq - 1000, 21500), 100):
        below, above = band(f - 1500, f - 200), band(f + 200, f + 1500)
        if below is not None and above is not None and below - above > best:
            cutoff, best = float(f), below - above
    res = {"cutoff": cutoff, "real_bits": real_bits, "real_rate": sr, "claimed_bits": claimed_bits}
    if cutoff is not None and cutoff < 16500:
        res |= {"verdict": "lossy-source", "eff_kbps": 128}
    elif cutoff is not None and cutoff < 19200:
        res |= {"verdict": "lossy-source", "eff_kbps": 256}
    elif cutoff is not None and cutoff < 20600:
        res["verdict"] = "suspect"  # 320k MP3 cuts ~20 kHz, but some masters are low-passed there too
    else:
        res["verdict"] = "genuine"
        # upsampled: (almost) nothing above the source's Nyquist; real hi-res keeps ultrasonic content/noise
        if sr > 48000:
            ref = band(15000, 19000)
            hf = band(27000, min(nyq, 40000))
            if ref is not None and hf is not None and ref - hf > 60:
                edge = next((float(f) for f in np.arange(19000, 30000, 100) if (band(f, f + 300) or -999) < ref - 30), None)
                res |= {"verdict": "upsampled", "real_rate": 44100 if edge and edge < 23300 else 48000, "cutoff": edge}
    if res["verdict"] == "genuine" and real_bits < claimed_bits:
        res["verdict"] = "padded"
    return res


async def save_spectrogram(samples, sr, doc_id):
    pcm, ch = samples[0][0] + b"".join(p for p, c in samples[1:] if c == samples[0][1]), samples[0][1]
    if not pcm:
        return
    SPECTRO_DIR.mkdir(exist_ok=True)
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-v", "quiet", "-y", "-f", "s32le", "-ar", str(sr), "-ac", str(ch), "-i", "-",
        "-lavfi", "showspectrumpic=s=1200x500:legend=1:scale=log", str(SPECTRO_DIR / f"{doc_id}.png"),
        stdin=asyncio.subprocess.PIPE)
    await proc.communicate(pcm)


def sample_bytes(info, start, end):
    kbps = info.get("kbps") or (end - start) * 8 / max(info.get("length") or 1, 1) / 1000
    return int(min(max(kbps * 1000 / 8 * SAMPLE_SECONDS + 65536, 256 * 1024), 2.5 * 1024 * 1024, end - start))


async def verify_one(client, msg, it, info):
    """Remote: fetch only the two slices (+ headers) and analyse them."""
    loop = asyncio.get_running_loop()
    rf = RemoteFile(client, loop, msg.document, it["file_name"])
    lay = await asyncio.to_thread(audio_layout, rf, it["ext"])
    if not lay or not info or not info.get("sample_rate"):
        return None
    start, end, extra = lay
    length = sample_bytes(info, start, end)
    dur = info.get("length") or it["duration"]
    secs = min(3.5, 0.8 * length / max(end - start, 1) * dur)  # decode only audio we actually fetched
    offs = [start + int((end - start - length) * p) for p in SAMPLE_POINTS]
    chunks = [await fetch_range(client, msg.document, o, length) for o in offs]
    rf.seek(0)
    samples = []
    with tempfile.TemporaryDirectory() as tmp:
        if it["ext"] == "flac":  # metadata + slice; the decoder resyncs on the next frame
            head = await asyncio.to_thread(rf.read, start)
            for i, chunk in enumerate(chunks):
                path = Path(tmp) / f"{i}.flac"
                path.write_bytes(head + chunk)
                samples.append(await decode(path, 0, secs))
        else:  # sparse file: real headers + slices at their true offsets (no real disk use), seek into each slice
            path = Path(tmp) / f"x.{it['ext']}"
            with open(path, "wb") as fh:
                fh.write(await asyncio.to_thread(rf.read, start + 65536))
                for off, n in extra:
                    fh.seek(off)
                    fh.write(await fetch_range(client, msg.document, off, n))
                for o, chunk in zip(offs, chunks):
                    fh.seek(o)
                    fh.write(chunk)
                fh.truncate(msg.document.size)
            for o in offs:
                samples.append(await decode(path, dur * ((o - start) + length * 0.1) / (end - start), secs))
    res = await asyncio.to_thread(spectrum_verdict, samples, info["sample_rate"], info.get("bits") or 16)
    if res:
        res["fetched_mb"] = round((length * len(offs) + rf.fetched) / 1e6, 2)
        res["channels"] = samples[0][1]
        await save_spectrogram(samples, info["sample_rate"], it["doc_id"])
    return res


class LocalFile(io.FileIO):
    def __init__(self, path):
        super().__init__(path)
        self.size = os.path.getsize(path)


async def verify_local(db, path, it):
    """Downloaded file: probe + verify from disk, zero network. Cached like remote results."""
    try:
        with LocalFile(path) as f:
            info = await asyncio.to_thread(probe, f, it["ext"])
        if info:
            db.execute("INSERT OR REPLACE INTO probes VALUES(?,?,?,?)",
                       (it["doc_id"], json.dumps(info), PROBE_VERSION, int(time.time())))
        res = None
        if info and info.get("sample_rate") and it.get("lossless"):
            dur = info.get("length") or it["duration"]
            samples = [await decode(path, dur * p) for p in SAMPLE_POINTS]
            res = await asyncio.to_thread(spectrum_verdict, samples, info["sample_rate"], info.get("bits") or 16)
            if res:
                res["fetched_mb"] = 0
                res["channels"] = samples[0][1]
                await save_spectrogram(samples, info["sample_rate"], it["doc_id"])
            db.execute("INSERT OR REPLACE INTO verify VALUES(?,?,?,?)",
                       (it["doc_id"], json.dumps(res) if res else None, VERIFY_VERSION, int(time.time())))
        db.commit()
        return res
    except Exception as e:
        err.print(f"[yellow]local verify failed[/] {path}: {e}")


async def verify_items(client, db, items):
    """Spectrum-verify lossless items never verified (with current code) before; cache every result."""
    if client is None:
        return
    done = {r["doc_id"] for r in db.execute("SELECT doc_id FROM verify WHERE version>=?", (VERIFY_VERSION,))}
    need = {}
    for it in items:
        if it["lossless"] and it["doc_id"] not in done:
            need.setdefault(it["doc_id"], it)
    if not need:
        return
    if not shutil.which("ffmpeg"):
        sys.exit("verify needs ffmpeg: brew install ffmpeg")
    msgs = await fetch_messages(client, db, [it["ref"] for it in need.values()])
    infos = {r["doc_id"]: json.loads(r["info"]) for r in db.execute("SELECT doc_id, info FROM probes WHERE info IS NOT NULL")}
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    count, mb = 0, 0.0

    async def one(it):
        nonlocal count, mb
        msg = msgs.get(it["ref"])
        if not msg:
            return
        async with sem:
            try:
                res = await verify_one(client, msg, it, infos.get(it["doc_id"]))
            except TRANSIENT as e:
                err.print(f"[yellow]network trouble, will retry next run[/] {it['ref']}: {e}")
                return
            except Exception as e:
                err.print(f"[yellow]verify failed[/] {it['ref']}: {e}")
                res = None
        db.execute("INSERT OR REPLACE INTO verify VALUES(?,?,?,?)",
                   (it["doc_id"], json.dumps(res) if res else None, VERIFY_VERSION, int(time.time())))
        db.commit()
        count += 1
        mb += (res or {}).get("fetched_mb", 0)
        status.update(f"Spectrum check {count}/{len(need)} ({mb:.1f} MB fetched)…")

    with err.status(f"Spectrum check 0/{len(need)}…") as status:
        await asyncio.gather(*(one(it) for it in need.values()))
    err.print(f"Spectrum checked {count} files, {mb:.1f} MB fetched · spectrograms in {SPECTRO_DIR}")


async def verify_best(client, db, items, groups_fn, rounds=3):
    """Verify the best copy of each group; if it turns out fake, the next copy moves up and gets verified too."""
    for _ in range(rounds):
        groups = groups_fn(apply_probes(db, items))
        await verify_items(client, db, [g[0] for g in groups])
        groups = groups_fn(apply_probes(db, items))
        if all(not g[0]["lossless"] or g[0].get("verified") for g in groups):
            break
    return groups_fn(apply_probes(db, items))


# ---------- matching / grouping ----------

def norm_text(s):
    return " ".join(re.sub(PUNCT, " ", unicodedata.normalize("NFKC", s or "").lower()).split())


def norm_title(item, strip=()):
    raw = item["title"] or item["file_name"] or item["caption"].split("\n")[0]
    s = unicodedata.normalize("NFKC", raw).lower()  # 𝐌𝐚𝐠𝐚𝐥𝐞 -> magale
    s = re.sub(r"\.(mp3|flac|m4a|wav|aac|opus|ogg|alac|aiff?)$", "", s)
    tags = [w for w in VERSION_WORDS if re.search(rf"\b{w}\b", s)]
    s = re.sub(r"[\[\(\{].*?[\]\)\}]", " ", s)
    s = re.sub(r"@\w+|https?://\S+|t\.me/\S+", " ", s)
    s = re.sub(r"\b\d{2,4}\s?kbps\b|\b(hq|hd|flac|mp3|lossless|hi-?res|24bit|16bit|320|256|128|official|audio|full song)\b", " ", s)
    s = re.sub(r"^\s*\d{1,3}\s*[-._)]\s*", " ", s)
    s = " " + " ".join(re.sub(PUNCT, " ", s).split()) + " "
    for a in strip:
        s = s.replace(" " + norm_text(a) + " ", " ")
    s = " ".join(s.split())
    return " ".join([s] + [t for t in tags if t not in s.split()]).strip()


def match(items, queries, min_score=80):
    """Local fuzzy search: every query word must match a word in title/artist/file name/caption
    (prefix or typo-tolerant). Returns items matching any query."""
    qs = [norm_text(q).split() for q in queries if norm_text(q)]
    hits = []
    for it in items:
        toks = set(norm_text(" ".join([it["title"], it["performer"], it["file_name"], it["caption"]])).split())
        best = 0
        for q in qs:
            scores = []
            for w in q:
                if w in toks or any(t.startswith(w) for t in toks):
                    scores.append(100)
                    continue
                m = process.extractOne(w, toks, scorer=fuzz.ratio, score_cutoff=min_score) if len(w) > 3 else None
                if not m:
                    break
                scores.append(m[1])
            else:
                best = max(best, sum(scores) / len(scores))
        if best:
            it["match"] = round(best)
            hits.append(it)
    return hits


def group(items, strip=()):
    """Cluster copies of the same song: similar cleaned title and duration within 3s. Best copy first.
    The same document posted in several chats counts once."""
    seen, uniq = set(), []
    for it in sorted(items, key=lambda x: (-x["score"], -x["duration"])):
        if it["doc_id"] not in seen:
            seen.add(it["doc_id"])
            uniq.append(it)
    clusters = []
    for it in uniq:
        it["norm"] = norm_title(it, strip)
        for c in clusters:
            head = c[0]
            known = it["duration"] and head["duration"]
            if known and abs(it["duration"] - head["duration"]) > 3:
                continue
            sim = max(fuzz.ratio(it["norm"], head["norm"]), fuzz.token_sort_ratio(it["norm"], head["norm"]))
            if sim >= (88 if known else 95):
                c.append(it)
                break
        else:
            clusters.append([it])
    return clusters


def public(item):
    return {k: v for k, v in item.items() if not k.startswith("_")}


TIER_STYLE = {"hires": "magenta", "lossless": "green", "320": "cyan", "256": "blue", "192": "yellow", "low": "dim"}


def show_groups(groups):
    table = Table(header_style="bold")
    for col in ("#", "Quality", "Size", "Len", "Channel", "Title"):
        table.add_column(col, no_wrap=col != "Title")
    flat = []
    for gi, g in enumerate(groups):
        if gi:
            table.add_section()
        for it in g:
            m, s = divmod(int(it["duration"]), 60)
            q = f"[{TIER_STYLE[it['tier']]}]{it['label']}[/]" + ("" if it["probed"] else " [dim]est[/]")
            name = unicodedata.normalize("NFKC", " - ".join(filter(None, [it["performer"], it["title"]])) or it["file_name"] or it["caption"][:60])
            table.add_row(str(len(flat)), q, f"{it['size'] / 1e6:.1f}MB", f"{m}:{s:02d}", it["chat"][:24], name)
            flat.append(it)
    out.print(table)
    return flat


# ---------- download ----------

def safe_name(s):
    s = re.sub(r'[\\/:*?"<>|\n\r\t]+', " ", s).strip(" .")
    return " ".join(s.split())[:150] or "audio"


def file_name_for(it):
    base = " - ".join(filter(None, [it["performer"], it["title"]])) or Path(it["file_name"]).stem or it.get("norm") or "audio"
    return f"{safe_name(unicodedata.normalize('NFKC', base))}.{it['ext'] or 'bin'}"


async def fetch_messages(client, db, refs):
    """refs: ['chat_id:msg_id', ...] -> {ref: Message}. 100 ids per call; refreshes the index; drops deleted posts."""
    by_chat = {}
    for r in refs:
        chat, mid = r.rsplit(":", 1)
        by_chat.setdefault(int(chat), []).append(int(mid))
    found = {}
    for chat, ids in by_chat.items():
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            try:
                msgs = await client.get_messages(chat, ids=chunk)
            except ValueError:  # entity not in session cache yet
                await client.get_dialogs()
                msgs = await client.get_messages(chat, ids=chunk)
            remember(db, [m for m in msgs if m])
            for mid, m in zip(chunk, msgs):
                if m and m.document:
                    found[f"{chat}:{mid}"] = m
                else:
                    db.execute("DELETE FROM files WHERE chat_id=? AND msg_id=?", (chat, mid))
    db.commit()
    return found


async def download_items(client, db, picks):
    """picks: [(item, dest_dir, song_key|None)]. Skips any document already downloaded (same file, any chat/name)."""
    have = {r["doc_id"]: r["path"] for r in db.execute("SELECT doc_id, path FROM downloads")}
    todo = []
    for it, dest, key in picks:
        if it["doc_id"] in have and Path(have[it["doc_id"]]).exists():
            err.print(f"[green]Have[/] {have[it['doc_id']]}")
            continue
        todo.append((it, dest, key))
    if not todo:
        return
    msgs = await fetch_messages(client, db, [it["ref"] for it, _, _ in todo])
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
    done = 0

    async def one(it, dest, key):
        nonlocal done
        msg = msgs.get(it["ref"])
        if not msg:
            err.print(f"[red]Gone[/] {it['ref']} (post deleted)")
            return
        async with sem:
            dest.mkdir(parents=True, exist_ok=True)
            name = file_name_for(it)
            final, n = dest / name, 2
            while final.exists():
                final = dest / f"{Path(name).stem} ({n}){Path(name).suffix}"
                n += 1
            part = final.with_name(final.name + ".part")
            try:
                await client.download_media(msg, file=str(part))
            except Exception as e:
                err.print(f"[red]Failed[/] {name}: {e}")
                return
            part.rename(final)
        db.execute("INSERT OR REPLACE INTO downloads VALUES(?,?,?,?,?)",
                   (it["doc_id"], str(final), key, it.get("score"), int(time.time())))
        db.commit()
        done += 1
        res = await verify_local(db, final, it) if it.get("lossless") and shutil.which("ffmpeg") else None
        note = f" · {res['verdict']}" if res else ""
        err.print(f"[green]Saved[/] ({done}/{len(todo)}) {final}{note}")

    await asyncio.gather(*(one(*t) for t in todo))


# ---------- artist plans ----------

def slug(s):
    return re.sub(r"[^\w]+", "-", s.lower()).strip("-")


def artist_matches(it, variants):
    hay = norm_text(" ".join([it["performer"], it["title"], it["file_name"], it["caption"]]))
    squashed = hay.replace(" ", "")
    return any(norm_text(v) in hay or norm_text(v).replace(" ", "") in squashed for v in variants)


async def build_plan(client, db, variants, verify=False, stereo=False):
    items = apply_probes(db, match(load_items(db), variants))
    # probe only what can change the pick: lossless copies, or lossy copies of songs with no lossless copy
    groups = group(items, strip=variants)
    to_probe = []
    for g in groups:
        lossless = [it for it in g if it["lossless"]]
        to_probe += lossless or g
    await ensure_probed(client, db, to_probe, stereo)
    if stereo:
        items = stereo_only(items)
    groups = group(items, strip=variants)
    if verify:
        groups = await verify_best(client, db, items, lambda its: group(its, strip=variants))
    entries = []
    for g in groups:
        best = g[0]
        flags = []
        if not any(artist_matches(it, variants) for it in g):
            flags.append("artist-not-in-metadata")
        if not best["lossless"] and best["score"] < 240:
            flags.append("low-quality-only")
        entries.append({"song": best["norm"], "duration": best["duration"], "best": public(best),
                        "alternatives": [public(it) for it in g[1:6]], "copies": len(g), "flags": flags,
                        "skip": False})
    plan = {"artist": variants[0], "variants": variants, "songs": len(entries),
            "total_mb": round(sum(e["best"]["size"] for e in entries) / 1e6, 1),
            "lossless": sum(e["best"]["lossless"] for e in entries), "entries": entries}
    PLANS.mkdir(exist_ok=True)
    path = PLANS / f"{slug(variants[0])}.json"
    path.write_text(json.dumps(plan, indent=1, ensure_ascii=False))
    return plan, path


def show_plan(plan, path):
    table = Table(header_style="bold")
    for col in ("#", "Quality", "Size", "Len", "Copies", "Song", "Flags"):
        table.add_column(col, no_wrap=col not in ("Song", "Flags"))
    for i, e in enumerate(plan["entries"]):
        b = e["best"]
        m, s = divmod(int(e["duration"]), 60)
        table.add_row(str(i), f"[{TIER_STYLE[b['tier']]}]{b['label']}[/]", f"{b['size'] / 1e6:.1f}MB", f"{m}:{s:02d}",
                      str(e["copies"]), e["song"], ", ".join(e["flags"]))
    out.print(table)
    out.print(f"{plan['songs']} unique songs · {plan['lossless']} lossless · {plan['total_mb']} MB → {path}")


async def run_plan(client, db, plan):
    artist_dir = DOWNLOADS / safe_name(plan["artist"].title())
    picks = []
    for e in plan["entries"]:
        if e["skip"]:
            continue
        key = f"{slug(plan['artist'])}|{e['song']}|{round(e['duration'] / 3)}"
        have = db.execute("SELECT path, score FROM downloads WHERE song_key=? ORDER BY score DESC", (key,)).fetchone()
        if have and have["score"] is not None and have["score"] >= e["best"]["score"] and Path(have["path"]).exists():
            continue
        picks.append((e["best"] | {"norm": e["song"]}, artist_dir, key))
    err.print(f"{len(picks)} to download, {len(plan['entries']) - len(picks)} skipped/already have")
    await download_items(client, db, picks)


# ---------- remote fallback / public channels ----------

REMOTE_TTL = 7 * 86400  # same remote query within a week is answered from the index


async def remote_search(client, db, queries, force=False):
    """Telegram global search for queries the index can't answer; results are stored, the query is logged."""
    now, fetched = int(time.time()), 0
    for q in queries:
        nq = norm_text(q)
        row = db.execute("SELECT at FROM remote_queries WHERE kind='audio' AND q=?", (nq,)).fetchone()
        if row and not force and now - row["at"] < REMOTE_TTL:
            continue
        hits = 0
        with err.status(f"Not in index, asking Telegram: “{q}”…"):
            for flt in FILTERS.values():
                hits += remember(db, [m async for m in client.iter_messages(None, search=q, filter=flt, limit=200)])
        db.execute("INSERT OR REPLACE INTO remote_queries VALUES('audio', ?, ?, ?, NULL)", (nq, now, hits))
        db.commit()
        fetched += hits
    return fetched


async def cmd_channels(client, db, args):
    """Find public channels by name (cached a week). Nothing is joined or scanned."""
    from telethon import utils
    from telethon.tl.functions.contacts import SearchRequest
    nq, now = norm_text(args.query), int(time.time())
    row = db.execute("SELECT at, result FROM remote_queries WHERE kind='channels' AND q=?", (nq,)).fetchone()
    if row and now - row["at"] < REMOTE_TTL:
        found = json.loads(row["result"])
    else:
        res = await client(SearchRequest(q=args.query, limit=50))
        found = [{"id": utils.get_peer_id(c), "title": c.title, "username": c.username,
                  "members": getattr(c, "participants_count", None), "joined": not getattr(c, "left", True),
                  "kind": chat_kind(c)} for c in res.chats if getattr(c, "username", None)]
        db.execute("INSERT OR REPLACE INTO remote_queries VALUES('channels', ?, ?, ?, ?)",
                   (nq, now, len(found), json.dumps(found)))
        db.commit()
    indexed = {r["chat_id"] for r in db.execute("SELECT DISTINCT chat_id FROM scans WHERE done=1")}
    for c in found:
        c["indexed"] = c["id"] in indexed
    if args.json:
        print(json.dumps(found, ensure_ascii=False, indent=1))
        return
    table = Table(header_style="bold")
    for col in ("Channel", "@", "Members", "Joined", "Indexed"):
        table.add_column(col)
    for c in found:
        table.add_row(c["title"], c["username"], str(c["members"] or "?"), "yes" if c["joined"] else "",
                      "yes" if c["indexed"] else "")
    out.print(table)
    out.print("Index one without joining: hifigram.py scan @username")


async def cmd_scan(client, db, args):
    """Index public channels without joining; later syncs fetch only their new posts."""
    from telethon import utils
    scans = {(r["chat_id"], r["filt"]): dict(r) for r in db.execute("SELECT * FROM scans")}
    for name in args.channels:
        username = name.rstrip("/").split("/")[-1].lstrip("@")
        entity = await client.get_entity(username)
        chat_id = utils.get_peer_id(entity)
        db.execute("INSERT OR IGNORE INTO extra_chats VALUES(?,?,?)", (chat_id, username, int(time.time())))
        remember_chat(db, chat_id, entity)
        latest = await client.get_messages(entity, limit=1)
        top = latest[0].id if latest else 0
        n = await scan_chat(client, db, entity, chat_id, getattr(entity, "title", username), top, scans, "[scan]")
        err.print(f"[green]Indexed[/] {getattr(entity, 'title', username)}: +{n} audio")


async def cmd_verify(client, db, args):
    items = []
    for ref in args.refs:
        chat, mid = ref.rsplit(":", 1)
        r = db.execute(ROW_SQL + " WHERE f.chat_id=? AND f.msg_id=?", (int(chat), int(mid))).fetchone()
        if r:
            items.append(item_from_row(r))
        else:
            err.print(f"[red]Not in index[/] {ref}")
    items = await ensure_probed(client, db, items)
    await verify_items(client, db, items)
    apply_probes(db, items)
    if args.json:
        print(json.dumps([public(i) for i in items], ensure_ascii=False, indent=1))
    else:
        show_groups([[i] for i in items])


# ---------- commands ----------

async def prepare(client, db, args):
    """Cheap incremental sync before local queries (unchanged chats cost nothing)."""
    if getattr(args, "offline", False) or client is None:
        return
    await sync(client, db)


async def cmd_search(client, db, args):
    await prepare(client, db, args)
    items = match(load_items(db, args.chat), args.queries)
    if (not items or args.remote) and not args.offline:
        if await remote_search(client, db, args.queries, force=args.remote):
            items = match(load_items(db, args.chat), args.queries)
    items = await ensure_probed(client, db, items, args.stereo) if not args.no_probe else apply_probes(db, items)
    if args.stereo:
        items = stereo_only(items)
    groups = group(items)[:args.limit]
    if args.verify:  # only the top 10 song groups (each check fetches ~0.5-4 MB)
        top = await verify_best(client, db, [i for g in groups[:10] for i in g], group)
        groups = (top + groups[10:])[:args.limit]
    if args.json:
        print(json.dumps([{"song": g[0]["norm"], "duration": g[0]["duration"], "copies": [public(i) for i in g]}
                          for g in groups], ensure_ascii=False, indent=1))
    else:
        show_groups(groups)


async def cmd_top(client, db, args):
    await prepare(client, db, args)
    items = load_items(db, args.chat)
    if args.queries:
        items = match(items, args.queries)
    items = [it for it in items if it["duration"] >= args.min_length]
    apply_probes(db, items)
    # shortlist: everything already probed + highest estimated bitrate lossless candidates (true hi-res is far ahead)
    unprobed = sorted((it for it in items if not it["probed"]), key=lambda it: (-it["lossless"], -it["kbps"]))
    shortlist = [it for it in items if it["probed"]]
    batch = max(100, args.n * 20)
    while True:  # with --stereo, surround files drop out: widen the shortlist until n songs remain
        shortlist += unprobed[:batch]
        unprobed = unprobed[batch:]
        shortlist = await ensure_probed(client, db, shortlist, args.stereo)
        if args.stereo:
            shortlist = stereo_only(shortlist)
        if not args.stereo or len(group(shortlist)) >= args.n * 3 or not unprobed or client is None:
            break
    groups = group(shortlist)[:args.n]
    if args.verify:  # fakes drop out, next-best copies/songs move up and get verified too
        groups = (await verify_best(client, db, shortlist, lambda its: group(its)[:args.n]))[:args.n]
    if args.json:
        print(json.dumps([{"song": g[0]["norm"], "duration": g[0]["duration"], "copies": [public(i) for i in g]}
                          for g in groups], ensure_ascii=False, indent=1))
    else:
        show_groups([g[:3] for g in groups])


async def cmd_get(client, db, args):
    def row(ref):
        chat, mid = ref.rsplit(":", 1)
        return db.execute(ROW_SQL + " WHERE f.chat_id=? AND f.msg_id=?", (int(chat), int(mid))).fetchone()

    missing = [r for r in args.refs if not row(r)]
    if missing:
        await fetch_messages(client, db, missing)  # not indexed yet: fetch once, now it is
    items = []
    for ref in args.refs:
        r = row(ref)
        if not r:
            err.print(f"[red]Not found[/] {ref}")
            continue
        items.append(item_from_row(r))
    apply_probes(db, items)
    await download_items(client, db, [(it, Path(args.dir), None) for it in items])


async def cmd_artist(client, db, args):
    path = PLANS / f"{slug(args.names[0])}.json"
    if args.download and path.exists() and not args.replan:
        plan = json.loads(path.read_text())
        err.print(f"Using saved plan {path} ({plan['songs']} songs)")
    else:
        await prepare(client, db, args)
        plan, path = await build_plan(client, db, args.names, verify=args.verify, stereo=args.stereo)
        if args.json:
            print(json.dumps(plan | {"plan_file": str(path)}, ensure_ascii=False, indent=1))
        else:
            show_plan(plan, path)
    if args.download:
        await run_plan(client, db, plan)


async def cmd_stats(client, db, args):
    q = lambda sql: db.execute(sql).fetchone()[0]
    out.print(f"audio posts {q('SELECT COUNT(*) FROM files')} · unique files {q('SELECT COUNT(DISTINCT doc_id) FROM files')}"
              f" · probed {q('SELECT COUNT(*) FROM probes WHERE info IS NOT NULL')}"
              f" · downloads {q('SELECT COUNT(*) FROM downloads')}"
              f" · chats scanned {q('SELECT COUNT(DISTINCT chat_id) FROM scans WHERE done=1')}"
              f" · pending scans {q('SELECT COUNT(*) FROM scans WHERE done=0')}")
    for r in db.execute("SELECT c.title, COUNT(*) n FROM files f JOIN chats c ON c.chat_id=f.chat_id "
                        "GROUP BY f.chat_id ORDER BY n DESC LIMIT 15"):
        out.print(f"  {r['n']:>7}  {r['title']}")


async def interactive(client, db):
    await sync(client, db)
    while True:
        q = input("\nSearch (blank to quit): ").strip()
        if not q:
            return
        items = await ensure_probed(client, db, match(load_items(db), [q]))
        flat = show_groups(group(items))
        if not flat:
            err.print("[red]No audio found.[/]")
            continue
        picks = input("Download # (e.g. 0 or 0,3 — blank to skip): ").strip()
        chosen = [flat[int(p)] for p in picks.replace(" ", "").split(",") if p]
        await download_items(client, db, [(it, DOWNLOADS, None) for it in chosen])


async def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("login")
    sub.add_parser("sync")
    sub.add_parser("stats")
    s = sub.add_parser("search")
    s.add_argument("queries", nargs="+")
    s.add_argument("--json", action="store_true")
    s.add_argument("--chat", help="only chats whose title contains this")
    s.add_argument("--limit", type=int, default=100, help="max song groups")
    s.add_argument("--no-probe", action="store_true", help="skip real-quality header check")
    s.add_argument("--offline", action="store_true", help="don't sync first, never call Telegram")
    s.add_argument("--remote", action="store_true", help="also ask Telegram (default: only when index has nothing)")
    s.add_argument("--verify", action="store_true", help="spectrum-check best copies for fakes (~1-5 MB each, cached)")
    s.add_argument("--stereo", action="store_true", help="skip surround (5.1 etc) files")
    t = sub.add_parser("top")
    t.add_argument("n", type=int, nargs="?", default=5)
    t.add_argument("queries", nargs="*")
    t.add_argument("--chat")
    t.add_argument("--min-length", type=int, default=60, help="skip tracks shorter than this many seconds")
    t.add_argument("--json", action="store_true")
    t.add_argument("--offline", action="store_true")
    t.add_argument("--verify", action="store_true", help="spectrum-check for fakes (~1-5 MB each, cached)")
    t.add_argument("--stereo", action="store_true", help="skip surround (5.1 etc) files")
    v = sub.add_parser("verify")
    v.add_argument("refs", nargs="+")
    v.add_argument("--json", action="store_true")
    c = sub.add_parser("channels")
    c.add_argument("query")
    c.add_argument("--json", action="store_true")
    sc = sub.add_parser("scan")
    sc.add_argument("channels", nargs="+", help="@username or t.me link of a public channel")
    g = sub.add_parser("get")
    g.add_argument("refs", nargs="+", help="CHAT_ID:MSG_ID from search output")
    g.add_argument("--dir", default=str(DOWNLOADS))
    a = sub.add_parser("artist")
    a.add_argument("names", nargs="+", help="artist name first, then spelling variants")
    a.add_argument("--plan", action="store_true", help="build plan only (default)")
    a.add_argument("--download", action="store_true", help="download saved plan (builds one if missing)")
    a.add_argument("--replan", action="store_true", help="with --download: rebuild plan first")
    a.add_argument("--json", action="store_true")
    a.add_argument("--offline", action="store_true")
    a.add_argument("--verify", action="store_true", help="spectrum-check best copy of each song (~1-5 MB each, cached)")
    a.add_argument("--stereo", action="store_true", help="skip surround (5.1 etc) files")
    args = p.parse_args()

    db = open_db()
    if args.cmd == "stats":
        return await cmd_stats(None, db, args)
    if getattr(args, "offline", False) and args.cmd in ("search", "top", "artist") and not getattr(args, "download", False):
        if not synced_once(db) and not db.execute("SELECT 1 FROM files LIMIT 1").fetchone():
            sys.exit("Index empty. Run: hifigram.py sync")
        return await {"search": cmd_search, "top": cmd_top, "artist": cmd_artist}[args.cmd](None, db, args)
    with client_lock():
        await run_command(args, db)


async def run_command(args, db):
    client = await connect(interactive=args.cmd in (None, "login"))
    async with client:
        if args.cmd == "login":
            me = await client.get_me()
            err.print(f"[green]Logged in as[/] {me.first_name} (@{me.username})")
        elif args.cmd == "sync":
            await sync(client, db)
        elif args.cmd == "search":
            await cmd_search(client, db, args)
        elif args.cmd == "top":
            await cmd_top(client, db, args)
        elif args.cmd == "get":
            await cmd_get(client, db, args)
        elif args.cmd == "artist":
            await cmd_artist(client, db, args)
        elif args.cmd == "verify":
            await cmd_verify(client, db, args)
        elif args.cmd == "channels":
            await cmd_channels(client, db, args)
        elif args.cmd == "scan":
            await cmd_scan(client, db, args)
        else:
            await interactive(client, db)


if __name__ == "__main__":
    asyncio.run(main())
