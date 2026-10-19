"""hifigram: find the best-quality copy of any song across your Telegram channels.

Usage:
  hifigram.py                                   interactive search + pick
  hifigram.py login                             one-time Telegram login
  hifigram.py sync                              index audio from all chats (resumable; later runs fetch only new posts)
  hifigram.py stats                             what's in the index
  hifigram.py search "q1" "q2" [--json]         fuzzy search the index, grouped by song, best copy first
  hifigram.py top 5 ["query"] [--chat NAME]     highest-quality unique songs
  hifigram.py verify CHAT:MSG ...               spectrum check for fake lossless (~1-5 MB each); also --verify on search/top/artist
  hifigram.py channels "tamil flac"             find public channels by name (nothing joined)
  hifigram.py scan @channel                     index a public channel without joining
  hifigram.py albums "name or artist"          find albums: songs found, missing tracks, best quality
  hifigram.py album "A" "B|alt" --plan          best copy of every track of each album -> plans/album-*.json
  hifigram.py album "A" "B" --download          download those albums (resumable, numbered files)
  hifigram.py stats --categories               formats, languages, lengths, versions, genres, decades, albums
  hifigram.py inspect CHAT:MSG                 every stored version of a post, exactly as Telegram sent it
  hifigram.py enrich                           re-read headers checked before every field was kept
  filters (search/top/artist/albums/album): --format flac,wav --lang tamil --version instrumental --no-version karaoke
                                             --length song --genre rock --decade 1990s
  hifigram.py get CHAT:MSG [CHAT:MSG ...]       download specific files
  hifigram.py artist "name" "variant" --plan    best copy of every unique song -> plans/<artist>.json
  hifigram.py artist "name" --download          download that plan (resumable, skips what you have)
"""
import argparse
import asyncio
import concurrent.futures
import contextlib
import fcntl
import hashlib
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
import zlib
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
BYTES_DB = HERE / "bytes.db"  # every byte ever read from a file, by (document, 128 KB block)

LOSSLESS = {"flac", "alac", "wav", "aiff", "aif", "ape", "wv", "dsf", "dff"}
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


class ByteStore:
    """Every 128 KB block ever read from a Telegram file, kept forever (bytes.db). Reads hit it first."""

    def __init__(self):
        self.db = sqlite3.connect(BYTES_DB, timeout=60)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS blocks(doc_id INTEGER, idx INTEGER, at INTEGER, data BLOB, "
                        "PRIMARY KEY(doc_id, idx))")

    def get(self, doc_id, idx):
        row = self.db.execute("SELECT data FROM blocks WHERE doc_id=? AND idx=?", (doc_id, idx)).fetchone()
        return row[0] if row else None

    def put(self, doc_id, idx, data):
        self.db.execute("INSERT OR IGNORE INTO blocks VALUES(?,?,?,?)", (doc_id, idx, int(time.time()), data))
        self.db.commit()


_BYTES = None
NET = {"bytes": 0}  # bytes actually pulled from Telegram this run (cache hits excluded)


def byte_store():
    global _BYTES
    if _BYTES is None:
        _BYTES = ByteStore()
    return _BYTES


async def get_blocks(client, doc, first, last, counter=None):
    """Blocks first..last of a document: from the byte cache, fetching only missing runs (then cached)."""
    st, blocks, missing = byte_store(), {}, []
    for i in range(first, last + 1):
        d = st.get(doc.id, i)
        if d is None:
            missing.append(i)
        else:
            blocks[i] = d
    runs = []
    for i in missing:
        if runs and i == runs[-1][-1] + 1:
            runs[-1].append(i)
        else:
            runs.append([i])
    for run in runs:
        k = run[0]
        async for chunk in client.iter_download(doc, offset=run[0] * BLOCK, request_size=BLOCK, limit=len(run),
                                                file_size=doc.size):
            data = bytes(chunk)
            blocks[k] = data
            st.put(doc.id, k, data)
            NET["bytes"] += len(data)
            if counter is not None:
                counter["bytes"] = counter.get("bytes", 0) + len(data)
            k += 1
    return blocks


class RemoteFile:
    """Seekable read-only file over a Telegram document; fetches only the blocks that get read."""

    def __init__(self, client, loop, doc, name):
        self.client, self.loop, self.doc, self.name = client, loop, doc, name
        self.size, self.pos, self.blocks, self.fetched = doc.size, 0, {}, 0

    async def _fetch(self, i):
        ctr = {}
        data = (await get_blocks(self.client, self.doc, i, i, ctr)).get(i, b"")
        self.fetched += ctr.get("bytes", 0)  # network bytes only; cache hits are free
        return data

    def _block(self, i):
        if i not in self.blocks:
            if self.fetched >= PROBE_CAP:
                raise ProbeCapReached()
            self.blocks[i] = asyncio.run_coroutine_threadsafe(self._fetch(i), self.loop).result(timeout=60)
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


TAG_KEYS = {  # tag name in file -> our key
    "ALBUM": "album", "TALB": "album", b"\xa9alb": "album",
    "ALBUMARTIST": "albumartist", "ALBUM ARTIST": "albumartist", "TPE2": "albumartist", b"aART": "albumartist",
    "ARTIST": "artist", "TPE1": "artist", b"\xa9ART": "artist",
    "TRACKNUMBER": "track", "TRCK": "track", b"trkn": "track",
    "TRACKTOTAL": "tracks", "TOTALTRACKS": "tracks",
    "DISCNUMBER": "disc", "TPOS": "disc", b"disk": "disc",
    "GENRE": "genre", "TCON": "genre", b"\xa9gen": "genre",
    "DATE": "year", "YEAR": "year", "TDRC": "year", "TYER": "year", b"\xa9day": "year",
}


def clean_tags(t):
    """Normalise raw tag values: '3/12' -> track 3, tracks 12; '2019-05-01' -> year 2019."""
    out = {}
    for k, v in t.items():
        v = str(v).strip().strip("\x00").strip()
        if not v:
            continue
        if k in ("track", "disc"):
            num, _, total = v.partition("/")
            if num.strip().isdigit():
                out[k] = int(num)
            if k == "track" and total.strip().isdigit():
                out["tracks"] = int(total)
        elif k == "tracks":
            if v.isdigit():
                out[k] = int(v)
        elif k == "year":
            m = re.search(r"(19|20)\d\d", v)
            if m:
                out[k] = int(m[0])
        elif k == "genre":
            out[k] = re.sub(r"^\((\d+)\)", "", v).strip() or v
        else:
            out[k] = v[:200]
    return out


def vorbis_comments(data):
    """-> (normalised tags, every comment as written, encoder vendor string)"""
    t, everything, vendor = {}, {}, None
    try:
        n = int.from_bytes(data[:4], "little")
        vendor = data[4:4 + n].decode("utf-8", "replace")
        pos = 4 + n
        count = int.from_bytes(data[pos:pos + 4], "little")
        pos += 4
        for _ in range(min(count, 500)):
            ln = int.from_bytes(data[pos:pos + 4], "little")
            k, _, v = data[pos + 4:pos + 4 + ln].decode("utf-8", "replace").partition("=")
            pos += 4 + ln
            everything.setdefault(k.upper(), []).append(v[:20000])
            if TAG_KEYS.get(k.upper()):
                t.setdefault(TAG_KEYS[k.upper()], v)
    except Exception:
        pass
    return clean_tags(t), everything, vendor


def flac_picture(f, length):
    """PICTURE block header only (type, mime, description, size in pixels); the image itself is skipped."""
    start = f.tell()
    h = f.read(min(length, 4096))
    f.seek(start + length)
    try:
        be = lambda a: int.from_bytes(h[a:a + 4], "big")
        ml = be(4)
        mime = h[8:8 + ml].decode("latin-1")
        dl = be(8 + ml)
        desc = h[12 + ml:12 + ml + dl].decode("utf-8", "replace")
        o = 12 + ml + dl
        return {"type": be(0), "mime": mime, "desc": desc, "width": be(o), "height": be(o + 4), "depth": be(o + 8),
                "bytes": be(o + 16)}
    except Exception:
        return {"bytes": length}


def id3_text(data):
    enc, raw = data[0], data[1:]
    codec = {0: "latin-1", 1: "utf-16", 2: "utf-16-be", 3: "utf-8"}.get(enc, "latin-1")
    return raw.decode(codec, "replace").split("\x00")[0]


def id3_tags(f, tag_size, version):
    """Every ID3v2.3/2.4 frame: text, TXXX, comments, lyrics, URLs; pictures only by header (image never read).
    -> (normalised tags, all frames, covers)"""
    t, frames, covers, pos, end = {}, {}, [], 10, 10 + tag_size
    while pos + 10 <= end and version in (3, 4) and len(frames) < 300:
        f.seek(pos)
        h = f.read(10)
        fid = h[:4].decode("latin-1", "replace")
        if not fid.strip("\x00") or not fid.isalnum():
            break
        size = int.from_bytes(h[4:8], "big") if version == 3 else (
            (h[4] << 21) | (h[5] << 14) | (h[6] << 7) | h[7])
        if size <= 0:
            break
        if fid == "APIC":
            d = f.read(min(size, 256))
            rest = d[1:]  # skip text-encoding byte
            mime, _, rest = rest.partition(b"\x00")
            covers.append({"mime": mime.decode("latin-1"), "type": rest[0] if rest else None, "bytes": size})
        elif size < 256 * 1024:
            d = f.read(size)
            try:
                if fid == "TXXX":
                    desc, _, val = id3_text_raw(d)
                    frames.setdefault(f"TXXX:{desc}", []).append(val)
                elif fid in ("COMM", "USLT"):
                    desc, _, val = id3_text_raw(d[:1] + d[4:])  # skip 3-byte language
                    frames.setdefault(f"{fid}:{desc}", []).append(val[:20000])
                elif fid.startswith("T"):
                    frames.setdefault(fid, []).append(id3_text(d))
                    if fid in TAG_KEYS:
                        t.setdefault(TAG_KEYS[fid], id3_text(d))
                elif fid.startswith("W"):
                    frames.setdefault(fid, []).append(d.decode("latin-1", "replace").strip("\x00"))
                else:
                    frames.setdefault(fid, []).append({"bytes": size})
            except Exception:
                frames.setdefault(fid, []).append({"bytes": size})
        else:
            frames.setdefault(fid, []).append({"bytes": size})
        pos += 10 + size
    return clean_tags(t), frames, covers


def id3_text_raw(d):
    """encoding byte + 'description\0value' -> (description, sep, value)"""
    enc = d[0]
    codec = {0: "latin-1", 1: "utf-16", 2: "utf-16-be", 3: "utf-8"}.get(enc, "latin-1")
    sep = b"\x00\x00" if enc in (1, 2) else b"\x00"
    raw = d[1:]
    i = raw.find(sep)
    if enc in (1, 2):
        while i != -1 and i % 2:
            i = raw.find(sep, i + 1)
    desc, val = (raw[:i], raw[i + len(sep):]) if i != -1 else (b"", raw)
    return desc.decode(codec, "replace").strip("\x00"), "", val.decode(codec, "replace").strip("\x00")


def mp4_tags(f, moov):
    """-> (normalised tags, every ilst item incl. freeform ----:mean:name, covers by size only)"""
    udta = mp4_child(f, *moov, b"udta")
    meta = mp4_child(f, *udta, b"meta") if udta else None
    ilst = mp4_child(f, meta[0] + 4, meta[1], b"ilst") if meta else None  # meta is a full box
    t, everything, covers = {}, {}, []
    for typ, s, e in (mp4_atoms(f, *ilst) if ilst else ()):
        name = typ.decode("latin-1")
        if typ == b"covr":  # cover art: record, never read
            for _, ds, de in mp4_atoms(f, s, e):
                f.seek(ds)
                kind = int.from_bytes(f.read(4), "big") & 0xFFFFFF
                covers.append({"mime": {13: "image/jpeg", 14: "image/png"}.get(kind, str(kind)), "bytes": de - ds - 8})
            continue
        if e - s > 256 * 1024:
            everything[name] = {"bytes": e - s}
            continue
        if typ == b"----":  # freeform: mean + name + data (ISRC, MusicBrainz ids, ...)
            parts = {}
            for ct, cs, ce in mp4_atoms(f, s, e):
                f.seek(cs + (4 if ct in (b"mean", b"name") else 8))
                parts[ct] = f.read(ce - cs - (4 if ct in (b"mean", b"name") else 8))
            name = "----:" + parts.get(b"mean", b"").decode("utf-8", "replace") + ":" + parts.get(b"name", b"").decode("utf-8", "replace")
            everything[name] = parts.get(b"data", b"").decode("utf-8", "replace")
            continue
        data = mp4_child(f, s, e, b"data")
        if not data:
            continue
        f.seek(data[0])
        dtype = int.from_bytes(f.read(4), "big") & 0xFFFFFF
        f.seek(data[0] + 8)  # type + locale
        v = f.read(data[1] - data[0] - 8)
        if typ in (b"trkn", b"disk"):
            val = f"{int.from_bytes(v[2:4], 'big')}/{int.from_bytes(v[4:6], 'big')}"
        elif dtype == 1:
            val = v.decode("utf-8", "replace")
        elif dtype in (21, 22, 0) and len(v) <= 8:
            val = int.from_bytes(v, "big")
        else:
            val = v.hex()[:2000]
        everything[name] = val
        if TAG_KEYS.get(typ):
            t[TAG_KEYS[typ]] = val
    return clean_tags(t), everything, covers


def probe_dsf(f):
    h = f.read(28 + 52)
    if h[:4] != b"DSD " or h[28:32] != b"fmt ":
        return None
    le = lambda a, b: int.from_bytes(h[a:b], "little")
    ch, sr, bits, samples = le(52, 56), le(56, 60), le(60, 64), le(64, 72)
    length = samples / sr if sr else 0
    return {"dsd": True, "sample_rate": sr, "bits": bits, "channels": ch, "length": length,
            "kbps": round(sr * ch / 1000)}


def probe_flac(f):
    if f.read(4) != b"fLaC":
        return None
    info, pos, tags, everything, vendor, covers = None, 4, {}, {}, None, []
    for _ in range(128):
        hdr = f.read(4)
        if len(hdr) < 4:
            return None
        last, btype, length = hdr[0] & 0x80, hdr[0] & 0x7F, int.from_bytes(hdr[1:4], "big")
        if btype == 0:
            d = f.read(length)
            x = int.from_bytes(d[10:18], "big")
            sr, bits, total = x >> 44, ((x >> 36) & 0x1F) + 1, x & 0xFFFFFFFFF
            info = {"sample_rate": sr, "bits": bits, "length": total / sr if sr else 0, "channels": ((x >> 41) & 7) + 1,
                    "samples": total, "md5": d[18:34].hex(),  # MD5 of the decoded audio: same audio = same MD5
                    "min_block": int.from_bytes(d[0:2], "big"), "max_block": int.from_bytes(d[2:4], "big")}
        elif btype == 4 and length < 1024 * 1024:  # VORBIS_COMMENT: every tag + encoder vendor string
            tags, everything, vendor = vorbis_comments(f.read(length))
        elif btype == 6:  # PICTURE: header only
            covers.append(flac_picture(f, length))
        else:
            f.seek(length, 1)
        pos += 4 + length
        if last:
            break
    if info and info["length"]:
        info["kbps"] = round((f.size - pos) * 8 / info["length"] / 1000)  # audio only, no cover art
    if info:
        info.update({"tags": tags, "all_tags": everything, "encoder": vendor, "covers": covers, "metadata_bytes": pos})
    return info


def probe_mp3(f):
    h = f.read(10)
    offset, tags, frames, covers = 0, {}, {}, []
    if h[:3] == b"ID3":
        size = (h[6] << 21) | (h[7] << 14) | (h[8] << 7) | h[9]
        if not h[5] & 0x40:
            tags, frames, covers = id3_tags(f, size, h[3])
        offset = 10 + size + (10 if h[5] & 0x10 else 0)
    info = MPEGInfo(f, offset)
    return {"kbps": round(info.bitrate / 1000), "sample_rate": info.sample_rate, "length": info.length,
            "vbr": info.bitrate_mode in (BitrateMode.VBR, BitrateMode.ABR), "channels": info.channels, "tags": tags,
            "all_tags": frames, "covers": covers, "id3_version": f"2.{h[3]}" if h[:3] == b"ID3" else None,
            "encoder": getattr(info, "encoder_info", "") or None, "encoder_settings": getattr(info, "encoder_settings", "") or None,
            "mpeg": f"{info.version}/layer{info.layer}", "mode": getattr(info, "mode", None), "sketchy": info.sketchy}


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
        tags, everything, covers = mp4_tags(f, top[b"moov"])
        return {"kbps": kbps, "sample_rate": sr, "bits": bits, "length": length, "alac": fmt == b"alac",
                "channels": channels, "tags": tags, "all_tags": everything, "covers": covers,
                "codec": fmt.decode(), "brands": [t.decode("latin-1") for t in top]}
    return None


def probe_wav(f):
    if f.read(4) != b"RIFF" or (f.read(4), f.read(4))[1] != b"WAVE":
        return None
    pos, fmt, info_tags, chunks = 12, None, {}, []
    for _ in range(64):
        f.seek(pos)
        h = f.read(8)
        if len(h) < 8:
            return None
        cid, size = h[:4], int.from_bytes(h[4:8], "little")
        chunks.append(cid.decode("latin-1", "replace"))
        if cid == b"LIST" and size < 1024 * 1024 and f.read(4) == b"INFO":  # INAM, IART, IPRD, IGNR, ICRD, ISFT...
            d = f.read(size - 4)
            i = 0
            while i + 8 <= len(d):
                k, ln = d[i:i + 4].decode("latin-1"), int.from_bytes(d[i + 4:i + 8], "little")
                info_tags[k] = d[i + 8:i + 8 + ln].decode("utf-8", "replace").strip("\x00")
                i += 8 + ln + (ln & 1)
        elif cid == b"bext" and size < 64 * 1024:  # Broadcast WAV: description, originator, date, coding history
            d = f.read(size)
            info_tags["bext"] = {"description": d[:256].decode("latin-1", "replace").strip("\x00"),
                                 "originator": d[256:288].decode("latin-1", "replace").strip("\x00"),
                                 "date": d[320:330].decode("latin-1", "replace"),
                                 "history": d[602:].decode("latin-1", "replace").strip("\x00")[:4000]}
        if cid == b"fmt ":
            d = f.read(16)
            fmt = {"sample_rate": int.from_bytes(d[4:8], "little"), "byte_rate": int.from_bytes(d[8:12], "little"),
                   "bits": int.from_bytes(d[14:16], "little"), "channels": int.from_bytes(d[2:4], "little")}
        elif cid == b"data" and fmt and fmt["byte_rate"]:
            length = size / fmt["byte_rate"]
            wav_keys = {"IPRD": "album", "IART": "artist", "IGNR": "genre", "ICRD": "year", "IPRT": "track"}
            return {"kbps": round(fmt["byte_rate"] * 8 / 1000), "sample_rate": fmt["sample_rate"],
                    "bits": fmt["bits"], "length": length, "channels": fmt["channels"], "all_tags": info_tags,
                    "chunks": chunks, "tags": clean_tags({v: info_tags[k] for k, v in wav_keys.items() if k in info_tags})}
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
    info = _probe(f, ext)
    if info:
        info["full"] = True  # every header field kept (PROBE_VERSION 4+)
    return info


def _probe(f, ext):
    f.seek(0)
    if ext == "flac":
        return probe_flac(f)
    if ext == "mp3":
        return probe_mp3(f)
    head = f.read(12)
    f.seek(0)
    if head[:4] == b"DSD ":
        return probe_dsf(f)
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
-- every object Telegram returns, in its exact binary (TL) form, zlib-compressed; a new row only when the bytes
-- change, so edits, view counts and reactions build up a history. Decode with `hifigram.py inspect`.
CREATE TABLE IF NOT EXISTS raw(kind TEXT, key TEXT, hash TEXT, first_seen INTEGER, last_seen INTEGER, data BLOB,
  PRIMARY KEY(kind, key, hash));
-- convenience columns for every post (audio or not), refreshed whenever the post is seen
CREATE TABLE IF NOT EXISTS post_meta(chat_id INTEGER, msg_id INTEGER, date INTEGER, media TEXT, views INTEGER,
  forwards INTEGER, replies INTEGER, reactions TEXT, fwd_chat INTEGER, fwd_msg INTEGER, fwd_date INTEGER,
  fwd_name TEXT, post_author TEXT, edit_date INTEGER, noforwards INTEGER, grouped_id INTEGER, text TEXT,
  PRIMARY KEY(chat_id, msg_id));
CREATE INDEX IF NOT EXISTS post_meta_date ON post_meta(chat_id, date);
CREATE TABLE IF NOT EXISTS views_history(chat_id INTEGER, msg_id INTEGER, at INTEGER, views INTEGER, forwards INTEGER,
  replies INTEGER, PRIMARY KEY(chat_id, msg_id, at));
CREATE TABLE IF NOT EXISTS events(at INTEGER, kind TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS languages(ref TEXT PRIMARY KEY, lang TEXT, conf REAL, why TEXT, version INTEGER);
"""
PROBE_VERSION = 4  # bump when probe code improves: failed probes from older versions get retried
FILTERS = {"all": None}  # chat scans take every message (text, photos, audio, service...), not just audio
SEARCH_FILTERS = {"music": InputMessagesFilterMusic, "files": InputMessagesFilterDocument}
FULLCHAT_TTL = 86400  # channel details (description, subscriber count...) re-read at most daily
VIEWS_WINDOW = 30 * 86400  # posts younger than this get a views/forwards snapshot on each daily refresh


def open_db():
    db = sqlite3.connect(DB, timeout=60)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")  # scheduled sync + manual searches can share the index
    except sqlite3.OperationalError:
        pass
    db.executescript(SCHEMA)
    for col in ("grouped_id INTEGER", "deleted_at INTEGER"):  # columns added after the first release
        try:
            db.execute(f"ALTER TABLE files ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    return db


def store_raw(db, kind, key, obj):
    """Keep the exact bytes of a Telegram object. Same bytes as before -> only last_seen moves."""
    try:
        data = bytes(obj)
    except Exception:
        return
    h, now = hashlib.sha1(data).hexdigest()[:20], int(time.time())
    cur = db.execute("INSERT OR IGNORE INTO raw VALUES(?,?,?,?,?,?)", (kind, str(key), h, now, now, zlib.compress(data, 6)))
    if not cur.rowcount:
        db.execute("UPDATE raw SET last_seen=? WHERE kind=? AND key=? AND hash=?", (now, kind, str(key), h))


def log_event(db, kind, detail):
    db.execute("INSERT INTO events VALUES(?,?,?)", (int(time.time()), kind, str(detail)[:2000]))


def post_facts(msg):
    from telethon import utils
    fwd = msg.fwd_from
    fwd_chat = None
    if fwd and fwd.from_id:
        try:
            fwd_chat = utils.get_peer_id(fwd.from_id)
        except Exception:
            pass
    reactions = None
    if getattr(msg, "reactions", None) and msg.reactions.results:
        reactions = json.dumps([[getattr(r.reaction, "emoticon", None) or getattr(r.reaction, "document_id", None)
                                 or type(r.reaction).__name__, r.count] for r in msg.reactions.results])
    media = type(msg.media).__name__.replace("MessageMedia", "") if msg.media else ("text" if msg.message else None)
    if type(msg).__name__ == "MessageService" or getattr(msg, "action", None):
        media = "service:" + type(msg.action).__name__.replace("MessageAction", "")
    return (msg.chat_id, msg.id, int(msg.date.timestamp()) if msg.date else None, media, msg.views, msg.forwards,
            msg.replies.replies if getattr(msg, "replies", None) else None, reactions, fwd_chat,
            getattr(fwd, "channel_post", None) if fwd else None, int(fwd.date.timestamp()) if fwd and fwd.date else None,
            getattr(fwd, "from_name", None) if fwd else None, msg.post_author,
            int(msg.edit_date.timestamp()) if msg.edit_date else None, int(bool(msg.noforwards)), msg.grouped_id,
            (msg.message or "")[:4000] or None)


MIME_EXT = {"flac": "flac", "x-flac": "flac", "mpeg": "mp3", "mp3": "mp3", "mp4": "m4a", "m4a": "m4a", "x-m4a": "m4a",
            "aac": "aac", "wav": "wav", "x-wav": "wav", "vnd.wave": "wav", "ogg": "ogg", "opus": "opus",
            "x-dsf": "dsf", "dsf": "dsf", "x-dff": "dff", "x-ape": "ape", "x-wavpack": "wv", "aiff": "aiff", "x-aiff": "aiff"}


def clean_ext(ext, mime):
    """File-name extension if it looks like one, else derived from the MIME type ("Song (From X.)" has no ext)."""
    ext = (ext or "").lower().lstrip(".")
    if ext in AUDIO_EXTS or ext in MIME_EXT.values():
        return ext
    return MIME_EXT.get((mime or "").split("/")[-1].lower(), ext if re.fullmatch(r"[a-z0-9]{2,4}", ext) else "")


def ext_of(msg):
    name = msg.file.name or ""
    ext = name.rsplit(".", 1)[-1] if "." in name else (msg.file.ext or "")
    return clean_ext(ext, msg.file.mime_type)


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
    store_raw(db, "chat", chat_id, chat)
    title = getattr(chat, "title", None) or " ".join(filter(None, [getattr(chat, "first_name", None),
                                                                    getattr(chat, "last_name", None)])) or "?"
    db.execute("INSERT INTO chats(chat_id, title, username, kind) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET "
               "title=excluded.title, username=excluded.username, kind=excluded.kind",
               (chat_id, title, getattr(chat, "username", None), chat_kind(chat)))


def remember(db, msgs):
    """Store every message we see, from any command: exact bytes + facts for all, a files row for audio."""
    n = 0
    seen_chats = set()
    for msg in msgs:
        if msg is None:
            continue
        store_raw(db, "message", f"{msg.chat_id}:{msg.id}", msg)
        db.execute("INSERT OR REPLACE INTO post_meta VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", post_facts(msg))
        if msg.chat and msg.chat_id not in seen_chats:
            seen_chats.add(msg.chat_id)
            store_raw(db, "chat", msg.chat_id, msg.chat)
        if not is_audio(msg):
            continue
        doc = msg.document
        audio = next((a for a in doc.attributes if isinstance(a, DocumentAttributeAudio)), None)
        if msg.chat:
            remember_chat(db, msg.chat_id, msg.chat)
        db.execute("INSERT OR REPLACE INTO files(chat_id, msg_id, doc_id, size, duration, mime, ext, title, performer, "
                   "file_name, caption, date, grouped_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            msg.chat_id, msg.id, doc.id, doc.size, (audio.duration if audio else 0) or 0, doc.mime_type or "",
            ext_of(msg), (audio.title if audio else None) or "", (audio.performer if audio else None) or "",
            msg.file.name or "", msg.message or "", int(msg.date.timestamp()) if msg.date else 0, msg.grouped_id))
        n += 1
    return n


ROW_SQL = ("SELECT f.*, c.title AS chat_title, c.username, c.kind FROM files f "
           "LEFT JOIN chats c ON c.chat_id = f.chat_id WHERE f.deleted_at IS NULL")


def item_from_row(r):
    link = None
    if r["username"]:
        link = f"https://t.me/{r['username']}/{r['msg_id']}"
    elif r["kind"] in ("channel", "group") and str(r["chat_id"]).startswith("-100"):
        link = f"https://t.me/c/{str(r['chat_id'])[4:]}/{r['msg_id']}"
    return {
        "ref": f"{r['chat_id']}:{r['msg_id']}", "doc_id": r["doc_id"], "chat": r["chat_title"] or "?", "link": link,
        "title": r["title"], "performer": r["performer"], "file_name": r["file_name"], "caption": r["caption"][:300],
        "duration": r["duration"], "size": r["size"], "mime": r["mime"], "ext": clean_ext(r["ext"], r["mime"]), "probed": False,
        "msg_id": r["msg_id"], "chat_id": r["chat_id"], "grouped_id": r["grouped_id"],
    }


def load_items(db, chat=None):
    sql, params = ROW_SQL, ()
    if chat:
        sql += " AND c.title LIKE ?"
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
        tag = f"{label} {name[:30]}"
        n = seen = 0
        with err.status(f"{tag}…") as status:
            if not st["done"]:
                batch = []
                async for m in client.iter_messages(entity, filter=flt, offset_id=st["oldest"]):
                    batch.append(m)
                    st["oldest"] = m.id
                    if len(batch) >= 200:
                        n += remember(db, batch)
                        seen += len(batch)
                        batch = []
                        save_scan(db, st)
                        status.update(f"{tag}: {seen} posts, {n} audio…")
                n += remember(db, batch)
                seen += len(batch)
                st["done"] = 1
            else:
                msgs = [m async for m in client.iter_messages(entity, filter=flt, min_id=st["newest"])]
                n += remember(db, msgs)
                seen += len(msgs)
                st["newest"] = max([st["newest"], top] + [m.id for m in msgs])
            save_scan(db, st)
        fetched += n
        if n or seen:
            err.print(f"{tag}: +{seen} posts, +{n} audio")
    db.execute("UPDATE chats SET top_id=? WHERE chat_id=?", (top, chat_id))
    db.commit()
    return fetched


async def chat_details(client, db, entity, chat_id):
    """Full channel/group info (description, subscribers, linked chat, pinned...), stored raw at most daily."""
    from telethon.tl.functions.channels import GetFullChannelRequest
    from telethon.tl.functions.messages import GetFullChatRequest
    from telethon.tl.types import Channel, Chat
    key = f"fullchat:{chat_id}"
    last = db.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    if last and time.time() - int(last[0]) < FULLCHAT_TTL:
        return
    try:
        if isinstance(entity, Channel):
            full = await client(GetFullChannelRequest(entity))
        elif isinstance(entity, Chat):
            full = await client(GetFullChatRequest(entity.id))
        else:
            return
        store_raw(db, "fullchat", chat_id, full)
    except TRANSIENT:
        return
    except Exception as e:
        log_event(db, "fullchat-error", f"{chat_id}: {e}")
    db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(int(time.time()))))


async def views_snapshot(client, db, entity, chat_id):
    """Views/forwards/replies of recent channel posts, recorded over time (daily)."""
    from telethon.tl.functions.messages import GetMessagesViewsRequest
    from telethon.tl.types import Channel
    if not (isinstance(entity, Channel) and entity.broadcast):
        return
    key = f"views:{chat_id}"
    last = db.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    now = int(time.time())
    if last and now - int(last[0]) < 86400:
        return
    ids = [r[0] for r in db.execute("SELECT msg_id FROM post_meta WHERE chat_id=? AND date>? "
                                    "AND (media IS NULL OR media NOT LIKE 'service:%')", (chat_id, now - VIEWS_WINDOW))]
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            res = await client(GetMessagesViewsRequest(peer=entity, id=chunk, increment=False))
        except TRANSIENT:
            return
        except Exception as e:
            log_event(db, "views-error", f"{chat_id}: {e}")
            return
        store_raw(db, "views", f"{chat_id}:{chunk[0]}-{chunk[-1]}", res)
        for mid, v in zip(chunk, res.views):
            replies = v.replies.replies if v.replies else None
            db.execute("INSERT OR IGNORE INTO views_history VALUES(?,?,?,?,?,?)",
                       (chat_id, mid, now, v.views, v.forwards, replies))
            db.execute("UPDATE post_meta SET views=?, forwards=?, replies=? WHERE chat_id=? AND msg_id=?",
                       (v.views, v.forwards, replies, chat_id, mid))
    db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(now)))
    db.commit()


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
        await chat_details(client, db, d.entity, d.id)
        if known.get(d.id) == top and done_all(d.id):
            skipped += 1
        else:
            fetched += await scan_chat(client, db, d.entity, d.id, d.name, top, scans, f"[{i}/{len(dialogs)}]")
        await views_snapshot(client, db, d.entity, d.id)
    joined = {d.id for d in dialogs}
    extra = [r for r in db.execute("SELECT chat_id, username FROM extra_chats") if r["chat_id"] not in joined]
    for r in extra:  # public channels indexed without joining: 1 call to see the newest post id
        entity = await client.get_entity(r["username"])
        latest = await client.get_messages(entity, limit=1)
        top = latest[0].id if latest else 0
        remember_chat(db, r["chat_id"], entity)
        await chat_details(client, db, entity, r["chat_id"])
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
    item["tags"] = (info or {}).get("tags") or {}
    item["all_tags"] = (info or {}).get("all_tags") or {}
    if lossless and (info or {}).get("dsd"):  # DSD64 = 2.8224 MHz 1-bit; ranked like the PCM rate it resolves
        sr = info["sample_rate"]
        item["label"] = f"DSD{round(sr / 44100)}"
        item["score"] = 10000 + 24 * 100 + sr / 32 / 1000
        item["tier"] = "hires"
    elif lossless:
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


async def ensure_probed(client, db, items, need=()):
    """Header-probe only documents never probed before (or failed with older probe code); cache every result.
    need: info keys (e.g. "channels", "tags") that older probes may lack; only those headers are re-read."""
    if client is None:  # --offline: cached results only
        return apply_probes(db, items)
    have = {r["doc_id"]: r for r in db.execute("SELECT doc_id, info, version FROM probes")}
    vers = {r["doc_id"]: r["result"] for r in db.execute("SELECT doc_id, result FROM verify")}
    todo = {}
    for it in items:
        row = have.get(it["doc_id"])
        if row and need and row["info"]:
            info = json.loads(row["info"])
            missing = [k for k in need if k not in info and not (k == "channels" and
                                                                  json.loads(vers.get(it["doc_id"]) or "{}").get(k))]
            if missing:
                todo.setdefault(it["doc_id"], it)
            continue
        if row and (row["info"] or row["version"] >= PROBE_VERSION):
            continue
        todo.setdefault(it["doc_id"], it)
    if todo:
        msgs = await fetch_messages(client, db, [it["ref"] for it in todo.values()])
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
            status.update(f"Checking real quality {done}/{len(todo)} (new files only)…")

        with err.status(f"Checking real quality 0/{len(todo)} (new files only)…") as status:
            await asyncio.gather(*(one(it) for it in todo.values()))
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


async def fetch_range(client, doc, offset, length, counter=None):
    first, last = offset // BLOCK, (offset + length - 1) // BLOCK
    blocks = await get_blocks(client, doc, first, last, counter)
    data = b"".join(blocks.get(i, b"") for i in range(first, last + 1))
    return data[offset - first * BLOCK:offset - first * BLOCK + length]


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
    ctr = {}
    rf = RemoteFile(client, loop, msg.document, it["file_name"])
    lay = await asyncio.to_thread(audio_layout, rf, it["ext"])
    if not lay or not info or not info.get("sample_rate"):
        return None
    start, end, extra = lay
    length = sample_bytes(info, start, end)
    dur = info.get("length") or it["duration"]
    secs = min(3.5, 0.8 * length / max(end - start, 1) * dur)  # decode only audio we actually fetched
    offs = [start + int((end - start - length) * p) for p in SAMPLE_POINTS]
    chunks = [await fetch_range(client, msg.document, o, length, ctr) for o in offs]
    rf.seek(0)
    samples = []
    with tempfile.TemporaryDirectory() as tmp:
        if it["ext"] == "flac":  # metadata + slice; the decoder resyncs on the next frame
            head = bytearray(await asyncio.to_thread(rf.read, 42))  # "fLaC" + STREAMINFO; pictures etc. skipped
            head[4] |= 0x80  # mark STREAMINFO as the last metadata block
            head = bytes(head)
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
                    fh.write(await fetch_range(client, msg.document, off, n, ctr))
                for o, chunk in zip(offs, chunks):
                    fh.seek(o)
                    fh.write(chunk)
                fh.truncate(msg.document.size)
            for o in offs:
                samples.append(await decode(path, dur * ((o - start) + length * 0.1) / (end - start), secs))
    res = await asyncio.to_thread(spectrum_verdict, samples, info["sample_rate"], info.get("bits") or 16)
    if res:
        res["fetched_mb"] = round((ctr.get("bytes", 0) + rf.fetched) / 1e6, 2)
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
        if it["lossless"] and it["ext"] not in ("dsf", "dff") and it["doc_id"] not in done:  # DSD: no PCM spectrum test
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
                m = process.extractOne(w, toks, scorer=fuzz.ratio, score_cutoff=min_score) if len(w) > 3 and min_score <= 100 else None
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
    if it.get("name_prefix"):  # album: "03 - Title.flac" ("(From Movie)" is redundant inside the album folder)
        base = it["title"] or re.sub(r"^\s*\d{1,3}\s*[-._)]\s*", "", Path(it["file_name"]).stem) or it.get("norm") or "audio"
        base = FROM_RE.sub("", base).strip() or base
        return f"{safe_name(it['name_prefix'] + unicodedata.normalize('NFKC', base))}.{it['ext'] or 'bin'}"
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
                else:  # never delete knowledge: mark it
                    db.execute("UPDATE files SET deleted_at=? WHERE chat_id=? AND msg_id=? AND deleted_at IS NULL",
                               (int(time.time()), chat, mid))
                    log_event(db, "deleted", f"{chat}:{mid}")
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


# ---------- categories / filters ----------

LANGS = ("tamil", "telugu", "hindi", "malayalam", "kannada", "bengali", "punjabi", "marathi", "english", "japanese",
         "korean", "chinese", "russian", "arabic", "spanish", "french", "german")
SCRIPTS = (("tamil", 0x0B80, 0x0BFF), ("telugu", 0x0C00, 0x0C7F), ("kannada", 0x0C80, 0x0CFF),
           ("malayalam", 0x0D00, 0x0D7F), ("hindi", 0x0900, 0x097F), ("bengali", 0x0980, 0x09FF),
           ("japanese", 0x3040, 0x30FF), ("chinese", 0x4E00, 0x9FFF), ("korean", 0xAC00, 0xD7AF),
           ("russian", 0x0400, 0x04FF), ("arabic", 0x0600, 0x06FF))
VERSIONS = ("remix", "live", "instrumental", "karaoke", "acoustic", "cover", "unplugged", "lofi", "slowed", "reprise",
            "ost", "bgm", "theme", "atmos", "remaster")


# ---------- language detection ----------
# Signals, strongest first; each adds weight to a language, with the reason kept:
#   language tag in the file (ID3 TLAN, Vorbis LANGUAGE)       1.0
#   native script in title/artist (மகளே, తెలుగు, हिंदी...)      0.95
#   album/movie known to be in a language (propagated)          0.6
#   artist: single-language seed list (0.7) / multi-language (0.2) / learned from confident tracks (0.5)
#   romanised words typical of a language (kaadhal, nenjam / prema, nuvvu / dil, pyaar / the, love)   0.25 each, max 0.6
#   channel name ("Tamil Lossless...")                           0.3 — a hint, never enough alone
# Results (language, confidence, reasons) are cached per post in the `languages` table.

LANG_VERSION = 1
LANG_CODES = {"ta": "tamil", "tam": "tamil", "tamil": "tamil", "te": "telugu", "tel": "telugu", "telugu": "telugu",
              "hi": "hindi", "hin": "hindi", "hindi": "hindi", "ml": "malayalam", "mal": "malayalam",
              "malayalam": "malayalam", "kn": "kannada", "kan": "kannada", "kannada": "kannada", "en": "english",
              "eng": "english", "english": "english", "ja": "japanese", "jpn": "japanese", "ko": "korean",
              "kor": "korean", "ru": "russian", "rus": "russian", "zh": "chinese", "chi": "chinese", "zho": "chinese"}
ARTIST_SEEDS = {  # (languages, strong). Strong = works (almost) only in that language.
    "tamil": (["ilaiyaraaja", "ilayaraja", "ilaiyaraja", "isaignani", "yuvan shankar raja", "harris jayaraj",
               "g v prakash", "gv prakash", "d imman", "santhosh narayanan", "sean roldan", "hiphop tamizha",
               "deva", "bharathwaj", "sai abhyankkar", "t m soundararajan", "tms", "m s viswanathan", "msv",
               "vairamuthu", "kannadasan", "na muthukumar", "thamarai", "madhan karky", "vivek", "vijay antony",
               "ghibran", "govind vasantha", "sam c s", "justin prabhakaran", "dhina", "sirpy", "gangai amaran",
               "karthik raja", "vidyasagar", "imman", "anthony daasan", "gana", "arivu", "thamizh aadhavan",
               "mohan kannan", "srinivas", "hariharan", "unni krishnan", "unnikrishnan", "malaysia vasudevan",
               "p unnikrishnan", "sirkazhi govindarajan", "jikki", "l r eswari", "vani jairam", "charulatha mani",
               "pradeep kumar", "andrea jeremiah", "dhanush", "sivakarthikeyan", "silambarasan", "str",
               "kamal haasan", "rajinikanth", "anirudh"], True),
    "telugu": (["devi sri prasad", "dsp", "thaman", "s thaman", "mani sharma", "m m keeravani", "keeravani",
                "mickey j meyer", "anup rubens", "sirivennela", "chandrabose", "ramajogayya sastry", "sid sriram telugu",
                "koti", "chakri", "ghantasala", "s p charan"], True),
    "hindi": (["arijit singh", "pritam", "vishal shekhar", "amit trivedi", "lata mangeshkar", "kishore kumar",
               "mohammed rafi", "asha bhosle", "jubin nautiyal", "atif aslam", "neha kakkar", "shankar ehsaan loy",
               "r d burman", "kumar sanu", "udit narayan", "alka yagnik", "javed akhtar", "gulzar", "sachet parampara"], True),
    "malayalam": (["sushin shyam", "bijibal", "m jayachandran", "vineeth sreenivasan", "shaan rahman",
                   "rex vijayan", "k s harisankar"], True),
    "kannada": (["arjun janya", "v harikrishna", "ajaneesh loknath", "rajesh krishnan", "hamsalekha"], True),
    "multi": (["a r rahman", "ar rahman", "s p balasubrahmanyam", "s p balasubramaniam", "spb", "s p b",
               "k s chithra", "ks chithra", "chithra", "k j yesudas", "yesudas", "p susheela", "s janaki",
               "sid sriram", "shreya ghoshal", "sonu nigam", "shankar mahadevan", "karthik", "haricharan",
               "harini", "chinmayi", "swetha mohan", "shweta mohan", "vijay yesudas", "benny dayal", "naresh iyer",
               "sunidhi chauhan", "kailash kher", "gopi sundar", "santhosh", "mano", "sujatha", "anuradha sriram"], False),
}
LEXICON = {
    "tamil": {"kaadhal", "kadhal", "kaathal", "nenjam", "nenje", "penne", "kanne", "kanmani", "uyire", "uyir",
              "azhagu", "azhagi", "azhage", "vaa", "naan", "nee", "unnai", "ennai", "mazhai", "nila", "nilave",
              "thendral", "poove", "poo", "chellam", "chella", "magale", "magal", "amma", "thalaivaa", "kuthu",
              "paattu", "pattu", "vaanam", "megam", "idhayam", "mudhal", "kannil", "kannukulla", "oru", "ennodu",
              "unakku", "enakku", "unnodu", "kaathirunthen", "raasa", "raasathi", "machan", "machi", "vaadi",
              "podi", "aaha", "adiye", "yaaro", "yaar", "ponnu", "thaane", "kanavu", "kangal", "vizhi", "manam",
              "nenjil", "kaadhalan", "kaadhali", "thangame", "thanga", "senthamizh", "tamizh", "thamizh", "vaazhkai",
              "anbe", "anbu", "malare", "malar", "thaai", "thaaye", "kanna", "kannamma", "rasa", "rasathi",
              "solla", "sollu", "paadum", "paadu", "aattam", "kaatre", "kaatru", "santhana", "mullai", "raasaave",
              "vaanambaadi", "azhagiya", "nathiyena", "pathikichu", "naan", "thaan", "unna", "ennithan"},
    "telugu": {"prema", "nuvvu", "nenu", "ninne", "pilla", "chinni", "gundello", "manasu", "naa", "nee",
               "ammayi", "abbayi", "andam", "andala", "kalalo", "vela", "cheli", "priya", "raave", "ra", "emo",
               "yemaindi", "oohalu", "ninnu", "naalo", "neelo", "jagame", "kotha", "bangaram", "manasaa"},
    "hindi": {"dil", "pyaar", "pyar", "ishq", "mera", "meri", "tera", "teri", "hai", "kya", "tum", "tumhe",
              "jaana", "mohabbat", "sajna", "yaar", "zindagi", "jaan", "aankhen", "sapna", "humko", "tujhe",
              "mujhe", "kahin", "raat", "chand", "bahon", "bin", "hawa", "dilbar", "mahiya", "saiyaan", "ke", "se"},
    "malayalam": {"ponne", "ente", "ninte", "pranayam", "kanmani", "mazha", "ormakal", "sundari", "kaattu"},
    "english": {"the", "love", "you", "me", "my", "of", "in", "and", "night", "heart", "baby", "your", "with",
                "for", "this", "what", "when", "girl", "time", "dance", "life", "light", "dream", "never",
                "forever", "remaster", "remastered", "feat", "live", "version", "edit", "mix"},
}


def _seed_index():
    idx = {}
    for lang, (names, strong) in ARTIST_SEEDS.items():
        for n in names:
            idx[n.replace(" ", "")] = (lang, strong)
    return idx


SEEDS = _seed_index()
SEED_PREFIX = {}
for _k, _v in SEEDS.items():
    if len(_k) > 5:
        SEED_PREFIX.setdefault(_k[:6], []).append((_k, _v))


def artist_parts(it):
    raw = " , ".join(filter(None, [it["performer"], (it.get("tags") or {}).get("albumartist"),
                                   (it.get("tags") or {}).get("artist")]))
    parts = re.split(r"\s*(?:,|&|;|/|\bfeat\.?|\bft\.?|\band\b|\bwith\b|\bx\b)\s*", unicodedata.normalize("NFKC", raw).lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts if p.strip()}


def lang_signals(it, album_lang=None, learned=None):
    """-> {language: weight}, [reasons]"""
    score, why = {}, []

    def add(lang, w, reason):
        score[lang] = score.get(lang, 0) + w
        why.append(f"{reason}→{lang}")

    info_tags = it.get("all_tags") or {}
    for key in ("TLAN", "LANGUAGE", "----:com.apple.iTunes:LANGUAGE"):
        val = info_tags.get(key)
        val = val[0] if isinstance(val, list) and val else val
        if isinstance(val, str) and LANG_CODES.get(val.strip().lower()[:8]):
            add(LANG_CODES[val.strip().lower()[:8]], 1.0, f"tag:{val}")
    counts = {}
    for ch in unicodedata.normalize("NFKC", f"{it['title']} {it['performer']} {it['file_name']}"):
        o = ord(ch)
        for name, a, b in SCRIPTS:
            if a <= o <= b:
                counts[name] = counts.get(name, 0) + 1
    if counts:
        add(max(counts, key=counts.get), 0.95, "script")
    if album_lang:
        add(album_lang, 0.6, "album")
    for part in artist_parts(it):
        seed = SEEDS.get(part) or next((v for k, v in SEED_PREFIX.get(part[:6], ()) if part.startswith(k)), None)
        if seed:
            lang, strong = seed
            if lang == "multi":
                for l in ("tamil", "telugu", "hindi", "malayalam"):
                    score[l] = score.get(l, 0) + 0.05  # tie-break nudge only
                why.append(f"artist:{part}(multi)")
            else:
                add(lang, 0.7 if strong else 0.2, f"artist:{part}")
        elif learned and part in learned:
            add(learned[part], 0.5, f"learned-artist:{part}")
    words = set(norm_text(f"{it['title']} {Path(it['file_name']).stem}").split())
    for lang, lex in LEXICON.items():
        hits = sorted(words & lex)
        if hits:
            add(lang, min(0.6, 0.25 * len(hits)), f"words:{','.join(hits[:3])}")
    chat = it["chat"].lower()
    for lang in LANGS:
        if lang in chat:
            add(lang, 0.3, "channel")
    return score, why


def decide(score):
    if not score:
        return "", 0.0
    lang = max(score, key=score.get)
    total = sum(score.values())
    return lang, round(min(1.0, score[lang]) * (score[lang] / total), 2)


def compute_languages(db, items):
    """Two passes: (1) per-post signals; albums and artists learn their language from confident posts;
    (2) propagate album and learned-artist languages back to every post. Cached in `languages`."""
    from collections import Counter, defaultdict
    cached = {r["ref"]: r for r in db.execute("SELECT ref, lang, conf, why FROM languages WHERE version=?", (LANG_VERSION,))}
    todo = [it for it in items if it["ref"] not in cached]
    if todo:
        first = {}
        album_votes, artist_votes = defaultdict(Counter), defaultdict(Counter)
        for it in todo:
            score, why = lang_signals(it)
            lang, conf = decide(score)
            first[it["ref"]] = (lang, conf)
            if conf >= 0.6 and lang:
                hint = album_hint(it)[0]
                if hint:
                    album_votes[album_norm(hint)][lang] += 1
                for part in artist_parts(it):
                    artist_votes[part][lang] += 1
        album_lang = {k: c.most_common(1)[0][0] for k, c in album_votes.items()
                      if c.most_common(1)[0][1] >= 2 and c.most_common(1)[0][1] / sum(c.values()) >= 0.75}
        learned = {a: c.most_common(1)[0][0] for a, c in artist_votes.items()
                   if c.most_common(1)[0][1] >= 5 and c.most_common(1)[0][1] / sum(c.values()) >= 0.8}
        rows = []
        for it in todo:
            hint = album_hint(it)[0]
            score, why = lang_signals(it, album_lang.get(album_norm(hint)) if hint else None, learned)
            lang, conf = decide(score)
            rows.append((it["ref"], lang, conf, "; ".join(why)[:500], LANG_VERSION))
            cached[it["ref"]] = {"lang": lang, "conf": conf, "why": "; ".join(why)[:500]}
        db.executemany("INSERT OR REPLACE INTO languages VALUES(?,?,?,?,?)", rows)
        db.commit()
    for it in items:
        c = cached[it["ref"]]
        it["lang"], it["lang_conf"], it["lang_why"] = c["lang"], c["conf"], c["why"]
    return items


def lang_of(it):
    return it.get("lang") or ""


def versions_of(it):
    hay = norm_text(" ".join([it["title"], it["file_name"]]))
    return [v for v in VERSIONS if re.search(rf"\b{v}\b", hay)]


def length_bucket(d):
    return "short" if d < 60 else "song" if d < 360 else "long" if d < 900 else "mix"


def needs(args):
    keys = ("channels",) if getattr(args, "stereo", False) else ()
    if getattr(args, "genre", None) or getattr(args, "decade", None):
        keys += ("tags",)
    return keys


def filter_items(items, args):
    """--format/--lang/--version/--length work on the index; --genre/--decade need file tags (read on probe)."""
    if getattr(args, "lang", None) and items and "lang" not in items[0]:
        compute_languages(open_db(), items)
    if getattr(args, "format", None):
        fmts = {f.strip().lower() for f in args.format.split(",")}
        items = [it for it in items if it["ext"] in fmts or (it["ext"] == "m4a" and "alac" in fmts and it.get("label", "").startswith("ALAC"))]
    if getattr(args, "lang", None):
        floor = 0.2 if getattr(args, "loose", False) else 0.45
        items = [it for it in items if lang_of(it) == args.lang.lower() and it.get("lang_conf", 0) >= floor]
    if getattr(args, "version", None):
        want = {v.strip().lower() for v in args.version.split(",")}
        items = [it for it in items if want & set(versions_of(it))]
    if getattr(args, "no_version", None):
        drop = {v.strip().lower() for v in args.no_version.split(",")}
        items = [it for it in items if not drop & set(versions_of(it))]
    if getattr(args, "length", None):
        items = [it for it in items if length_bucket(it["duration"]) == args.length]
    return items


def filter_tags(items, args):
    if getattr(args, "genre", None):
        g = args.genre.lower()
        items = [it for it in items if g in (it.get("tags") or {}).get("genre", "").lower()]
    if getattr(args, "decade", None):
        d = int(re.sub(r"\D", "", args.decade)[:4])
        items = [it for it in items if d <= (it.get("tags") or {}).get("year", 0) < d + 10]
    return items


def add_filter_args(sp):
    sp.add_argument("--format", help="comma list: flac,wav,alac,dsf,mp3,...")
    sp.add_argument("--lang", help="tamil, telugu, hindi, malayalam, english, japanese, ... (tags, script, album, "
                                   "artist, words, channel; see `inspect` for the reasons)")
    sp.add_argument("--loose", action="store_true", help="with --lang: include weak matches (e.g. channel name only)")
    sp.add_argument("--version", help="only these versions: instrumental,remix,live,karaoke,acoustic,ost,...")
    sp.add_argument("--no-version", help="exclude these versions, e.g. karaoke,remix")
    sp.add_argument("--length", choices=("short", "song", "long", "mix"), help="<1 min, 1-6, 6-15, >15 min")
    sp.add_argument("--genre", help="from file tags (read on header check)")
    sp.add_argument("--decade", help="e.g. 1990s, from file tags")
    sp.add_argument("--no-sync", action="store_true", help="use the index as is (still checks headers/fakes online)")


# ---------- albums ----------
# A post has no album field, so albums are inferred, strongest signal first:
#   1. ALBUM tag inside the file (read during the header check, cover art skipped)
#   2. "(From "Movie")" in title/file name, or "Album:/Movie:" in the caption
#   3. Telegram media groups (posted together) and runs of consecutive numbered tracks in one chat

FROM_RE = re.compile(r"""\(\s*from\s*[_"'“”‘’]*\s*([^)"'“”_]+?)\s*[_"'“”‘’]*\s*\)""", re.I)
CAPTION_ALBUM_RE = re.compile(r"^\s*(?:album|movie|film|ost)\s*[:\-–]\s*(.+?)\s*$", re.I | re.M)
TRACKNO_RE = re.compile(r"^\s*(?:\d-)?(\d{1,3})\s*[-._)\s]")


EDITION_RE = re.compile(r"[\(\[][^\)\]]*(soundtrack|o\.?s\.?t|motion picture|deluxe|edition|remaster|expanded|"
                        r"anniversary|bonus|version|hi-?res|24.?bit|flac)[^\)\]]*[\)\]]", re.I)


def album_norm(name):
    """'Baththa (Original Motion Picture Soundtrack)' == 'Baththa' == 'BATHTHA [Deluxe Edition]'"""
    return norm_text(EDITION_RE.sub(" ", name or ""))


def track_no(it):
    t = (it.get("tags") or {}).get("track")
    if t:
        return t
    m = TRACKNO_RE.match(it["file_name"] or it["title"] or "")
    return int(m[1]) if m else None


def album_hint(it):
    tag = (it.get("tags") or {}).get("album")
    if tag:  # 'Magale (From "Baththa") - Single' is a track of Baththa
        m = FROM_RE.search(tag)
        tag = m[1] if m else re.sub(r"\s*[-–]\s*(single|ep)\s*$", "", tag, flags=re.I)
        return tag, "tag"
    for s in (it["title"], it["file_name"]):
        m = FROM_RE.search(s or "")
        if m:
            return m[1], "from"
    m = CAPTION_ALBUM_RE.search(it["caption"] or "")
    return (m[1][:80], "caption") if m else (None, None)


def from_hint(it):
    """Album/movie named in the post itself (title, file name, caption), ignoring the file's own album tag."""
    for t in (it["title"], it["file_name"]):
        m = FROM_RE.search(t or "")
        if m:
            return m[1]
    m = CAPTION_ALBUM_RE.search(it["caption"] or "")
    return m[1] if m else None


def album_index(items):
    """-> {album_key: {"name", "source", "artists": Counter, "items": [...], "chats": set}}"""
    from collections import Counter
    runs, cur = [], []
    for it in sorted(items, key=lambda x: (x["chat_id"], x["msg_id"])):
        t = track_no(it)
        if cur:
            last = cur[-1]
            same = it["chat_id"] == last["chat_id"] and (
                (it["grouped_id"] and it["grouped_id"] == last["grouped_id"]) or
                (it["msg_id"] - last["msg_id"] <= 3 and t is not None and track_no(last) is not None
                 and t == track_no(last) + 1))
            if not same:
                runs.append(cur)
                cur = []
        cur.append(it)
    if cur:
        runs.append(cur)
    albums = {}
    for run in runs:
        hints = Counter()
        sources = {}
        for it in run:
            name, src = album_hint(it)
            if name and len(album_norm(name)) > 1:
                hints[album_norm(name)] += 1
                sources.setdefault(album_norm(name), (name, src))
        if hints:
            key = hints.most_common(1)[0][0]
            name, src = sources[key]
        elif len(run) >= 3 and sum(track_no(i) is not None for i in run) >= 3:
            key, name, src = f"run:{run[0]['ref']}", None, "run"
        else:
            continue
        a = albums.setdefault(key, {"key": key, "name": name, "source": src, "artists": Counter(), "items": [],
                                    "chats": set()})
        if (a["source"] == "from" and src == "tag") or (src == a["source"] and len(name or "") < len(a["name"] or "")):
            a["name"], a["source"] = name, src  # a real tag beats a title hint; shortest spelling wins
        numbered = sum(track_no(i) is not None for i in run) >= 3
        for it in run:  # a track number counts only from a real album copy (tagged, or part of a numbered run)
            tags = it.get("tags") or {}
            it["album_track"] = tags.get("track") if tags.get("tracks", 0) > 1 or (tags.get("track") and numbered) \
                else (track_no(it) if numbered else None)
        a["items"] += run
        a["chats"].add(run[0]["chat"])
        for it in run:
            artist = (it.get("tags") or {}).get("albumartist") or it["performer"]
            if artist:
                a["artists"][unicodedata.normalize("NFKC", artist)[:60]] += 1
    for a in albums.values():
        if a["name"] is None:
            top = a["artists"].most_common(1)
            a["name"] = f"{top[0][0] if top else 'Unknown'} – untitled ({a['items'][0]['chat'][:20]})"
    return albums


def album_summary(a):
    tracks = sorted({i["album_track"] for i in a["items"] if i.get("album_track")})
    missing = [n for n in range(1, (max(tracks) if tracks else 0) + 1) if n not in tracks]
    songs = group(list(a["items"]))
    best = [g[0] for g in songs]
    return {"album": a["name"], "source": a["source"], "artist": ", ".join(n for n, _ in a["artists"].most_common(2)),
            "songs": len(songs), "tracks": len(tracks), "missing": missing[:20], "copies": len(a["items"]),
            "lossless": sum(b["lossless"] for b in best), "best_labels": sorted({b["label"] for b in best})[:4],
            "size_mb": round(sum(b["size"] for b in best) / 1e6, 1), "chats": sorted(a["chats"])[:3], "key": a["key"]}


def match_albums(albums, names, artist=None):
    hits = []
    for a in albums.values():
        n = album_norm(a["name"])
        ok = any(fuzz.ratio(album_norm(v), n) >= 88 or album_norm(v) == n or
                 (len(album_norm(v)) > 3 and re.search(rf"\b{re.escape(album_norm(v))}\b", n)) for v in names)
        if ok and artist:
            ok = any(fuzz.partial_ratio(norm_text(artist), norm_text(x)) >= 85 for x in a["artists"])
        if ok:
            hits.append(a)
    return hits


async def name_albums(client, db, albums):
    """Unnamed runs: read the tags of one track each (~130 KB) to learn the album name. Cached forever."""
    firsts = [a["items"][0] for a in albums if a["source"] == "run"]
    if firsts and client is not None:
        await ensure_probed(client, db, firsts, ("tags",))
        apply_probes(db, [i for a in albums for i in a["items"]])


# ---------- plans (artist or album) ----------

def slug(s):
    return re.sub(r"[^\w]+", "-", s.lower()).strip("-")


def artist_matches(it, variants):
    hay = norm_text(" ".join([it["performer"], it["title"], it["file_name"], it["caption"]]))
    squashed = hay.replace(" ", "")
    return any(norm_text(v) in hay or norm_text(v).replace(" ", "") in squashed for v in variants)


async def plan_from_items(client, db, items, name, variants, kind, verify=False, stereo=False, args=None):
    """Best copy of every unique song in items -> plans/<kind>-<name>.json (review, then --download)."""
    need = ("channels",) if stereo else ()
    apply_probes(db, items)
    # probe only what can change the pick: lossless copies, or lossy copies of songs with no lossless copy
    to_probe = []
    strip = variants if kind == "artist" else ()
    for g in group(items, strip=strip):
        lossless = [it for it in g if it["lossless"]]
        to_probe += lossless or g
    await ensure_probed(client, db, to_probe, need + (("tags",) if kind == "album" else ()))
    if stereo:
        items = stereo_only(items)
    if args is not None:
        items = filter_tags(items, args)
    groups = group(items, strip=strip)
    if verify:
        groups = await verify_best(client, db, items, lambda its: group(its, strip=strip))
    entries = []
    for g in groups:
        best = g[0]
        flags = []
        if kind == "artist" and not any(artist_matches(it, variants) for it in g):
            flags.append("artist-not-in-metadata")
        if not best["lossless"] and best["score"] < 240:
            flags.append("low-quality-only")
        from collections import Counter
        nums = Counter(i.get("album_track") for i in g if i.get("album_track")) if kind == "album" else Counter()
        track = nums.most_common(1)[0][0] if nums else None
        entries.append({"song": best["norm"], "track": track, "duration": best["duration"], "best": public(best),
                        "alternatives": [public(it) for it in g[1:6]], "copies": len(g), "flags": flags,
                        "skip": False})
    if kind == "album":
        entries.sort(key=lambda e: (e["track"] is None, e["track"] or 0))
    tracks = sorted({e["track"] for e in entries if e["track"]})
    plan = {"kind": kind, "name": name, "artist": name, "variants": variants, "songs": len(entries),
            "missing_tracks": [n for n in range(1, (max(tracks) if tracks else 0) + 1) if n not in tracks],
            "total_mb": round(sum(e["best"]["size"] for e in entries) / 1e6, 1),
            "lossless": sum(e["best"]["lossless"] for e in entries), "entries": entries}
    PLANS.mkdir(exist_ok=True)
    path = PLANS / f"{'album-' if kind == 'album' else ''}{slug(variants[0])}.json"  # what the user typed
    path.write_text(json.dumps(plan, indent=1, ensure_ascii=False))
    return plan, path


async def build_plan(client, db, variants, verify=False, stereo=False, args=None):
    items = match(load_items(db), variants)
    if args is not None:
        items = filter_items(apply_probes(db, items), args)
    return await plan_from_items(client, db, items, variants[0], variants, "artist", verify, stereo, args)


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
    missing = f" · missing tracks {plan['missing_tracks']}" if plan.get("missing_tracks") else ""
    out.print(f"{plan['songs']} unique songs · {plan['lossless']} lossless · {plan['total_mb']} MB{missing} → {path}")


async def run_plan(client, db, plan):
    name = plan.get("name") or plan["artist"]
    folder = DOWNLOADS / safe_name(name if plan.get("kind") == "album" else name.title())
    picks = []
    for e in plan["entries"]:
        if e["skip"]:
            continue
        key = f"{plan.get('kind', 'artist')}:{slug(name)}|{e['song']}|{round(e['duration'] / 3)}"
        have = db.execute("SELECT path, score FROM downloads WHERE song_key=? ORDER BY score DESC", (key,)).fetchone()
        if have and have["score"] is not None and have["score"] >= e["best"]["score"] and Path(have["path"]).exists():
            continue
        prefix = f"{e['track']:02d} - " if plan.get("kind") == "album" and e.get("track") else ""
        picks.append((e["best"] | {"norm": e["song"], "name_prefix": prefix}, folder, key))
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
            for flt in SEARCH_FILTERS.values():
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
        r = db.execute(ROW_SQL + " AND f.chat_id=? AND f.msg_id=?", (int(chat), int(mid))).fetchone()
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
    if getattr(args, "offline", False) or getattr(args, "no_sync", False) or client is None:
        return
    await sync(client, db)


async def cmd_search(client, db, args):
    await prepare(client, db, args)
    items = match(load_items(db, args.chat), args.queries)
    if (not items or args.remote) and not args.offline:
        if await remote_search(client, db, args.queries, force=args.remote):
            items = match(load_items(db, args.chat), args.queries)
    items = filter_items(apply_probes(db, items), args)
    items = await ensure_probed(client, db, items, needs(args)) if not args.no_probe else apply_probes(db, items)
    items = filter_tags(items, args)
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
    items = filter_items(apply_probes(db, items), args)
    # shortlist: everything already probed + highest estimated bitrate lossless candidates (true hi-res is far ahead)
    unprobed = sorted((it for it in items if not it["probed"]), key=lambda it: (-it["lossless"], -it["kbps"]))
    shortlist = [it for it in items if it["probed"]]
    batch = max(100, args.n * 20)
    while True:  # with --stereo, surround files drop out: widen the shortlist until n songs remain
        shortlist += unprobed[:batch]
        unprobed = unprobed[batch:]
        shortlist = await ensure_probed(client, db, shortlist, needs(args))
        if args.stereo:
            shortlist = stereo_only(shortlist)
        shortlist = filter_tags(shortlist, args)
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
        return db.execute(ROW_SQL + " AND f.chat_id=? AND f.msg_id=?", (int(chat), int(mid))).fetchone()

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
        plan, path = await build_plan(client, db, args.names, verify=args.verify, stereo=args.stereo, args=args)
        if args.json:
            print(json.dumps(plan | {"plan_file": str(path)}, ensure_ascii=False, indent=1))
        else:
            show_plan(plan, path)
    if args.download:
        await run_plan(client, db, plan)


async def cmd_albums(client, db, args):
    """Find albums by name or artist. Unnamed runs in the results get named from one track's tags (~130 KB)."""
    await prepare(client, db, args)
    items = filter_items(apply_probes(db, load_items(db, args.chat)), args)
    albums = album_index(items)

    track_refs = {it["ref"] for it in match(items, args.queries, min_score=101)} if args.queries else set()  # exact/prefix words only

    def hits():
        found = match_albums(albums, args.queries, args.artist) if args.queries else []
        keys = {a["key"] for a in found}
        # albums with a matching track (e.g. a still-unnamed run containing "05. Vaazhanum Baththa.flac")
        found += [a for a in albums.values() if a["key"] not in keys and any(i["ref"] in track_refs for i in a["items"])]
        who = args.artist or (args.queries[0] if args.queries else None)
        if who:  # also albums by an artist of that name (covers still-unnamed runs)
            keys = {a["key"] for a in found}
            found += [a for a in albums.values() if a["key"] not in keys and
                      any(fuzz.partial_ratio(norm_text(who), norm_text(x)) >= 90 for x in a["artists"])]
        return sorted(found, key=lambda a: (a["source"] == "run", -len(a["items"])))[:args.limit]

    found = hits()
    if any(a["source"] == "run" for a in found):
        await name_albums(client, db, found)
        albums = album_index(items)
        found = hits()
    rows = [album_summary(a) for a in found]
    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=1))
        return
    table = Table(header_style="bold")
    for col in ("#", "Album", "Artist", "Songs", "Missing", "Best", "Size", "Channel"):
        table.add_column(col, no_wrap=col in ("#", "Songs", "Size"))
    for i, r in enumerate(rows):
        table.add_row(str(i), r["album"] + ("" if r["source"] != "run" else " [dim](untitled)[/]"), r["artist"][:30],
                      f"{r['songs']} ({r['lossless']} lossless)", ",".join(map(str, r["missing"][:6])) or "-",
                      " ".join(r["best_labels"][:2]), f"{r['size_mb']:.0f}MB", r["chats"][0][:22] if r["chats"] else "")
    out.print(table)
    out.print('Plan one or more: hifigram.py album "Album A" "Album B|alt spelling" --plan --verify')


async def cmd_album(client, db, args):
    """One plan per album name ("Name|alt spelling" for variants); --download downloads them all."""
    albums = None
    for spec in args.names:
        variants = [v.strip() for v in spec.split("|") if v.strip()]
        path = PLANS / f"album-{slug(variants[0])}.json"
        if args.download and path.exists() and not args.replan:
            plan = json.loads(path.read_text())
            err.print(f"Using saved plan {path} ({plan['songs']} songs)")
        else:
            if albums is None:
                await prepare(client, db, args)
                items = filter_items(apply_probes(db, load_items(db)), args)
                albums = album_index(items)
            found = match_albums(albums, variants, args.artist)
            # film songs are often posted inside compilations or as singles: collect tracks marked "(From X)" too
            keys = {album_norm(v) for v in variants}
            loose = [i for i in items if album_norm(from_hint(i) or "") in keys]
            if not found and not loose:
                err.print(f'[red]No album matching[/] "{spec}". Try: hifigram.py albums "{variants[0]}"')
                continue
            name = max(found, key=lambda a: (a["source"] == "tag", len(a["items"])))["name"] if found else variants[0]
            refs = {i["ref"] for a in found for i in a["items"]}
            # a loose track's number belongs to the compilation it came from, not to this album
            tracks = [i for a in found for i in a["items"]] + [i | {"album_track": None} for i in loose if i["ref"] not in refs]
            plan, path = await plan_from_items(client, db, tracks, name, variants, "album", args.verify, args.stereo, args)
            if args.json:
                print(json.dumps(plan | {"plan_file": str(path)}, ensure_ascii=False, indent=1))
            else:
                show_plan(plan, path)
        if args.download:
            await run_plan(client, db, plan)


def decode_raw(blob):
    from telethon.extensions import BinaryReader
    return BinaryReader(zlib.decompress(blob)).tgread_object()


async def cmd_inspect(client, db, args):
    """Every stored version of a post (or chat / doc probe) — the exact data Telegram sent, decoded."""
    def dump(o):
        return json.dumps(o, default=lambda x: x.isoformat() if hasattr(x, "isoformat") else
                          (f"<{len(x)} bytes>" if isinstance(x, (bytes, bytearray)) else str(x)), ensure_ascii=False, indent=1)
    for ref in args.refs:
        rows = db.execute("SELECT kind, key, first_seen, last_seen, data FROM raw WHERE key=? ORDER BY first_seen", (ref,)).fetchall()
        if not rows:
            err.print(f"[yellow]no raw data for[/] {ref}")
        for r in rows:
            out.print(f"[bold]{r['kind']} {r['key']}[/] seen {time.strftime('%Y-%m-%d %H:%M', time.localtime(r['first_seen']))}"
                      f" → {time.strftime('%Y-%m-%d %H:%M', time.localtime(r['last_seen']))}")
            print(dump(decode_raw(r["data"]).to_dict()))
        chat, _, mid = ref.partition(":")
        if mid.isdigit():
            f = db.execute("SELECT doc_id FROM files WHERE chat_id=? AND msg_id=?", (int(chat), int(mid))).fetchone()
            if f:
                for table in ("probes", "verify"):
                    row = db.execute(f"SELECT * FROM {table} WHERE doc_id=?", (f[0],)).fetchone()
                    if row:
                        out.print(f"[bold]{table}[/]")
                        print(dump(dict(row)))


async def cmd_enrich(client, db, args):
    """Re-read headers of files probed before every field was kept (PROBE_VERSION < 4). Bytes are cached after."""
    rows = [r["doc_id"] for r in db.execute("SELECT doc_id, info FROM probes WHERE info IS NOT NULL")
            if "full" not in json.loads(r["info"])]
    items = [it for it in load_items(db) if it["doc_id"] in set(rows)]
    err.print(f"{len(rows)} older header checks to enrich")
    await ensure_probed(client, db, items, ("full",))


async def cmd_stats(client, db, args):
    q = lambda sql: db.execute(sql).fetchone()[0]
    out.print(f"audio posts {q('SELECT COUNT(*) FROM files')} · unique files {q('SELECT COUNT(DISTINCT doc_id) FROM files')}"
              f" · probed {q('SELECT COUNT(*) FROM probes WHERE info IS NOT NULL')}"
              f" · downloads {q('SELECT COUNT(*) FROM downloads')}"
              f" · chats scanned {q('SELECT COUNT(DISTINCT chat_id) FROM scans WHERE done=1')}"
              f" · pending scans {q('SELECT COUNT(*) FROM scans WHERE done=0')}")
    out.print(f"raw objects {q('SELECT COUNT(*) FROM raw')} ({q('SELECT COUNT(DISTINCT key) FROM raw')} distinct)"
              f" · posts with facts {q('SELECT COUNT(*) FROM post_meta')} · views snapshots {q('SELECT COUNT(*) FROM views_history')}"
              f" · soft-deleted {q('SELECT COUNT(*) FROM files WHERE deleted_at IS NOT NULL')} · events {q('SELECT COUNT(*) FROM events')}")
    if BYTES_DB.exists():
        b = sqlite3.connect(BYTES_DB)
        n, size = b.execute("SELECT COUNT(*), COALESCE(SUM(LENGTH(data)),0) FROM blocks").fetchone()
        out.print(f"byte cache {n} blocks · {size / 1e6:.1f} MB")
    for r in db.execute("SELECT c.title, COUNT(*) n FROM files f JOIN chats c ON c.chat_id=f.chat_id "
                        "GROUP BY f.chat_id ORDER BY n DESC LIMIT 15"):
        out.print(f"  {r['n']:>7}  {r['title']}")
    if getattr(args, "categories", False):
        from collections import Counter
        items = apply_probes(db, load_items(db))
        tagged = [it for it in items if it["tags"]]
        cats = {
            "format": Counter(it["ext"] for it in items),
            "language": Counter((lang_of(it) if it["lang_conf"] >= 0.45 else "") or "unknown"
                                for it in compute_languages(db, items)),
            "length": Counter(length_bucket(it["duration"]) for it in items),
            "version": Counter(v for it in items for v in versions_of(it)),
            f"genre (tags, {len(tagged)} files)": Counter(it["tags"].get("genre", "?") for it in tagged),
            f"decade (tags, {len(tagged)} files)": Counter(f"{it['tags']['year'] // 10 * 10}s" for it in tagged
                                                           if it["tags"].get("year")),
        }
        for name, c in cats.items():
            out.print(f"[bold]{name}[/]: " + " · ".join(f"{k} {v:,}" for k, v in c.most_common(12)))
        out.print(f"albums: {len(album_index(items)):,} (named + numbered runs)")


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
    ins = sub.add_parser("inspect")
    ins.add_argument("refs", nargs="+", help="CHAT:MSG (or a chat id) — every stored version, decoded")
    sub.add_parser("enrich")
    st = sub.add_parser("stats")
    st.add_argument("--categories", action="store_true", help="format/language/length/version/genre/decade/albums")
    s = sub.add_parser("search")
    s.add_argument("queries", nargs="+")
    s.add_argument("--json", action="store_true")
    s.add_argument("--chat", help="only chats whose title contains this")
    s.add_argument("--limit", type=int, default=100, help="max song groups")
    s.add_argument("--no-probe", action="store_true", help="skip real-quality header check")
    s.add_argument("--offline", action="store_true", help="don't sync first, never call Telegram")
    s.add_argument("--remote", action="store_true", help="also ask Telegram (default: only when index has nothing)")
    s.add_argument("--verify", action="store_true", help="spectrum-check best copies for fakes (~1-5 MB each, cached)")
    add_filter_args(s)
    s.add_argument("--stereo", action="store_true", help="skip surround (5.1 etc) files")
    t = sub.add_parser("top")
    t.add_argument("n", type=int, nargs="?", default=5)
    t.add_argument("queries", nargs="*")
    t.add_argument("--chat")
    t.add_argument("--min-length", type=int, default=60, help="skip tracks shorter than this many seconds")
    t.add_argument("--json", action="store_true")
    t.add_argument("--offline", action="store_true")
    t.add_argument("--verify", action="store_true", help="spectrum-check for fakes (~1-5 MB each, cached)")
    add_filter_args(t)
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
    add_filter_args(a)
    al = sub.add_parser("albums")
    al.add_argument("queries", nargs="*", help="album name(s) or artist")
    al.add_argument("--artist")
    al.add_argument("--chat")
    al.add_argument("--limit", type=int, default=30)
    al.add_argument("--json", action="store_true")
    al.add_argument("--offline", action="store_true")
    add_filter_args(al)
    ab = sub.add_parser("album")
    ab.add_argument("names", nargs="+", help='one album per argument; "Name|alt spelling" for variants')
    ab.add_argument("--artist", help="only albums by this artist (disambiguates common names)")
    ab.add_argument("--plan", action="store_true", help="build plans only (default)")
    ab.add_argument("--download", action="store_true", help="download the saved plans (builds missing ones)")
    ab.add_argument("--replan", action="store_true")
    ab.add_argument("--verify", action="store_true")
    ab.add_argument("--stereo", action="store_true")
    ab.add_argument("--json", action="store_true")
    ab.add_argument("--offline", action="store_true")
    add_filter_args(ab)
    a.add_argument("--stereo", action="store_true", help="skip surround (5.1 etc) files")
    # refs like -1001234567890:42 start with "-": move them behind "--" so argparse doesn't read them as options
    argv = sys.argv[1:]
    refs = [a for a in argv if re.fullmatch(r"-\d+:\d+", a)]
    if refs and "--" not in argv:
        argv = [a for a in argv if a not in refs] + ["--"] + refs
    args = p.parse_args(argv)

    db = open_db()
    if args.cmd == "stats":
        return await cmd_stats(None, db, args)
    if args.cmd == "inspect":
        return await cmd_inspect(None, db, args)
    if getattr(args, "offline", False) and args.cmd in ("search", "top", "artist", "albums", "album") and not getattr(args, "download", False):
        if not synced_once(db) and not db.execute("SELECT 1 FROM files LIMIT 1").fetchone():
            sys.exit("Index empty. Run: hifigram.py sync")
        return await {"search": cmd_search, "top": cmd_top, "artist": cmd_artist, "albums": cmd_albums,
                      "album": cmd_album}[args.cmd](None, db, args)
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
        elif args.cmd == "enrich":
            await cmd_enrich(client, db, args)
        elif args.cmd == "albums":
            await cmd_albums(client, db, args)
        elif args.cmd == "album":
            await cmd_album(client, db, args)
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
