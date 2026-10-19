# hifigram

**Find the best-quality copy of any song across your Telegram channels, without downloading them all.**

The same track is often posted in many channels as MP3, FLAC, WAV and ALAC, at 16/44.1, 24/96 or "32/192". Some of those
"lossless" files are really MP3s or upsampled CD rips. hifigram indexes every audio post in the chats you've joined,
reads the **real** bit depth, sample rate and channel count from a few hundred KB of each file's header, and can
**detect fake lossless files** from a spectrum of two short slices. It then groups the copies of each song and ranks them best
first, so you download the one worth having.

```
$ python hifigram.py search "song title" --verify
┏━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ # ┃ Quality                       ┃ Size   ┃ Len  ┃ Channel      ┃ Title               ┃
┡━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 0 │ FLAC 24/48 ✓                  │ 54.8MB │ 4:18 │ Lossless Hub │ Artist - Song Title │
│ 1 │ ALAC 24/48                    │ 59.1MB │ 4:18 │ Audio Vault  │ Artist - Song Title │
│ 2 │ FLAC 16/44.1                  │ 40.6MB │ 4:18 │ Lossless Hub │ Artist - Song Title │
│ 3 │ FLAC 24/96 ⚠ really 24/44.1   │ 98.0MB │ 4:18 │ HiRes Drops  │ Artist - Song Title │
│ 4 │ MP3 320                       │  9.9MB │ 4:18 │ Music Daily  │ Song Title          │
└───┴───────────────────────────────┴────────┴──────┴──────────────┴─────────────────────┘
```

## Features

- **Search all your chats at once.** A local index (SQLite) covers every audio post in every joined chat and channel,
  including audio posted as plain files. Search is fuzzy (typos, partial words, 𝐟𝐚𝐧𝐜𝐲 𝐔𝐧𝐢𝐜𝐨𝐝𝐞 titles), takes
  seconds over hundreds of thousands of files, and works offline.
- **Real quality, not file size.** It reads FLAC STREAMINFO, MP3 frame and Xing headers, MP4/ALAC atoms, WAV `fmt `
  and ADTS headers through ranged downloads, and skips embedded cover art. Typical cost is 130–400 KB per file.
- **Fake-lossless detection** (`--verify`). It decodes about 3.5 s at two points of the song and flags:
  - MP3/AAC-sourced "FLAC": a brick-wall cutoff at 16–20 kHz
  - upsampled "hi-res": empty ultrasonics above 22.05 or 24 kHz
  - padded bit depth: "24/32-bit" that is really 16/24-bit, including 32-bit float on a 24-bit grid

  It costs about 1–3 MB per file and saves a spectrogram PNG for every check. Fakes are moved down the ranking.
- **Groups copies of the same song** by cleaned title and duration, so reposts and spelling variants become one entry.
- **`top N`**: the highest-quality unique songs across everything, or within a search or channel. `--stereo` skips
  5.1 surround and `--min-length` skips intros.
- **Bulk artist downloads**: a plan picks the best copy of every unique song by an artist. Review it, then download. It
  can resume, and it skips anything you already have.
- **Albums**: `albums "name or artist"` finds albums with track count, missing tracks and best quality. `album "A" "B"`
  plans several albums at once and downloads them as numbered files. Albums are inferred from file tags (read during
  the header check, cover art skipped), `(From "Movie")` titles, captions, Telegram album posts and numbered runs of
  tracks. Film songs scattered across compilations and singles are collected too.
- **Categories and filters**: `--format flac,wav,dsf`, `--lang tamil`, `--version instrumental`, `--no-version karaoke`,
  `--length song`, plus `--genre` and `--decade` from file tags. `stats --categories` shows the breakdown.
- **DSD** (`.dsf`) is recognised and labelled `DSD64/128/256`.
- **Never fetches anything twice.** Scans are checkpointed and resumable. Later syncs fetch only new posts, and unchanged
  chats cost zero API calls. Header checks, verify results and downloads are cached per file, so a file reposted in
  ten channels is checked once.
- **Public channels without joining.** Find them by name (`channels`) and index them (`scan @name`).

## Install

Needs Python 3.10+ and (for `--verify`) [ffmpeg](https://ffmpeg.org).

```sh
git clone https://github.com/dharanish-v/hifigram && cd hifigram
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
brew install ffmpeg          # or apt install ffmpeg
```

Create a Telegram API app at <https://my.telegram.org> → *API development tools* to get an `api_id` and `api_hash`.
Then log in once, in a normal terminal, because it asks for the code Telegram sends you:

```sh
.venv/bin/python hifigram.py login
```

This stores `config.json` and `session.session` next to the script. **The session file gives full access to your
Telegram account. Never share or commit it.** Both are in `.gitignore`.

## Usage

```sh
python hifigram.py sync                                  # index all chats (first run is long and resumable)
python hifigram.py search "song title" "alt spelling"    # grouped by song, best copy first
python hifigram.py search "song" --verify                # + fake-lossless check on the top results
python hifigram.py top 10 --stereo --verify              # best-quality songs across everything
python hifigram.py top 5 "artist" --chat "lossless"      # within a search and channel
python hifigram.py get -1001234567890:4242               # download (refs come from search output)
python hifigram.py artist "Artist Name" "alt spelling" --plan --verify   # best copy of every song -> plans/
python hifigram.py artist "Artist Name" --download                       # download the plan
python hifigram.py albums "movie or artist"             # albums: songs found, missing tracks, best quality
python hifigram.py album "Album A" "Album B|alt" --plan --verify --stereo  # one plan per album -> plans/album-*.json
python hifigram.py album "Album A" "Album B" --download  # numbered files in downloads/<Album>/
python hifigram.py search "song" --lang tamil --format flac --no-version karaoke   # filters
python hifigram.py stats --categories                    # formats, languages, versions, genres, decades, albums
python hifigram.py channels "flac"                       # find public channels by name
python hifigram.py scan @somechannel                     # index one without joining
python hifigram.py stats                                 # what's in the index
python hifigram.py                                       # interactive search + pick
```

Add `--json` for machine-readable output. `search`, `top` and `artist` run a quick incremental sync first. Add
`--offline` to skip that and never touch Telegram.

### Daily sync (macOS)

```sh
sed "s|__DIR__|$PWD|g" com.hifigram.sync.plist > ~/Library/LaunchAgents/com.hifigram.sync.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.hifigram.sync.plist
```

### Claude Code skill

`.claude/skills/hifigram/SKILL.md` teaches [Claude Code](https://claude.com/claude-code) to drive hifigram. Ask in plain
language ("find the best copy of …", "plan all songs by …") and it handles spelling variants, quality checks and
review steps. It loads automatically inside this repo. To use it anywhere, copy it to `~/.claude/skills/hifigram/`
and set `HIFIGRAM_DIR` to your checkout.

## How quality is ranked

1. Lossless beats lossy.
2. Then bit depth (counted up to 24: no real recording has more) and sample rate.
3. Lossy files are ranked by bitrate, with AAC and Opus weighted above MP3.
4. Spectrum results override the header. A FLAC with a 16 kHz cutoff ranks like the 128 kbps MP3 it came from.
5. A verified-genuine copy wins ties.

Labels: `FLAC 24/96 ✓` genuine · `⚠ fake: lossy source ~128k` · `⚠ really 24/44.1` · `? maybe 320k source` (cutoff
around 20 kHz: a 320 kbps MP3 or a low-passed master) · `est` / `~` = not yet header-checked.

## Limits

- It sees only chats you've joined, plus public channels you `scan`.
- Spectrum checks are heuristics on two short slices. Inspect `spectrograms/<id>.png` when it matters.
- Telegram rate limits apply. The client waits and continues automatically.

## Respect

Use it for content you have the right to access and download. hifigram reads only chats your own account can see,
uses normal API calls at a gentle pace, and never posts or forwards anything.

## License

[MIT](LICENSE)
