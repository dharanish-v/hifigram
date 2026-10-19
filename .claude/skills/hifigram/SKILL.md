---
name: hifigram
description: Find and download the best-quality copy of songs from the user's own Telegram chats/channels with the hifigram CLI. Use when the user asks to find/search/get a song or album, "download all songs by <artist>", "top N highest quality", "is this FLAC fake", or describes a song vaguely (movie, lyrics, mood) and wants it from Telegram.
---

# hifigram

`HG` below means `<repo>/.venv/bin/python <repo>/hifigram.py`, where `<repo>` is the hifigram checkout (the folder
containing `hifigram.py`; use `$HIFIGRAM_DIR` if set). Run commands from anywhere with absolute paths.

**Rule: never waste a Telegram call.** Everything is cached in `<repo>/index.db`: audio post metadata from every
chat, header-probe results per file (doc_id), spectrum verdicts, scan checkpoints, downloads. Searches run locally.
Don't write throwaway scripts that call Telegram. If you must, store what they fetch via `hifigram.remember(db, msgs)`.

If a command exits with "Not logged in" / "No config.json", the user must run `HG login` in a normal terminal (it
asks for the code Telegram sends). Never try to log in for them. Never print, copy or commit `config.json` or
`session.session`: the session gives full access to their Telegram account.

## Commands

- `HG sync`: index audio from all joined chats. First run is long (resumable); later runs skip unchanged chats
  with zero calls and fetch only new posts. `search`/`top`/`artist` auto-sync first; `--offline` = no Telegram at all.
- `HG stats`: index size and per-chat counts (no Telegram calls).
- `HG search "q1" "q2" --json [--chat NAME]`: local fuzzy search (every query word must match title/artist/file
  name/caption; prefix and typo tolerant). Output: groups `{song, duration, copies:[{ref, doc_id, label, tier,
  score, size, chat, title, performer, file_name, caption, link, probed, channels}]}`, best copy first. Nothing
  in the index → asks Telegram global search once (stored; the query is cached 7 days). `--remote` forces it.
- `HG top 5 ["query"] [--chat NAME] [--stereo] [--verify] --json`: highest-quality unique songs; skips tracks
  under 60 s (`--min-length`).
- `HG get <ref> [<ref> ...]`: download to `<repo>/downloads/`. A file (doc_id) is never downloaded twice; lossless
  downloads are spectrum-checked locally for free.
- `HG verify <ref> ...` or `--verify` on search/top/artist: fake-lossless check (MP3/AAC-sourced, upsampled,
  padded 24/32-bit, float-from-24) from two short slices, about 1–3 MB per file, cached forever. `search --verify`
  checks the top 10 groups only. Labels: `✓` genuine · `⚠ fake: lossy source ~128k` · `⚠ really 24/44.1` ·
  `? maybe 320k source` (ambiguous). Fakes are re-ranked down. Spectrogram: `<repo>/spectrograms/<doc_id>.png`
  (Read it to show the user).
- `--stereo`: skip surround (labels show `5.1` / `mono`). Use it for "best quality" unless they want surround.
- `HG artist "Canonical Name" "variant" --plan [--verify] [--stereo] --json` → `<repo>/plans/<slug>.json`;
  `HG artist "Canonical Name" --download` downloads the saved plan (resumable).
- `HG channels "name" --json`: public channels by name. `HG scan @username`: index one without joining. Only scan
  channels the user picked.

## Find one song

1. Turn the request into several queries yourself: likely titles, spelling and transliteration variants, the
   movie or album name (titles are often "Song (From Movie)"), native-script titles. For vague descriptions, name
   candidate songs from your own knowledge. Local search is free, so try many variants.
2. `HG search ... --json`. Show a short ranked list (song, best label, size, channel) and recommend one. Add
   `--verify` when they care about quality.
3. Download on their pick: `HG get <ref>`. Mention the size first if it's large.

## All songs by an artist

1. `HG artist "Canonical Name" "variant" --plan --verify --stereo --json`.
2. Review the entries: `artist-not-in-metadata` (probably another artist), `low-quality-only`, duplicates the
   grouping missed, unwanted remixes or karaoke. Set `"skip": true` in the plan JSON to exclude.
3. Tell the user: N songs, lossless count, total size, what you skipped and why. **Ask before downloading.**
4. `HG artist "Canonical Name" --download`.

## Notes

- Quality order: lossless (bit depth counted up to 24, then sample rate) > lossy by bitrate (AAC/Opus above MP3).
  Verified results override header claims; a verified-genuine copy wins ties.
- `est` / `~` labels = not header-checked yet (guessed from size ÷ duration).
- Without `--verify`, labels are what the file claims. Say so when ranking "highest quality".
- Use only for content the user has the right to access and download.
