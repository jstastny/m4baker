# m4baker

Convert audiobook collections (MP3/M4A/OGG/FLAC folders, ZIP archives) to M4B files with chapters, metadata, and cover art.

## Quick start

```bash
docker run --rm -v /path/to/audiobooks:/data jstastny/m4baker
```

Output goes to `/path/to/audiobooks/m4b/` with filenames like `Author - Title.m4b`.

## AI-assisted metadata

Provide a [Claude API key](https://console.anthropic.com/settings/keys) for accurate author/title detection — handles messy tags, narrator vs author confusion, encoding issues, etc.:

```bash
docker run --rm \
  -e CLAUDE_API_KEY=sk-ant-... \
  -v /path/to/audiobooks:/data \
  jstastny/m4baker
```

Without the key, built-in heuristics are used (work well for most books).

To override the model (default: cheapest available — Haiku first, Sonnet fallback):

```bash
docker run --rm \
  -e CLAUDE_API_KEY=sk-ant-... \
  -e CLAUDE_MODEL=claude-sonnet-4-20250514 \
  -v /path/to/audiobooks:/data \
  jstastny/m4baker
```

## More options

```bash
# Parallel processing (4 books at once)
docker run --rm -v /path/to/audiobooks:/data jstastny/m4baker -j 4

# Dry run — show what would be converted
docker run --rm -v /path/to/audiobooks:/data jstastny/m4baker -n

# Combine
docker run --rm \
  -e CLAUDE_API_KEY=sk-ant-... \
  -v /path/to/audiobooks:/data \
  jstastny/m4baker -j 4
```

## Features

- **Multi-format** — MP3, M4A, OGG, OPUS, WMA, FLAC, WAV, AAC
- **AI metadata** — optional Claude API for author/title extraction
- **Chapters** — from `bookinfo.html`, playlists (`.pls`, `.m3u`), tags, or filenames
- **Cover art** — from directory images, embedded art, or online (Open Library, Google Books)
- **Reentrant** — skips already converted books
- **ZIP support** — extracts and processes ZIP archives

## Running without Docker

Requires Python 3.10+ and ffmpeg/ffprobe. Optional: `tqdm` for progress bars.

```bash
python3 convert_to_m4b.py /path/to/audiobooks
python3 convert_to_m4b.py --claude-api-key sk-ant-... -j 4 -n /path/to/audiobooks
```

## Building locally

```bash
docker build -t m4baker .
docker run --rm -v /path/to/audiobooks:/data m4baker
```
