# spoken

Convert audiobook collections (MP3 folders / ZIP archives) to M4B files with chapters, metadata, and cover art.

## Features

- Scans for audiobooks as ZIP archives or directory trees with MP3 files
- Extracts metadata from `bookinfo.html`, MP3 tags, or folder structure
- Respects track order from playlists (`.pls`, `.m3u`, `.m3u8`) or `bookinfo.html`
- Embeds chapter markers, cover art, and metadata (author, title, narrator)
- Reentrant — already converted books are skipped on re-run
- Parallel processing with `-j`

## Requirements

- Python 3.10+
- ffmpeg / ffprobe

No pip dependencies.

## Usage

```bash
python3 convert_to_m4b.py /path/to/audiobooks
python3 convert_to_m4b.py -j 4 /path/to/audiobooks
```

Output goes to `/path/to/audiobooks/m4b/` with filenames like `Author - Title.m4b`.

## Docker

```bash
docker build -t audiobook-m4b .
docker run --rm -v /path/to/audiobooks:/data audiobook-m4b
docker run --rm -v /path/to/audiobooks:/data audiobook-m4b -j 4
```

## Supported input layouts

```
audiobooks/
  some-book.zip                          # ZIP with MP3s + optional bookinfo.html
  Author Name/
    Series Name/
      1. Book Title/
        track-01.mp3
        track-02.mp3
      2. Another Book/
        ...
```

The script detects the folder depth and derives author, series, and book title accordingly.
