#!/usr/bin/env python3
"""
Convert audiobook collections (MP3 folders / ZIP archives) to M4B files
with chapters, metadata, and cover art.

Usage:
    python3 convert_to_m4b.py [directory]

Scans <directory> (default: current dir) for audiobooks stored as:
  - ZIP archives containing MP3s (with optional bookinfo.html / playlist.pls)
  - Folder trees whose leaf directories contain MP3 files
  - Folders with .m3u/.m3u8/.pls playlists (used for track ordering)

Outputs to <directory>/m4b/ with filenames like "Author - Title.m4b".

Reentrant: books whose output .m4b already exists are skipped.

Requirements: ffmpeg, ffprobe (both on PATH)
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path

# ── constants ────────────────────────────────────────────────────────────────

AAC_BITRATE = "128k"
OUTPUT_SUBDIR = "m4b"
TEMP_SUBDIR = ".tmp"


# ── low-level helpers ────────────────────────────────────────────────────────


def ffprobe_json(filepath):
    """Run ffprobe on *filepath* and return the parsed JSON (or {})."""
    try:
        r = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                str(filepath),
            ],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            return json.loads(r.stdout)
    except Exception:
        pass
    return {}


def ffprobe_duration_ms(filepath):
    """Return duration of an audio file in milliseconds (0 on error)."""
    data = ffprobe_json(filepath)
    try:
        return int(float(data["format"]["duration"]) * 1000)
    except (KeyError, ValueError, TypeError):
        return 0


def ffprobe_tags(filepath):
    """Return the format-level tag dict of an audio file."""
    return ffprobe_json(filepath).get("format", {}).get("tags", {})


def file_has_cover_stream(filepath):
    """True if the file has an embedded picture (video stream)."""
    for s in ffprobe_json(filepath).get("streams", []):
        if s.get("codec_type") == "video":
            return True
    return False


def extract_cover_from_mp3(mp3_path, out_path):
    """Try to extract an embedded cover image to *out_path*. Return True on success."""
    r = subprocess.run(
        ["ffmpeg", "-y", "-v", "quiet", "-i", str(mp3_path),
         "-an", "-vcodec", "copy", str(out_path)],
        capture_output=True,
    )
    if r.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
        return True
    out_path.unlink(missing_ok=True)
    return False


def find_largest_image(directory):
    """Find the largest .jpg/.jpeg/.png in *directory*. Returns Path or None."""
    candidates = []
    for ext in ("*.jpg", "*.jpeg", "*.png", "*.JPG", "*.JPEG", "*.PNG"):
        candidates.extend(Path(directory).glob(ext))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_size)


def sanitize_filename(name):
    """Strip characters that are unsafe in filenames."""
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    return name.strip(". ")


def strip_number_prefix(name):
    """'13. Foo' → 'Foo',  '2, Bar' → 'Bar'."""
    return re.sub(r"^\d+[.,]\s*", "", name).strip()


def extract_number_prefix(name):
    """'13. Foo' → 13,  'Foo' → None."""
    m = re.match(r"^(\d+)[.,]", name)
    return int(m.group(1)) if m else None


def format_duration(ms):
    """Milliseconds → '5h 23m' / '23m 10s'."""
    total_s = ms // 1000
    h, remainder = divmod(total_s, 3600)
    m, s = divmod(remainder, 60)
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s"


# ── playlist parsing ─────────────────────────────────────────────────────────


def parse_pls(text):
    """
    Parse a PLS playlist.  Returns a list of dicts with keys
    'file' and optionally 'title'.
    """
    entries = {}  # number → dict
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r"(?i)File(\d+)\s*=\s*(.*)", line)
        if m:
            n = int(m.group(1))
            entries.setdefault(n, {})["file"] = m.group(2).strip()
            continue
        m = re.match(r"(?i)Title(\d+)\s*=\s*(.*)", line)
        if m:
            n = int(m.group(1))
            entries.setdefault(n, {})["title"] = m.group(2).strip()
    return [entries[k] for k in sorted(entries)]


def parse_m3u(text):
    """
    Parse an M3U/M3U8 playlist.  Returns a list of dicts with keys
    'file' and optionally 'title'.
    """
    entries = []
    pending_title = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.upper().startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF:"):
            # #EXTINF:duration,title
            parts = line.split(",", 1)
            if len(parts) == 2:
                pending_title = parts[1].strip()
            continue
        if line.startswith("#"):
            continue
        entry = {"file": line}
        if pending_title:
            entry["title"] = pending_title
            pending_title = None
        entries.append(entry)
    return entries


def find_and_parse_playlist(directory):
    """
    Look for a playlist file (.m3u, .m3u8, .pls) in *directory*.
    Returns a list of entry dicts or None.
    """
    directory = Path(directory)
    for pattern in ("*.m3u8", "*.m3u", "*.pls"):
        for pl_path in sorted(directory.glob(pattern)):
            try:
                text = pl_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if pattern == "*.pls":
                entries = parse_pls(text)
            else:
                entries = parse_m3u(text)
            if entries:
                return entries
    return None


# ── bookinfo.html parser ─────────────────────────────────────────────────────


class _BookInfoParser(HTMLParser):
    """Extract metadata from the provider's bookinfo.html."""

    def __init__(self):
        super().__init__()
        self._capture = None
        self.title = ""
        self.author = ""
        self.reader = ""
        self.cover_src = ""
        self.chapters = []  # list of {'title': ..., 'filename': ...}
        self._in_chapters = False
        self._cur = {}

    def handle_starttag(self, tag, attrs):
        ad = dict(attrs)
        aid = ad.get("id", "")
        cls = ad.get("class", "")

        if aid == "Title":
            self._capture = "title"
        elif aid == "Author":
            self._capture = "author"
        elif aid == "Reader":
            self._capture = "reader"
        elif aid == "Chapters":
            self._in_chapters = True

        if tag == "img" and "src" in ad and not self.cover_src:
            self.cover_src = ad["src"]

        if self._in_chapters:
            if tag == "h2" and cls and "ChapterTitle" in cls:
                self._capture = "ch_title"
            elif tag == "p":
                if cls and "Link" in cls:
                    self._capture = "ch_link"

    def handle_data(self, data):
        d = data.strip()
        if not d or not self._capture:
            return
        target = self._capture
        self._capture = None

        if target == "title":
            self.title = d
        elif target == "author":
            self.author = d
        elif target == "reader":
            self.reader = d
        elif target == "ch_title":
            self._cur["title"] = d
        elif target == "ch_link":
            self._cur["filename"] = d
            self.chapters.append(self._cur)
            self._cur = {}


def parse_bookinfo(html_text):
    """Parse bookinfo.html, return a _BookInfoParser with .title, .author, etc."""
    p = _BookInfoParser()
    try:
        p.feed(html_text)
    except Exception:
        pass
    return p


# ── chapter title heuristics (for folder books without bookinfo) ─────────────


def derive_chapter_titles(mp3_files):
    """
    Derive a list of human-friendly chapter titles for *mp3_files*.

    Strategy:
      1. Use title tags from the MP3s if they vary meaningfully across tracks.
      2. Use the differing suffix of the filenames if meaningful.
      3. Fall back to 'Chapter 01' … 'Chapter NN'.
    """
    n = len(mp3_files)

    # -- strategy 1: ID3 title tags --
    raw_titles = []
    for mp3 in mp3_files:
        t = ffprobe_tags(mp3).get("title", "").strip()
        raw_titles.append(t)

    stripped = [re.sub(r"^\s*\d+\s*", "", t).strip() for t in raw_titles]
    unique = set(s for s in stripped if s)
    if len(unique) > 1:
        # titles vary — clean leading track numbers and use them
        out = []
        for i, t in enumerate(raw_titles):
            cleaned = re.sub(r"^\s*\d+\s+", "", t).strip()
            out.append(cleaned or f"Chapter {i + 1:02d}")
        return out

    # -- strategy 2: filenames --
    stems = [mp3.stem for mp3 in mp3_files]
    prefix = os.path.commonprefix(stems)
    # trim prefix to the last separator-like character so we don't cut mid-word
    m = re.match(r"^(.*[\s\-_.,]).*$", prefix)
    if m:
        prefix = m.group(1)
    else:
        prefix = ""
    suffixes = [s[len(prefix):].strip(" -_.,") for s in stems]
    unique_suf = set(s for s in suffixes if s and not re.fullmatch(r"\d+", s))
    if len(unique_suf) > 1:
        out = []
        for i, suf in enumerate(suffixes):
            cleaned = re.sub(r"^\d+\s*[-–—]?\s*", "", suf).strip()
            out.append(cleaned or f"Chapter {i + 1:02d}")
        return out

    # -- strategy 3: plain numbering --
    return [f"Chapter {i + 1:02d}" for i in range(n)]


# ── FFMETADATA chapter file ──────────────────────────────────────────────────


def _ff_escape(value):
    """Escape a value for FFMETADATA1 format."""
    s = str(value)
    for ch in ("\\", "=", ";", "#", "\n"):
        s = s.replace(ch, f"\\{ch}")
    return s


def build_ffmetadata(chapters, meta):
    """
    Return a string in FFMETADATA1 format.

    *chapters*: list of {'title': str, 'duration_ms': int}
    *meta*:     dict of top-level tags (title, artist, album, …)
    """
    lines = [";FFMETADATA1"]
    for k, v in meta.items():
        if v:
            lines.append(f"{k}={_ff_escape(v)}")

    time_ms = 0
    for ch in chapters:
        start = time_ms
        end = time_ms + ch["duration_ms"]
        lines.append("[CHAPTER]")
        lines.append("TIMEBASE=1/1000")
        lines.append(f"START={start}")
        lines.append(f"END={end}")
        lines.append(f"title={_ff_escape(ch['title'])}")
        time_ms = end

    return "\n".join(lines) + "\n"


# ── core conversion ──────────────────────────────────────────────────────────


def convert_to_m4b(mp3_files, chapters, metadata, cover_path, output_path, temp_dir):
    """
    Merge *mp3_files* into a single M4B at *output_path*.

    Returns True on success.
    """
    # concat demuxer file list
    concat_file = temp_dir / "concat.txt"
    with open(concat_file, "w", encoding="utf-8") as fh:
        for mp3 in mp3_files:
            escaped = str(mp3).replace("'", "'\\''")
            fh.write(f"file '{escaped}'\n")

    # chapter / tag metadata file
    meta_tags = {
        "title": metadata.get("title", ""),
        "artist": metadata.get("author", ""),
        "album": metadata.get("title", ""),
        "album_artist": metadata.get("author", ""),
        "genre": "Audiobook",
    }
    if metadata.get("reader"):
        meta_tags["composer"] = metadata["reader"]
    meta_file = temp_dir / "metadata.txt"
    with open(meta_file, "w", encoding="utf-8") as fh:
        fh.write(build_ffmetadata(chapters, meta_tags))

    # assemble ffmpeg command
    cmd = [
        "ffmpeg", "-y", "-v", "warning",
        "-f", "concat", "-safe", "0", "-i", str(concat_file),
        "-i", str(meta_file),
    ]

    use_cover = cover_path and Path(cover_path).exists()
    if use_cover:
        cmd.extend(["-i", str(cover_path)])
        cmd.extend([
            "-map", "0:a", "-map", "2:v",
            "-c:v", "copy",
            "-disposition:v:0", "attached_pic",
        ])
    else:
        cmd.extend(["-map", "0:a"])

    cmd.extend([
        "-map_metadata", "1",
        "-c:a", "aac", "-b:a", AAC_BITRATE,
        "-movflags", "+faststart",
        str(output_path),
    ])

    total_ms = sum(ch["duration_ms"] for ch in chapters)
    print(f"  Encoding {len(mp3_files)} tracks ({format_duration(total_ms)}) ...")

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print("  ERROR: ffmpeg failed:")
        for line in r.stderr.strip().splitlines()[-15:]:
            print(f"    {line}")
        return False
    return True


# ── discovery ─────────────────────────────────────────────────────────────────


def discover_books(base_dir):
    """
    Walk *base_dir* and return a list of ``('zip', path)`` and
    ``('folder', path)`` tuples representing individual audiobooks.
    """
    base_dir = Path(base_dir)
    skip = {OUTPUT_SUBDIR, TEMP_SUBDIR}
    books = []

    # zip archives at the top level
    for f in sorted(base_dir.iterdir()):
        if f.is_file() and f.suffix.lower() == ".zip":
            books.append(("zip", f))

    # folder books — leaf directories that contain mp3 files
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = sorted(d for d in dirs if d not in skip)
        rp = Path(root)
        mp3s = [f for f in files if f.lower().endswith(".mp3")]
        if mp3s:
            books.append(("folder", rp))
            dirs.clear()  # don't descend further

    return books


# ── online cover lookup ───────────────────────────────────────────────────────

# Minimum image size in bytes to accept (rejects 1x1 pixel placeholders etc.)
_MIN_COVER_BYTES = 1000
_HTTP_TIMEOUT = 15


def _download_url(url, output_path):
    """Download *url* to *output_path*.  Return True on success."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "m4baker/1.0"})
        resp = urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT)
        data = resp.read()
        if len(data) < _MIN_COVER_BYTES:
            return False
        with open(output_path, "wb") as fh:
            fh.write(data)
        return True
    except Exception:
        return False


def fetch_cover_google_books(title, author, output_path):
    """
    Search Google Books by title + author and download the best cover.
    Returns True on success.
    """
    try:
        q = f"{title} {author}" if author else title
        params = urllib.parse.urlencode({"q": q, "maxResults": "5"})
        url = f"https://www.googleapis.com/books/v1/volumes?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "m4baker/1.0"})
        resp = urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT)
        data = json.loads(resp.read())

        for item in data.get("items", []):
            links = item.get("volumeInfo", {}).get("imageLinks", {})
            thumb = links.get("thumbnail") or links.get("smallThumbnail")
            if not thumb:
                continue
            # request highest available zoom
            img_url = re.sub(r"zoom=\d", "zoom=4", thumb)
            if _download_url(img_url, output_path):
                return True
    except Exception:
        pass
    return False


def fetch_cover_openlibrary(isbn, output_path):
    """
    Try to fetch a cover image from Open Library by ISBN.
    Returns True on success.
    """
    if not isbn:
        return False
    url = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
    return _download_url(url, output_path)


def fetch_cover_online(title, author, output_path, isbn=None):
    """
    Try to find a cover image online.  Tries Open Library (by ISBN)
    then Google Books (by title/author).  Returns True on success.
    """
    if fetch_cover_openlibrary(isbn, output_path):
        print("  Cover: found via Open Library (ISBN)")
        return True
    if fetch_cover_google_books(title, author, output_path):
        print("  Cover: found via Google Books")
        return True
    return False


# ── resolving the best cover image ────────────────────────────────────────────


def resolve_cover(directory, mp3_files, bookinfo_cover_src=None,
                  title=None, author=None, isbn=None):
    """
    Return a Path to the best cover image we can find, or None.

    Priority:
      1. The file referenced in bookinfo.html (if present and exists).
      2. The largest .jpg/.png in the directory.
      3. An image extracted from the first MP3 that has one embedded.
      4. Online lookup (Open Library by ISBN, then Google Books by title/author).
    """
    directory = Path(directory)

    # 1 – bookinfo reference
    if bookinfo_cover_src:
        p = directory / bookinfo_cover_src
        if p.exists():
            return p
        # maybe just the name without path prefix
        candidates = list(directory.rglob(Path(bookinfo_cover_src).name))
        if candidates:
            return candidates[0]

    # 2 – largest image file sitting next to the MP3s
    img = find_largest_image(directory)
    if img:
        return img

    # 3 – extract from the first MP3 with an embedded cover
    for mp3 in mp3_files[:3]:  # check a few in case the first has none
        if file_has_cover_stream(mp3):
            out = directory / ".cover_extracted.jpg"
            if extract_cover_from_mp3(mp3, out):
                return out

    # 4 – online lookup as last resort
    if title:
        out = directory / ".cover_online.jpg"
        if fetch_cover_online(title, author or "", out, isbn=isbn):
            return out

    return None


# ── normalise author name ─────────────────────────────────────────────────────


def normalise_author(raw):
    """'Flanagan, John' → 'John Flanagan'; already-normal names pass through."""
    raw = raw.strip()
    if "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return raw


# ── process a ZIP audiobook ──────────────────────────────────────────────────


def process_zip(zip_path, output_dir, temp_base):
    print(f"\n{'=' * 60}")
    print(f"ZIP: {zip_path.name}")

    temp_dir = temp_base / zip_path.stem
    extract_dir = temp_dir / "data"

    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

            # ── read bookinfo.html (optional) ──
            bookinfo = None
            info = None
            for n in names:
                if os.path.basename(n).lower() == "bookinfo.html":
                    bookinfo = zf.read(n).decode("utf-8", errors="replace")
                    break
            if bookinfo:
                info = parse_bookinfo(bookinfo)

            # ── read playlist (optional) ──
            playlist_entries = None
            for n in names:
                bn = os.path.basename(n).lower()
                if bn.endswith(".pls"):
                    text = zf.read(n).decode("utf-8", errors="replace")
                    playlist_entries = parse_pls(text)
                    break
                if bn.endswith((".m3u", ".m3u8")):
                    text = zf.read(n).decode("utf-8", errors="replace")
                    playlist_entries = parse_m3u(text)
                    break

            # ── metadata ──
            author = (info.author if info else "") or ""
            title = (info.title if info else "") or ""
            reader = (info.reader if info else "") or ""
            cover_src = (info.cover_src if info else "") or ""

            if not title:
                title = zip_path.stem.replace("-", " ").strip().title()

            # ── extract ──
            print("  Extracting ...")
            extract_dir.mkdir(parents=True, exist_ok=True)
            zf.extractall(extract_dir)

        # ── order MP3 files ──
        all_mp3s = sorted(extract_dir.rglob("*.mp3"))
        mp3_by_name = {p.name: p for p in all_mp3s}

        mp3_files = None

        # try bookinfo chapter order first
        if info and info.chapters:
            ordered = [mp3_by_name[ch["filename"]]
                       for ch in info.chapters
                       if ch.get("filename") in mp3_by_name]
            if ordered:
                mp3_files = ordered

        # then playlist order
        if not mp3_files and playlist_entries:
            ordered = [mp3_by_name[e["file"]]
                       for e in playlist_entries
                       if e.get("file") in mp3_by_name]
            if ordered:
                mp3_files = ordered

        # fallback: sorted glob
        if not mp3_files:
            mp3_files = all_mp3s

        if not mp3_files:
            print("  WARNING: no MP3 files found — skipping")
            return

        # ── fallback author from tags ──
        if not author:
            tags = ffprobe_tags(mp3_files[0])
            author = normalise_author(
                tags.get("artist") or tags.get("album_artist") or ""
            )
        if not author:
            author = "Unknown"
        else:
            author = normalise_author(author)

        output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
        output_path = output_dir / output_name
        if output_path.exists():
            print(f"  SKIP (already exists): {output_name}")
            return

        # ── chapter titles + durations ──
        # prefer bookinfo titles, then playlist titles, then derive heuristically
        if info and info.chapters and len(info.chapters) == len(mp3_files):
            ch_titles = [
                re.sub(r"^\d+\s+", "", ch.get("title", "")).strip()
                for ch in info.chapters
            ]
        elif playlist_entries and len(playlist_entries) == len(mp3_files):
            ch_titles = [
                re.sub(r"^\d+\s+", "", e.get("title", "")).strip()
                for e in playlist_entries
            ]
        else:
            ch_titles = derive_chapter_titles(mp3_files)

        chapters = []
        for i, mp3 in enumerate(mp3_files):
            dur = ffprobe_duration_ms(mp3)
            t = ch_titles[i] if i < len(ch_titles) and ch_titles[i] else f"Chapter {i + 1:02d}"
            chapters.append({"title": t, "duration_ms": dur})

        # ── cover ──
        isbn = ffprobe_tags(mp3_files[0]).get("ISBN", "") if mp3_files else ""
        cover_path = resolve_cover(
            extract_dir, mp3_files, cover_src,
            title=title, author=author, isbn=isbn,
        )

        # ── convert ──
        metadata = {"title": title, "author": author, "reader": reader}
        output_dir.mkdir(parents=True, exist_ok=True)
        tmp_out = temp_dir / "output.m4b"
        if convert_to_m4b(mp3_files, chapters, metadata, cover_path, tmp_out, temp_dir):
            shutil.move(str(tmp_out), str(output_path))
            size_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"  DONE: {output_name} ({size_mb:.1f} MB)")
        else:
            print(f"  FAILED: {output_name}")

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


# ── process a folder audiobook ────────────────────────────────────────────────


def process_folder(folder_path, base_dir, output_dir, temp_base):
    rel = folder_path.relative_to(base_dir)
    print(f"\n{'=' * 60}")
    print(f"Folder: {rel}")

    parts = list(rel.parts)
    mp3_files = sorted(folder_path.glob("*.mp3"))
    if not mp3_files:
        print("  WARNING: no MP3 files — skipping")
        return

    # ── check for a playlist and reorder if found ──
    playlist_entries = find_and_parse_playlist(folder_path)
    if playlist_entries:
        mp3_by_name = {p.name: p for p in mp3_files}
        ordered = [mp3_by_name[e["file"]]
                   for e in playlist_entries
                   if e.get("file") in mp3_by_name]
        if ordered:
            mp3_files = ordered

    # ── determine author + title from folder structure ──
    author = ""
    title = ""

    if len(parts) >= 3:
        # Author / Series / Book
        author = parts[0]
        series = strip_number_prefix(parts[1])
        book = strip_number_prefix(parts[2])
        num = extract_number_prefix(parts[2])
        if num is not None:
            title = f"{series} {num:02d} - {book}"
        else:
            title = f"{series} - {book}"
    elif len(parts) == 2:
        author = parts[0]
        title = strip_number_prefix(parts[1])
    elif len(parts) == 1:
        title = strip_number_prefix(parts[0]) or parts[0]
    else:
        title = folder_path.name

    # fallback author from tags
    if not author:
        tags = ffprobe_tags(mp3_files[0])
        author = normalise_author(
            tags.get("artist") or tags.get("album_artist") or ""
        )
    if not author:
        author = "Unknown"

    output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
    output_path = output_dir / output_name
    if output_path.exists():
        print(f"  SKIP (already exists): {output_name}")
        return

    temp_dir = temp_base / sanitize_filename(str(rel).replace(os.sep, "_"))
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ── chapter titles ──
        if playlist_entries and len(playlist_entries) == len(mp3_files):
            ch_titles_raw = [e.get("title", "").strip() for e in playlist_entries]
            # only use if they carry meaningful info
            unique = set(re.sub(r"^\d+\s*", "", t).strip() for t in ch_titles_raw if t)
            if len(unique) > 1:
                ch_titles = [re.sub(r"^\d+\s+", "", t).strip() for t in ch_titles_raw]
            else:
                ch_titles = derive_chapter_titles(mp3_files)
        else:
            ch_titles = derive_chapter_titles(mp3_files)

        chapters = []
        for i, mp3 in enumerate(mp3_files):
            dur = ffprobe_duration_ms(mp3)
            t = ch_titles[i] if i < len(ch_titles) and ch_titles[i] else f"Chapter {i + 1:02d}"
            chapters.append({"title": t, "duration_ms": dur})

        # ── cover ──
        tags = ffprobe_tags(mp3_files[0])
        isbn = tags.get("ISBN", "")
        cover_path = resolve_cover(
            folder_path, mp3_files,
            title=title, author=author, isbn=isbn,
        )

        # ── narrator from tags ──
        reader = ""
        comment = tags.get("comment", "")
        m = re.search(r"[Čč]te:\s*(.*)", comment)
        if m:
            reader = m.group(1).strip()

        # ── convert ──
        metadata = {"title": title, "author": author, "reader": reader}
        output_dir.mkdir(parents=True, exist_ok=True)
        tmp_out = temp_dir / "output.m4b"
        if convert_to_m4b(mp3_files, chapters, metadata, cover_path, tmp_out, temp_dir):
            shutil.move(str(tmp_out), str(output_path))
            size_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"  DONE: {output_name} ({size_mb:.1f} MB)")
        else:
            print(f"  FAILED: {output_name}")

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


# ── main ──────────────────────────────────────────────────────────────────────


def _process_one(btype, bpath, base_dir, output_dir, temp_base):
    """Process a single book. Designed to be called from a thread pool."""
    try:
        if btype == "zip":
            process_zip(bpath, output_dir, temp_base)
        else:
            process_folder(bpath, base_dir, output_dir, temp_base)
    except Exception as exc:
        print(f"  ERROR ({bpath}): {exc}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert audiobook MP3 collections to M4B files."
    )
    parser.add_argument(
        "directory", nargs="?", default=".",
        help="Root directory to scan (default: current dir)",
    )
    parser.add_argument(
        "-j", "--jobs", type=int, default=1,
        help="Number of books to process in parallel (default: 1)",
    )
    args = parser.parse_args()

    base_dir = Path(args.directory).resolve()
    output_dir = base_dir / OUTPUT_SUBDIR
    temp_base = base_dir / TEMP_SUBDIR
    jobs = max(1, args.jobs)

    # preflight
    for tool in ("ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            print(f"ERROR: '{tool}' not found on PATH")
            sys.exit(1)

    print(f"Source:  {base_dir}")
    print(f"Output:  {output_dir}")
    print(f"Jobs:    {jobs}")

    books = discover_books(base_dir)
    n_zip = sum(1 for t, _ in books if t == "zip")
    n_dir = sum(1 for t, _ in books if t == "folder")
    print(f"Found {n_zip} zip archive(s), {n_dir} folder book(s)  ({len(books)} total)")

    if jobs == 1:
        for btype, bpath in books:
            _process_one(btype, bpath, base_dir, output_dir, temp_base)
    else:
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            futures = [
                pool.submit(_process_one, btype, bpath, base_dir, output_dir, temp_base)
                for btype, bpath in books
            ]
            for f in as_completed(futures):
                f.result()  # surfaces any uncaught exceptions

    # clean up temp root if empty
    if temp_base.exists():
        try:
            temp_base.rmdir()
        except OSError:
            pass

    print(f"\n{'=' * 60}")
    print("All done.")


if __name__ == "__main__":
    main()
