#!/usr/bin/env python3
"""
Convert audiobook collections (MP3 folders / ZIP archives) to M4B files
with chapters, metadata, and cover art.

Usage:
    python3 convert_to_m4b.py [directory]
    python3 convert_to_m4b.py --dry-run /path/to/audiobooks
    python3 convert_to_m4b.py -j 4 /path/to/audiobooks

Scans <directory> (default: current dir) for audiobooks stored as:
  - ZIP archives containing audio files (with optional bookinfo.html / playlist.pls)
  - Folder trees whose leaf directories contain audio files
    (MP3, M4A, OGG, OPUS, WMA, FLAC, WAV, AAC)
  - Folders with .m3u/.m3u8/.pls playlists (used for track ordering)

Outputs to <directory>/m4b/ with filenames like "Author - Title.m4b".

Reentrant: books whose output .m4b already exists are skipped.

Requirements: ffmpeg, ffprobe (both on PATH)
Optional:     tqdm (pip install tqdm) for progress bars
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# ── constants ────────────────────────────────────────────────────────────────

AAC_BITRATE = "128k"
OUTPUT_SUBDIR = "m4b"
TEMP_SUBDIR = ".tmp"
AUDIO_EXTENSIONS = {".mp3", ".m4a", ".m4b", ".ogg", ".oga", ".opus",
                    ".wma", ".flac", ".wav", ".aac"}


def is_audio_file(name):
    """Return True if *name* (str or Path) has a recognised audio extension."""
    return Path(name).suffix.lower() in AUDIO_EXTENSIONS

# ── tqdm-safe logging ────────────────────────────────────────────────────────

_progress_bar = None


def log(msg):
    """Print a message. Uses tqdm.write() when a progress bar is active."""
    if _progress_bar is not None:
        _progress_bar.write(msg)
    else:
        print(msg)


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
    """Return the format-level tag dict of an audio file (charset-repaired)."""
    raw = ffprobe_json(filepath).get("format", {}).get("tags", {})
    return {k: _fix_mojibake(v) if isinstance(v, str) else v
            for k, v in raw.items()}


# Characters that appear in CP1250-as-Latin1 mojibake but are unusual in
# normal Czech/Slovak UTF-8 text.  If any are present the string is very
# likely mojibake and we try to recover it via Latin-1 → CP1250.
# Mappings: ř→ø ů→ù č→è Č→È ě→ì ž→\x9e š→\x9a ň→ò ď→ï ť→\x9d
_MOJIBAKE_PRINTABLE = set("øùèÈìòþÞ¹©ï»")
# C1 control characters (0x80-0x9F) — in CP1250 these map to useful chars
# like š(0x9A), ž(0x9E), ť(0x9D), etc. Their presence in text is a strong
# signal of CP1250 mojibake since valid UTF-8 never produces bare C1 chars.
_MOJIBAKE_C1_RANGE = range(0x80, 0xA0)


def _looks_like_mojibake(text):
    """Heuristic: does the text contain CP1250-as-Latin1 artifacts?"""
    for ch in text:
        if ch in _MOJIBAKE_PRINTABLE or ord(ch) in _MOJIBAKE_C1_RANGE:
            return True
    return False


def _fix_mojibake(text):
    """
    Detect and repair CP1250-encoded text that was decoded as Latin-1.

    Many older Czech audiobook MP3s have ID3v1 tags encoded in Windows-1250
    but the tagger (or ffprobe) interprets them as Latin-1, producing
    garbled diacritics: ř→ø, ů→ù, č→è, ě→ì, ž→\x9E (C1 control), etc.
    """
    if not text or not _looks_like_mojibake(text):
        return text
    try:
        # Re-encode as Latin-1 to get the original CP1250 bytes, then decode
        recovered = text.encode("latin-1").decode("cp1250")
        # Basic sanity: recovered text should not contain control chars
        if not any(ord(c) < 32 for c in recovered if c not in "\n\r\t"):
            return recovered
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return text


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


def normalise_author(raw):
    """'Flanagan, John' → 'John Flanagan'; already-normal names pass through."""
    raw = raw.strip()
    if "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
    return raw


def _normalise_for_compare(s):
    """Lower-case, collapse whitespace/punctuation for fuzzy title comparison."""
    return re.sub(r"[\s\-_.,:;!?]+", " ", s.lower()).strip()


def parse_folder_name(name):
    """
    Try to extract (author, title) from a folder/file name.

    Recognised patterns (in order):
      - ``"Author__Title"``         (double underscore — unambiguous)
      - ``"Author - Title"``        (dash separator — also ``_-_``)
      - ``"Author, First - Title"`` (comma in author part before the dash)
      - ``"Author: Title"``         (colon separator)

    Returns ``("", "")`` when no pattern matches.
    The author is passed through :func:`normalise_author`.
    Underscores are replaced with spaces in both parts.
    """
    name = name.strip()

    # Try "Author__Title" first (double underscore — unambiguous)
    if "__" in name:
        parts = name.split("__", 1)
        a = normalise_author(parts[0].replace("_", " ").strip())
        t = parts[1].replace("_", " ").strip()
        return a, t

    # Normalise underscores to spaces for dash/colon patterns
    normalised = name.replace("_", " ").strip()

    # Try dash separator.  When there are multiple " - " segments, use the
    # *last* one as the split point so hyphenated surnames like
    # "Adler - Olsen J. - Složka 64" → author "Adler - Olsen J.", title "Složka 64".
    # But also handle parenthetical suffixes like "(Bareš)" as part of the title.
    if " - " in normalised:
        idx = normalised.rfind(" - ")
        raw_a = normalised[:idx].strip()
        raw_t = normalised[idx + 3:].strip()
        if raw_a and raw_t:
            a = normalise_author(raw_a)
            return a, raw_t

    # Try "Author: Title" in name
    if ": " in normalised:
        parts = normalised.split(": ", 1)
        a = normalise_author(parts[0].strip())
        t = parts[1].strip()
        if a and t:
            return a, t

    return "", ""


def _is_audiobook_genre(tags):
    """True if the genre tag indicates an audiobook."""
    genre = tags.get("genre", "").lower()
    return any(kw in genre for kw in (
        "audiobook", "audiokniha", "knihy", "mluvené", "book", "spoken",
        "povídka", "roman", "próza",
    ))


def extract_book_metadata(tags, folder_name=""):
    """
    Extract *(author, title)* from audio file tags.

    Audiobook tags are notoriously inconsistent — the ``artist`` field may
    hold the author **or** the narrator depending on the provider.  The
    ``album`` field is typically the best source for the book title, but
    sometimes embeds the author too (``"Author: Title"``).

    *folder_name* (the leaf directory name, stripped of numbering) is used
    as a title fallback, for ``album`` pattern validation, and as a last-
    resort source of author/title via :func:`parse_folder_name`.

    Returns ``(author, title)`` — either may be ``""`` if nothing useful
    was found.
    """
    album = tags.get("album", "").strip()
    artist = tags.get("artist", "").strip()
    album_artist = tags.get("album_artist", "").strip()
    composer = tags.get("composer", "").strip()
    tag_title = tags.get("title", "").strip()
    comment = tags.get("comment", "")

    author = ""
    title = album  # default: whole album tag is the title

    # ── Strategy 1a: album "Author: Title" pattern ──
    #   e.g. "Filip Rožek: GUMP - Jsme dvojka"
    if ": " in album:
        candidate_author, candidate_title = album.split(": ", 1)
        candidate_author = candidate_author.strip()
        candidate_title = candidate_title.strip()
        if candidate_author and candidate_title and folder_name:
            ct = _normalise_for_compare(candidate_title)
            fn = _normalise_for_compare(folder_name)
            min_len = min(len(ct), len(fn), 8)
            if min_len and ct[:min_len] == fn[:min_len]:
                author = candidate_author
                title = candidate_title

    # ── Strategy 1b: title tag "Author: Title" pattern ──
    #   e.g. title = "Vegard Steiro Amundsen: Made in Norway"
    if not author and not album and ": " in tag_title:
        candidate_author, candidate_title = tag_title.split(": ", 1)
        candidate_author = candidate_author.strip()
        candidate_title = candidate_title.strip()
        if candidate_author and candidate_title:
            author = normalise_author(candidate_author)
            title = candidate_title

    # ── Strategy 2: narrator detection → prefer composer as author ──
    if not author:
        narrator = ""
        m = re.search(r"[Čč]te:\s*(.*)", comment)
        if m:
            narrator = m.group(1).strip()

        if narrator and artist:
            na = _normalise_for_compare(narrator)
            ar = _normalise_for_compare(artist)
            if na == ar:
                # artist IS the narrator — try composer instead
                author = normalise_author(composer) if composer else ""
            else:
                author = normalise_author(artist)
        elif composer and artist and composer != artist:
            # When composer differs from artist — on audiobooks the composer
            # is often the book author while artist is the narrator.
            # Only prefer composer when the genre explicitly says audiobook;
            # otherwise fall through to the normal artist-first logic, because
            # many music-tagged audiobooks use artist=author, composer=narrator.
            if _is_audiobook_genre(tags):
                author = normalise_author(composer)
            else:
                author = normalise_author(artist)
        else:
            # Prefer artist; when empty, try composer before album_artist
            # (album_artist on audiobooks is frequently the narrator)
            raw = artist or composer or album_artist
            author = normalise_author(raw) if raw else ""

    # ── Strategy 3: folder name parsing ──
    #   When tags are useless (empty, hashes, no metadata), try to extract
    #   author/title from the folder or parent folder name.
    if not author and folder_name:
        fn_author, fn_title = parse_folder_name(folder_name)
        if fn_author:
            author = fn_author
        if fn_title and not title:
            title = fn_title

    # ── Strategy 3b: folder name *is* an author name ──
    #   e.g. "Asimov, Isaac" — a "Last, First" pattern with no title part.
    #   Use it as author and try to derive title from the title tag or filename.
    if not author and folder_name and "," in folder_name:
        parts = folder_name.split(",", 1)
        if len(parts) == 2 and parts[1].strip():
            # Looks like "Last, First" — use as author
            author = normalise_author(folder_name)
            # Try to extract a title from the title tag (strip the author
            # name and generic words like "AUDIOKNIHA")
            if tag_title and not title:
                # Strip author name variants from the title tag
                t = tag_title
                for pattern in [
                    re.escape(author), re.escape(folder_name),
                    re.escape(artist), re.escape(folder_name.replace(",", "")),
                ]:
                    if pattern:
                        t = re.sub(pattern, "", t, flags=re.IGNORECASE).strip()
                t = re.sub(r"(?i)\baudiokniha\b", "", t).strip(" .,;:-")
                if t:
                    title = t

    # ── Title fallback ──
    # If the tag-derived title looks like a radio station abbreviation
    # (very short, mostly uppercase) and the folder name is more
    # descriptive, prefer the folder name.
    if not title:
        title = folder_name
    elif (folder_name and len(title) <= 4
          and sum(1 for c in title if c.isupper()) >= len(title) // 2
          and len(folder_name) > len(title) * 2):
        title = folder_name

    return author, title


# ── AI-assisted metadata extraction ──────────────────────────────────────────

_CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
# Default models to try, cheapest first.  The first model that responds
# successfully is used for all subsequent calls.  Haiku is ~12x cheaper
# than Sonnet but may not be available on all account tiers.
_CLAUDE_DEFAULT_MODELS = ["claude-haiku-4-5-20251001", "claude-sonnet-4-20250514"]
_claude_models_to_try = list(_CLAUDE_DEFAULT_MODELS)
_claude_model = None  # resolved on first successful call

# Module-level API key — set by main() from CLI args / env var.
_claude_api_key = None
_claude_api_warned = False


def _call_claude(prompt, api_key):
    """
    Send a single prompt to the Claude API. Returns the text response
    or ``""`` on any failure.  Uses only stdlib (urllib).

    On the first call, tries models from :data:`_CLAUDE_MODELS` in order
    (cheapest first) and remembers which one works.
    """
    global _claude_model, _claude_api_warned

    models_to_try = [_claude_model] if _claude_model else _claude_models_to_try

    for model in models_to_try:
        payload = json.dumps({
            "model": model,
            "max_tokens": 256,
            "messages": [{"role": "user", "content": prompt}],
        }).encode()

        req = urllib.request.Request(
            _CLAUDE_API_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read())
                if not _claude_model:
                    _claude_model = model
                    log(f"  AI model: {model}")
                return body["content"][0]["text"]
        except urllib.error.HTTPError as e:
            try:
                err_body = json.loads(e.read())
                err_msg = err_body.get("error", {}).get("message", "")
                err_type = err_body.get("error", {}).get("type", "")
            except Exception:
                err_msg, err_type = str(e), ""
            # "not_found_error" means the model isn't available on this
            # tier — try the next one.
            if err_type == "not_found_error" and not _claude_model:
                continue
            if not _claude_api_warned:
                log(f"  WARNING: Claude API error: {err_msg}")
                log("  Falling back to heuristic metadata extraction.")
                _claude_api_warned = True
            return ""
        except Exception:
            return ""

    if not _claude_api_warned:
        log("  WARNING: No Claude model available on this account tier.")
        log("  Falling back to heuristic metadata extraction.")
        _claude_api_warned = True
    return ""


def _ai_judge_metadata(candidates, tags, folder_name="", parent_name="",
                        filenames=None, api_key=None):
    """
    Feed all collected metadata candidates to Claude and let it pick
    the best author and title.  Returns ``(author, title)`` or
    ``("", "")`` on failure.
    """
    if not api_key:
        return "", ""

    # Build context: raw signals + heuristic candidates
    parts = []

    # 1 — heuristic candidates
    if candidates:
        parts.append("Heuristic candidates (our best guesses so far):\n" +
                      "\n".join(f"  {k}: {v}" for k, v in candidates.items()
                                if v))

    # 2 — raw tags
    if tags:
        useful = {k: v for k, v in tags.items()
                  if k.lower() in (
                      "title", "album", "artist", "album_artist", "composer",
                      "genre", "comment", "performer", "date", "encoded_by",
                      "TSOC", "artist-sort", "TSO2",
                  ) and v and v.strip()}
        if useful:
            parts.append("Raw audio file tags:\n" + "\n".join(
                f"  {k}: {v}" for k, v in useful.items()))

    # 3 — folder/file context
    if folder_name:
        parts.append(f"Folder name (leaf): {folder_name}")
    if parent_name:
        parts.append(f"Parent folder: {parent_name}")
    if filenames:
        sample = filenames[:5]
        parts.append("Audio filenames (first 5):\n" + "\n".join(
            f"  {f}" for f in sample))
        if len(filenames) > 5:
            parts.append(f"  ... ({len(filenames)} files total)")

    if not parts:
        return "", ""

    context = "\n".join(parts)

    prompt = f"""You are reviewing metadata for a Czech audiobook. Below you have raw signals (audio file tags, folder names, filenames) AND heuristic candidates that our code already extracted.

Your job: decide the correct AUTHOR (the writer of the book) and TITLE. Use ONLY the information below — do NOT substitute names from your world knowledge.

Rules:
- The heuristic candidates are often correct — confirm or fix them.
- In audiobooks, "artist" and "album_artist" are OFTEN the narrator, not the author. "composer" is often the author. But not always.
- The "album" tag is usually the book title. It sometimes contains "Author: Title".
- Folder names often follow "Last, First - Title" or "Author__Title" patterns.
- If only one person name appears across all fields, that person is the author.
- Fix obvious Czech diacritics (e.g. "Mechanicky pomeranc" → "Mechanický pomeranč").
- Keep titles in Czech. Do NOT translate to English.
- Return author as "Firstname Lastname" (not "Lastname, Firstname").

{context}

Respond with ONLY a JSON object:
{{"author": "...", "title": "..."}}"""

    text = _call_claude(prompt, api_key)
    if not text:
        return "", ""

    try:
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        data = json.loads(text.strip())
        author = data.get("author", "").strip()
        title = data.get("title", "").strip()
        return author, title
    except (json.JSONDecodeError, AttributeError):
        return "", ""


def smart_extract_metadata(tags, folder_name="", parent_name="",
                           filenames=None):
    """
    Extract (author, title) by running heuristics first, then optionally
    passing all collected signals to Claude for final judgement.
    """
    # Step 1: Run heuristics — always
    h_author, h_title = extract_book_metadata(tags, folder_name=folder_name)
    if not h_author and parent_name and "," in parent_name:
        h_author = normalise_author(parent_name)

    # Also parse folder name for an alternative candidate
    fn_author, fn_title = parse_folder_name(folder_name) if folder_name else ("", "")
    pn_author = normalise_author(parent_name) if parent_name and "," in parent_name else ""

    # Step 2: If AI is available, let it judge
    if _claude_api_key:
        candidates = {}
        if h_author:
            candidates["heuristic_author"] = h_author
        if h_title:
            candidates["heuristic_title"] = h_title
        if fn_author and fn_author != h_author:
            candidates["folder_name_author"] = fn_author
        if fn_title and fn_title != h_title:
            candidates["folder_name_title"] = fn_title
        if pn_author and pn_author != h_author:
            candidates["parent_folder_author"] = pn_author

        ai_author, ai_title = _ai_judge_metadata(
            candidates, tags, folder_name=folder_name,
            parent_name=parent_name, filenames=filenames,
            api_key=_claude_api_key,
        )
        if ai_author or ai_title:
            return ai_author or h_author, ai_title or h_title

    return h_author, h_title


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
    Merge audio files (*mp3_files*) into a single M4B at *output_path*.

    Accepts any audio format that ffmpeg can decode (MP3, M4A, OGG, etc.).
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
    log(f"  Encoding {len(mp3_files)} tracks ({format_duration(total_ms)}) ...")

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        log("  ERROR: ffmpeg failed:")
        for line in r.stderr.strip().splitlines()[-15:]:
            log(f"    {line}")
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

    # folder books — leaf directories that contain audio files
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = sorted(d for d in dirs if d not in skip)
        rp = Path(root)
        audio = [f for f in files if is_audio_file(f)]
        if audio:
            books.append(("folder", rp))
            dirs.clear()  # don't descend further

    return books


# ── planning (cheap metadata extraction for the pre-run summary) ─────────────


def plan_zip_book(zip_path, output_dir):
    """
    Cheaply determine output name, track count, and skip/convert status
    for a ZIP audiobook.  Only reads metadata from the zip — no extraction.
    """
    author = ""
    title = ""
    n_tracks = 0
    source_label = "ZIP"

    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            n_tracks = sum(1 for n in names if is_audio_file(n))

            for n in names:
                if os.path.basename(n).lower() == "bookinfo.html":
                    html = zf.read(n).decode("utf-8", errors="replace")
                    info = parse_bookinfo(html)
                    author = info.author
                    title = info.title
                    break
    except Exception:
        pass

    if not title:
        title = zip_path.stem.replace("-", " ").strip().title()
    author = normalise_author(author) if author else "Unknown"

    output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
    output_path = output_dir / output_name
    status = "skip" if output_path.exists() else "convert"

    return {
        "output_name": output_name,
        "status": status,
        "n_tracks": n_tracks,
        "source_label": source_label,
    }


def _author_from_parent(folder_path, base_dir):
    """
    If the folder sits inside an author-named parent directory, try to
    extract the author name from the parent.  Returns ``""`` on failure.

    Handles patterns like ``Backman, Frederik / Muz jmenem Ove`` where
    the parent folder is ``"Backman, Frederik"`` (i.e. ``Last, First``).
    """
    try:
        rel = folder_path.relative_to(base_dir)
    except ValueError:
        return ""
    parts = rel.parts
    if len(parts) < 2:
        return ""
    parent_name = parts[-2]  # immediate parent relative to base
    # Accept "Last, First" pattern as likely an author name
    if "," in parent_name:
        return normalise_author(parent_name)
    return ""


def _parent_folder_name(folder_path, base_dir):
    """Return the immediate parent folder name relative to base_dir, or ''."""
    try:
        parts = folder_path.relative_to(base_dir).parts
    except ValueError:
        return ""
    return parts[-2] if len(parts) >= 2 else ""


def plan_folder_book(folder_path, base_dir, output_dir):
    """
    Cheaply determine output name, track count, and skip/convert status
    for a folder audiobook.  Metadata comes from audio file tags; the leaf
    folder name is only used as a title fallback.
    """
    rel = folder_path.relative_to(base_dir)
    audio_files = sorted(f for f in folder_path.iterdir()
                         if f.is_file() and is_audio_file(f.name))
    n_tracks = len(audio_files)
    folder_name = strip_number_prefix(folder_path.name) or folder_path.name
    parent_name = _parent_folder_name(folder_path, base_dir)

    if audio_files:
        tags = ffprobe_tags(audio_files[0])
        filenames = [f.name for f in audio_files]
        author, title = smart_extract_metadata(
            tags, folder_name=folder_name, parent_name=parent_name,
            filenames=filenames,
        )
    else:
        author, title = "", folder_name

    if not author:
        author = "Unknown"
    if not title:
        title = folder_name

    output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
    output_path = output_dir / output_name
    status = "skip" if output_path.exists() else "convert"

    return {
        "output_name": output_name,
        "status": status,
        "n_tracks": n_tracks,
        "source_label": str(rel),
    }


def build_plan(books, base_dir, output_dir):
    """
    Return a list of plan dicts for all discovered books.
    Each dict has: btype, bpath, output_name, status, n_tracks, source_label.
    """
    plan = []
    for btype, bpath in books:
        if btype == "zip":
            info = plan_zip_book(bpath, output_dir)
        else:
            info = plan_folder_book(bpath, base_dir, output_dir)
        info["btype"] = btype
        info["bpath"] = bpath
        plan.append(info)
    return plan


def display_plan(plan):
    """Print a formatted plan table."""
    if not plan:
        print("Nothing found.")
        return

    # compute column widths
    max_name = max(len(p["output_name"]) for p in plan)
    max_name = min(max_name, 72)  # cap for very long names

    print(f"\nPlan ({len(plan)} books):\n")
    for p in plan:
        status_tag = "SKIP   " if p["status"] == "skip" else "CONVERT"
        name = p["output_name"]
        if len(name) > max_name:
            name = name[:max_name - 1] + "\u2026"
        tracks = f"{p['n_tracks']} tracks"
        print(f"  {status_tag}  {name:<{max_name}}  {tracks:>10}")

    n_convert = sum(1 for p in plan if p["status"] == "convert")
    n_skip = sum(1 for p in plan if p["status"] == "skip")
    print(f"\n  {n_convert} to convert, {n_skip} already done\n")


# ── online cover lookup ───────────────────────────────────────────────────────

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
        log("  Cover: found via Open Library (ISBN)")
        return True
    if fetch_cover_google_books(title, author, output_path):
        log("  Cover: found via Google Books")
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
        candidates = list(directory.rglob(Path(bookinfo_cover_src).name))
        if candidates:
            return candidates[0]

    # 2 – largest image file sitting next to the audio files
    img = find_largest_image(directory)
    if img:
        return img

    # 3 – extract from the first audio file with an embedded cover
    for mp3 in mp3_files[:3]:
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


# ── process a ZIP audiobook ──────────────────────────────────────────────────


def process_zip(zip_path, output_dir, temp_base):
    log(f"  ZIP: {zip_path.name}")

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
            log("  Extracting ...")
            extract_dir.mkdir(parents=True, exist_ok=True)
            zf.extractall(extract_dir)

        # ── order audio files ──
        all_audio = sorted(f for f in extract_dir.rglob("*")
                           if f.is_file() and is_audio_file(f.name))
        audio_by_name = {p.name: p for p in all_audio}

        mp3_files = None

        # try bookinfo chapter order first
        if info and info.chapters:
            ordered = [audio_by_name[ch["filename"]]
                       for ch in info.chapters
                       if ch.get("filename") in audio_by_name]
            if ordered:
                mp3_files = ordered

        # then playlist order
        if not mp3_files and playlist_entries:
            ordered = [audio_by_name[e["file"]]
                       for e in playlist_entries
                       if e.get("file") in audio_by_name]
            if ordered:
                mp3_files = ordered

        # fallback: sorted list
        if not mp3_files:
            mp3_files = all_audio

        if not mp3_files:
            log("  WARNING: no audio files found — skipping")
            return "skip"

        # ── fallback author from tags (or AI) ──
        if not author:
            tags = ffprobe_tags(mp3_files[0])
            filenames = [f.name for f in mp3_files]
            tag_author, tag_title = smart_extract_metadata(
                tags, folder_name=title, filenames=filenames,
            )
            author = tag_author
            if not title and tag_title:
                title = tag_title
        if not author:
            author = "Unknown"
        else:
            author = normalise_author(author)

        output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
        output_path = output_dir / output_name
        if output_path.exists():
            log(f"  SKIP (already exists): {output_name}")
            return "skip"

        # ── chapter titles + durations ──
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
            log(f"  DONE: {output_name} ({size_mb:.1f} MB)")
            return "done"
        else:
            log(f"  FAILED: {output_name}")
            return "failed"

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


# ── process a folder audiobook ────────────────────────────────────────────────


def process_folder(folder_path, base_dir, output_dir, temp_base):
    rel = folder_path.relative_to(base_dir)
    log(f"  Folder: {rel}")

    mp3_files = sorted(f for f in folder_path.iterdir()
                       if f.is_file() and is_audio_file(f.name))
    if not mp3_files:
        log("  WARNING: no audio files — skipping")
        return "skip"

    # ── check for a playlist and reorder if found ──
    playlist_entries = find_and_parse_playlist(folder_path)
    if playlist_entries:
        audio_by_name = {p.name: p for p in mp3_files}
        ordered = [audio_by_name[e["file"]]
                   for e in playlist_entries
                   if e.get("file") in audio_by_name]
        if ordered:
            mp3_files = ordered

    # ── determine author + title from tags ──
    folder_name = strip_number_prefix(folder_path.name) or folder_path.name
    parent_name = _parent_folder_name(folder_path, base_dir)
    tags = ffprobe_tags(mp3_files[0])
    filenames = [f.name for f in mp3_files]
    author, title = smart_extract_metadata(
        tags, folder_name=folder_name, parent_name=parent_name,
        filenames=filenames,
    )
    if not author:
        author = "Unknown"
    if not title:
        title = folder_name

    output_name = sanitize_filename(f"{author} - {title}") + ".m4b"
    output_path = output_dir / output_name
    if output_path.exists():
        log(f"  SKIP (already exists): {output_name}")
        return "skip"

    temp_dir = temp_base / sanitize_filename(str(rel).replace(os.sep, "_"))
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # ── chapter titles ──
        if playlist_entries and len(playlist_entries) == len(mp3_files):
            ch_titles_raw = [e.get("title", "").strip() for e in playlist_entries]
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
            log(f"  DONE: {output_name} ({size_mb:.1f} MB)")
            return "done"
        else:
            log(f"  FAILED: {output_name}")
            return "failed"

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


# ── main ──────────────────────────────────────────────────────────────────────


def _process_one(btype, bpath, base_dir, output_dir, temp_base):
    """Process a single book. Returns 'done', 'skip', or 'failed'."""
    try:
        if btype == "zip":
            return process_zip(bpath, output_dir, temp_base)
        else:
            return process_folder(bpath, base_dir, output_dir, temp_base)
    except Exception as exc:
        log(f"  ERROR ({bpath}): {exc}")
        return "failed"


def main():
    global _progress_bar

    ap = argparse.ArgumentParser(
        description="Convert audiobook MP3 collections to M4B files."
    )
    ap.add_argument(
        "directory", nargs="?", default=".",
        help="Root directory to scan (default: current dir)",
    )
    ap.add_argument(
        "-j", "--jobs", type=int, default=1,
        help="Number of books to process in parallel (default: 1)",
    )
    ap.add_argument(
        "-n", "--dry-run", action="store_true",
        help="Show what would be converted, then exit",
    )
    ap.add_argument(
        "--claude-api-key",
        default=os.environ.get("CLAUDE_API_KEY", ""),
        help="Anthropic API key for AI-assisted metadata extraction "
             "(or set CLAUDE_API_KEY env var)",
    )
    ap.add_argument(
        "--claude-model",
        default=os.environ.get("CLAUDE_MODEL", ""),
        help="Claude model to use (default: tries cheapest first: "
             + ", ".join(_CLAUDE_DEFAULT_MODELS) + ")",
    )
    args = ap.parse_args()

    global _claude_api_key, _claude_models_to_try, _claude_model

    base_dir = Path(args.directory).resolve()
    output_dir = base_dir / OUTPUT_SUBDIR
    temp_base = base_dir / TEMP_SUBDIR
    jobs = max(1, args.jobs)
    _claude_api_key = args.claude_api_key or None
    if args.claude_model:
        # User specified a model — use only that one, skip auto-detection
        _claude_models_to_try = [args.claude_model]
        _claude_model = args.claude_model

    # preflight
    for tool in ("ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            print(f"ERROR: '{tool}' not found on PATH")
            sys.exit(1)

    print(f"Source:  {base_dir}")
    print(f"Output:  {output_dir}")
    if _claude_api_key:
        print(f"AI:      enabled")
    if not args.dry_run:
        print(f"Jobs:    {jobs}")

    # ── discover ──
    books = discover_books(base_dir)
    if not books:
        print("\nNo audiobooks found.")
        return

    # ── plan ──
    print("\nScanning metadata ...")
    plan = build_plan(books, base_dir, output_dir)
    display_plan(plan)

    to_convert = [p for p in plan if p["status"] == "convert"]

    if args.dry_run:
        return

    if not to_convert:
        print("Nothing to do.")
        return

    # ── process ──
    results = {"done": 0, "skip": 0, "failed": 0}

    if tqdm:
        bar = tqdm(
            total=len(to_convert),
            desc="Converting",
            unit="book",
            dynamic_ncols=True,
        )
        _progress_bar = bar
    else:
        bar = None
        _progress_bar = None

    try:
        if jobs == 1:
            for p in to_convert:
                if bar:
                    bar.set_postfix_str(p["output_name"][:40], refresh=True)
                result = _process_one(
                    p["btype"], p["bpath"], base_dir, output_dir, temp_base,
                )
                results[result or "failed"] += 1
                if bar:
                    bar.update(1)
        else:
            with ThreadPoolExecutor(max_workers=jobs) as pool:
                futures = {}
                for p in to_convert:
                    f = pool.submit(
                        _process_one,
                        p["btype"], p["bpath"], base_dir, output_dir, temp_base,
                    )
                    futures[f] = p
                for f in as_completed(futures):
                    p = futures[f]
                    result = f.result()
                    results[result or "failed"] += 1
                    if bar:
                        bar.set_postfix_str(p["output_name"][:40], refresh=False)
                        bar.update(1)
    finally:
        if bar:
            bar.close()
        _progress_bar = None

    # clean up temp root if empty
    if temp_base.exists():
        try:
            temp_base.rmdir()
        except OSError:
            pass

    # ── summary ──
    print(f"\n{'=' * 60}")
    n_skip_plan = sum(1 for p in plan if p["status"] == "skip")
    parts = []
    if results["done"]:
        parts.append(f"{results['done']} converted")
    if n_skip_plan:
        parts.append(f"{n_skip_plan} already existed")
    if results["skip"]:
        parts.append(f"{results['skip']} skipped at runtime")
    if results["failed"]:
        parts.append(f"{results['failed']} FAILED")
    print(f"Done: {', '.join(parts)}.")


if __name__ == "__main__":
    main()
