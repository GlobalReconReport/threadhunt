"""
YouTube collector — public channel scraping without an API key.

YouTube embeds a `var ytInitialData = {...}` JSON blob in every channel-videos
page.  We extract video titles, description snippets, and upload dates from
that blob.  No authentication, no quota, no key needed.

Supported URL patterns (tried in order):
  https://www.youtube.com/@HANDLE/videos     (new @handle format)
  https://www.youtube.com/c/HANDLE/videos    (legacy /c/ path)
  https://www.youtube.com/user/HANDLE/videos (legacy /user/ path)

Each video is stored as a post:
  post_id   = YouTube video ID
  content   = "TITLE\n\nDESCRIPTION_SNIPPET"
  timestamp = approximated from "N days/weeks/months ago"
  platform  = 'youtube'

Cap: 50 videos per run (ytInitialData first page only; no JS pagination).
"""
import re
import json
import time
import logging
from datetime import datetime, timezone, timedelta

import config
import db

logger = logging.getLogger('threadhunt')

_YT_CAP = 50

_YT_HEADERS = {
    'User-Agent':      ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/124.0.0.0 Safari/537.36'),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept':          'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
}

# URL patterns to attempt for a given handle
_CHANNEL_URL_PATTERNS = [
    'https://www.youtube.com/@{handle}/videos',
    'https://www.youtube.com/c/{handle}/videos',
    'https://www.youtube.com/user/{handle}/videos',
    'https://www.youtube.com/{handle}/videos',
]


# ── Page fetching ─────────────────────────────────────────────────────────────

def _fetch_channel_html(session, handle: str) -> str | None:
    """
    Try each URL pattern for the handle.
    Return HTML string on first 200 that contains ytInitialData, else None.
    """
    timeout = config.get('request_timeout', 15)
    for pattern in _CHANNEL_URL_PATTERNS:
        url = pattern.format(handle=handle)
        try:
            r = session.get(url, headers=_YT_HEADERS, timeout=timeout,
                            allow_redirects=True)
            if r.status_code == 200 and 'ytInitialData' in r.text:
                logger.debug("YouTube: found channel at %s", url)
                return r.text
        except Exception as e:
            logger.debug("YouTube fetch error %s: %s", url, e)
    return None


# ── ytInitialData extraction ──────────────────────────────────────────────────

def _extract_yt_initial_data(html: str) -> dict:
    """
    Locate and parse the ytInitialData JSON blob embedded in the page.
    Uses JSONDecoder.raw_decode() to correctly handle any JSON size.
    """
    needle = 'var ytInitialData = '
    pos = html.find(needle)
    if pos < 0:
        # Alternate form used in some pages
        needle = 'window["ytInitialData"] = '
        pos = html.find(needle)
    if pos < 0:
        return {}

    json_start = pos + len(needle)
    try:
        data, _ = json.JSONDecoder().raw_decode(html[json_start:])
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def _find_video_renderers(obj, found: list | None = None) -> list:
    """
    Recursively walk ytInitialData looking for videoRenderer dicts.
    YouTube's JSON structure changes; recursive search is robust.
    Caps at 200 results.
    """
    if found is None:
        found = []
    if len(found) >= 200:
        return found

    if isinstance(obj, dict):
        if 'videoId' in obj and 'title' in obj:
            found.append(obj)
        else:
            for v in obj.values():
                _find_video_renderers(v, found)
    elif isinstance(obj, list):
        for item in obj:
            _find_video_renderers(item, found)
    return found


# ── Timestamp parsing ─────────────────────────────────────────────────────────

def _parse_relative_time(text: str) -> str:
    """
    Convert YouTube's relative timestamps to approximate ISO 8601.
    Handles: "3 days ago", "2 weeks ago", "1 month ago", "5 years ago",
             "Streamed 4 hours ago", "Premiered 2 days ago", etc.
    Falls back to now() if unparseable.
    """
    if not text:
        return datetime.now(timezone.utc).isoformat()

    text = text.lower()
    m = re.search(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?', text)
    if not m:
        return datetime.now(timezone.utc).isoformat()

    n    = int(m.group(1))
    unit = m.group(2)
    now  = datetime.now(timezone.utc)
    delta_map = {
        'second': timedelta(seconds=n),
        'minute': timedelta(minutes=n),
        'hour':   timedelta(hours=n),
        'day':    timedelta(days=n),
        'week':   timedelta(weeks=n),
        'month':  timedelta(days=n * 30),
        'year':   timedelta(days=n * 365),
    }
    return (now - delta_map[unit]).isoformat()


# ── Post extraction ───────────────────────────────────────────────────────────

def _renderer_to_post(renderer: dict) -> dict | None:
    """
    Convert a videoRenderer dict to a post dict
    {video_id, title, description, timestamp}.
    """
    video_id = renderer.get('videoId', '').strip()
    if not video_id:
        return None

    # Title
    title = ''
    title_node = renderer.get('title', {})
    if isinstance(title_node, dict):
        runs = title_node.get('runs', [])
        if runs:
            title = runs[0].get('text', '')
        elif 'simpleText' in title_node:
            title = title_node['simpleText']
    if not title:
        return None

    # Description snippet
    desc = ''
    desc_node = renderer.get('descriptionSnippet', {})
    if isinstance(desc_node, dict):
        for run in desc_node.get('runs', []):
            desc += run.get('text', '')

    # Timestamp from relative time
    time_node = renderer.get('publishedTimeText', {})
    relative  = (time_node.get('simpleText', '') if isinstance(time_node, dict)
                 else str(time_node))
    timestamp = _parse_relative_time(relative)

    content = title.strip()
    if desc.strip():
        content += '\n\n' + desc.strip()

    return {
        'video_id':  video_id,
        'title':     title,
        'content':   content,
        'timestamp': timestamp,
    }


# ── Public entry point ────────────────────────────────────────────────────────

def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect video titles and descriptions from a public YouTube channel.
    target: channel handle — e.g. 'TheGrayzone', 'DDGeopolitics', 'RT'
    Returns count of new posts inserted.

    Stores each video as a post:
      content   = "TITLE\\n\\nDESCRIPTION_SNIPPET"
      platform  = 'youtube'
      post_id   = YouTube video ID
    """
    cap = min(config.get('collector_post_cap', 500), _YT_CAP)

    if verbose_cb:
        verbose_cb(f"Fetching YouTube channel: {target}")

    html = _fetch_channel_html(session, target)
    if not html:
        logger.warning("YouTube: could not fetch channel '%s' (all URL patterns failed)", target)
        if verbose_cb:
            verbose_cb(f"YouTube: channel '{target}' not found or blocked")
        return 0

    if verbose_cb:
        verbose_cb(f"YouTube: parsing ytInitialData for {target}")

    data = _extract_yt_initial_data(html)
    if not data:
        logger.warning("YouTube: no ytInitialData in response for '%s'", target)
        return 0

    renderers = _find_video_renderers(data)
    if not renderers:
        logger.warning("YouTube: no videoRenderers found for '%s'", target)
        return 0

    logger.info("YouTube: found %d videos for channel '%s'", len(renderers), target)

    # Upsert channel account
    with db.get_conn() as conn:
        account_id = db.upsert_account(conn, target, 'youtube')
    if account_id is None:
        logger.error("YouTube: failed to upsert account for '%s'", target)
        return 0

    new_count = 0
    with db.get_conn() as conn:
        for renderer in renderers[:cap]:
            post = _renderer_to_post(renderer)
            if not post:
                continue
            if keyword and keyword.lower() not in post['content'].lower():
                continue
            if db.insert_post(conn, account_id, 'youtube',
                              post['video_id'], post['content'], post['timestamp']):
                new_count += 1
                if verbose_cb and new_count % 10 == 0:
                    verbose_cb(f"YouTube: {new_count} new videos stored...")

    logger.info("YouTube: %d new posts from channel '%s'", new_count, target)
    return new_count
