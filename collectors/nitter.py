"""
Nitter collector — Twitter/X data without the API.
Rotates through a pool of public Nitter instances.
Health-checks on first use; dead instances dropped silently.
Cap: 500 posts per target per run (configurable).
"""
import re
import time
import logging
import hashlib
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import config
import db

logger = logging.getLogger('threadhunt')

_live_instances: list | None = None


# ── Instance management ───────────────────────────────────────────────────────

def health_check_instances(session) -> list:
    """GET / on each instance with timeout=5. Return only responsive ones."""
    global _live_instances
    instances = config.get('nitter_instances', [])
    live = []
    for inst in instances:
        try:
            r = session.get(inst + '/', timeout=5, allow_redirects=True)
            if r.status_code == 200 and 'nitter' in r.text.lower():
                live.append(inst)
        except Exception:
            pass
    _live_instances = live
    logger.info("Nitter health check: %d/%d instances live", len(live), len(instances))
    return live


def _instances(session) -> list:
    global _live_instances
    if _live_instances is None:
        health_check_instances(session)
    return _live_instances or []


def _fetch(session, path: str) -> BeautifulSoup | None:
    """Try each live instance in order. Return parsed HTML or None."""
    for inst in _instances(session):
        url = inst + path
        try:
            r = session.get(url, timeout=config.get('request_timeout', 10))
            if r.status_code == 200:
                return BeautifulSoup(r.text, 'html.parser')
            if r.status_code == 429:
                continue  # rate-limited, try next
        except Exception as e:
            logger.debug("Nitter %s error: %s", inst, e)
    return None


# ── Profile scraping ──────────────────────────────────────────────────────────

def scrape_profile(session, username: str) -> dict:
    """Return dict with profile fields. Empty dict on failure."""
    soup = _fetch(session, f'/{username}')
    if not soup:
        return {}

    profile: dict = {'username': username}

    # Bio
    bio_el = soup.select_one('.profile-bio p')
    if not bio_el:
        bio_el = soup.select_one('.profile-bio')
    profile['bio'] = bio_el.get_text(strip=True) if bio_el else ''

    # Stats — Nitter renders them as anchored list items
    followers = _parse_stat(soup, '/followers')
    following = _parse_stat(soup, '/following')
    profile['followers'] = followers
    profile['following'] = following

    # Profile pic hash
    pic_el = soup.select_one('.profile-card-avatar img')
    if pic_el and pic_el.get('src'):
        pic_src = pic_el['src']
        if pic_src.startswith('/'):
            insts = _instances(session)
            if insts:
                pic_src = insts[0] + pic_src
        try:
            pr = session.get(pic_src, timeout=5)
            if pr.status_code == 200:
                profile['profile_pic_hash'] = hashlib.md5(pr.content).hexdigest()
        except Exception:
            pass

    return profile


def _parse_stat(soup, href_suffix: str) -> int:
    """Extract a stat number from a Nitter profile-statlist link."""
    for a in soup.select('.profile-statlist a'):
        if a.get('href', '').endswith(href_suffix):
            num_el = a.select_one('.profile-stat-num')
            if num_el:
                try:
                    return int(num_el.get_text(strip=True).replace(',', ''))
                except ValueError:
                    pass
    return 0


# ── Timeline scraping ─────────────────────────────────────────────────────────

def scrape_timeline(session, username: str, cap: int) -> list:
    """
    Generator — yields post dicts from user timeline up to cap.
    Each dict: {post_id, content, timestamp}
    """
    path = f'/{username}'
    collected = 0

    while collected < cap:
        soup = _fetch(session, path)
        if not soup:
            break

        items = soup.select('.timeline-item:not(.show-more)')
        if not items:
            break

        page_count = 0
        for item in items:
            if collected >= cap:
                break
            post = _parse_item(item)
            if post:
                yield post
                collected += 1
                page_count += 1

        if page_count == 0:
            break

        # Advance pagination via cursor
        show_more = soup.select_one('.show-more a')
        if not show_more or not show_more.get('href'):
            break
        href = show_more['href']
        m = re.search(r'cursor=([^&]+)', href)
        if not m:
            break
        path = f'/{username}?cursor={m.group(1)}'
        time.sleep(1.2)   # polite pacing


def _parse_item(item) -> dict | None:
    """Extract post_id, content, timestamp from a .timeline-item element."""
    try:
        content_el = item.select_one('.tweet-content')
        if not content_el:
            return None
        content = content_el.get_text(separator=' ', strip=True)
        if not content:
            return None

        # Post ID from tweet permalink
        link = item.select_one('.tweet-link')
        if not link:
            return None
        m = re.search(r'/status/(\d+)', link.get('href', ''))
        if not m:
            return None
        post_id = m.group(1)

        # Timestamp from the title attribute
        ts_el = item.select_one('.tweet-date a')
        timestamp = _parse_nitter_date(ts_el['title']) if (ts_el and ts_el.get('title')) else _now_iso()

        return {'post_id': post_id, 'content': content, 'timestamp': timestamp}
    except Exception as e:
        logger.debug("Nitter item parse error: %s", e)
        return None


def _parse_nitter_date(raw: str) -> str:
    """
    Handle multiple Nitter date formats:
      "Jan 1, 2023 · 12:00 PM UTC"
      "1 Jan 2023, 12:00:00 UTC"
    Returns ISO 8601 or the raw string if parsing fails.
    """
    raw = raw.replace(' · ', ' ').replace(',', '').strip()
    formats = [
        '%b %d %Y %I:%M %p UTC',
        '%b %d %Y %H:%M UTC',
        '%d %b %Y %H:%M:%S UTC',
        '%d %b %Y %H:%M UTC',
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            pass
    return raw


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Public entry point ────────────────────────────────────────────────────────

def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect posts from a Twitter username via Nitter.
    verbose_cb(msg): optional callback for live status updates.
    Returns count of new posts inserted.
    """
    cap = config.get('collector_post_cap', 500)

    if verbose_cb:
        verbose_cb(f"Checking instances...")
    health_check_instances(session)

    if not _instances(session):
        logger.error("Nitter: no live instances available")
        return 0

    if verbose_cb:
        verbose_cb(f"Scraping profile @{target}")

    profile = scrape_profile(session, target)
    new_count = 0

    with db.get_conn() as conn:
        account_id = db.upsert_account(
            conn, target, 'twitter',
            followers=profile.get('followers', 0),
            following=profile.get('following', 0),
            profile_pic_hash=profile.get('profile_pic_hash'),
            bio=profile.get('bio', ''),
        )
        if account_id is None:
            logger.error("Nitter: failed to upsert account %s", target)
            return 0

        if verbose_cb:
            verbose_cb(f"Collecting timeline (cap={cap})")

        for post in scrape_timeline(session, target, cap=cap):
            if keyword and keyword.lower() not in post['content'].lower():
                continue
            if db.insert_post(conn, account_id, 'twitter',
                              post['post_id'], post['content'], post['timestamp']):
                new_count += 1
                if verbose_cb and new_count % 50 == 0:
                    verbose_cb(f"{new_count} new posts so far...")

    logger.info("Nitter: %d new posts from @%s", new_count, target)
    return new_count
