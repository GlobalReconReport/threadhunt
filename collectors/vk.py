"""
VK (VKontakte) collector — mobile endpoint scraping with pagination.
Uses m.vk.com (server-side rendered) instead of desktop vk.com (JS-heavy).
The mobile endpoint paginates via ?offset=N, yielding 100+ posts per run.
Targets: public groups/pages (e.g. 'rt_russian', 'ria_novosti').
Cap: 500 posts per run.
"""
import re
import time
import logging
import hashlib
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import config
import db
from utils.text import strip_html

logger = logging.getLogger('threadhunt')

# Mobile endpoint — server-side rendered, paginates without JS
VK_MOBILE_URL = 'https://m.vk.com/{group}'
# Step size for mobile pagination
VK_PAGE_STEP  = 10


def _fetch_mobile(session, group: str, offset: int = 0) -> BeautifulSoup | None:
    """
    Fetch m.vk.com/{group}?offset=N (server-side rendered HTML).
    Returns parsed BeautifulSoup or None on failure.
    """
    url = VK_MOBILE_URL.format(group=group)
    params = {'offset': offset} if offset else {}

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Linux; Android 11; SM-G998B) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Mobile Safari/537.36'
        )
    }

    try:
        r = session.get(url, params=params, headers=headers,
                        timeout=config.get('request_timeout', 15))
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser')
        logger.debug("VK mobile %s offset=%d HTTP %d", group, offset, r.status_code)
    except Exception as e:
        logger.warning("VK mobile fetch error %s offset=%d: %s", group, offset, e)
    return None


def _parse_mobile_posts(soup: BeautifulSoup) -> list:
    """
    Extract posts from m.vk.com wall HTML.
    Mobile VK uses .wall_item with .pi_text for post text.
    Returns list of {post_id, content, timestamp}.
    """
    results = []

    # Mobile VK: each post is a <div class="wi_body"> inside a wall item
    for post_el in soup.select('.wall_item'):
        post = _parse_mobile_item(post_el)
        if post:
            results.append(post)

    # Fallback: try desktop selectors (m.vk.com sometimes returns desktop-ish HTML)
    if not results:
        for post_el in soup.select('._post, .post, .wall_post_cont'):
            post = _parse_desktop_item(post_el)
            if post:
                results.append(post)

    return results


def _parse_mobile_item(el) -> dict | None:
    """Parse a mobile VK wall post element."""
    try:
        # Post ID from data-post attribute or link href
        post_id = el.get('data-post', '') or el.get('id', '')
        if not post_id:
            link = el.select_one('a[href*="/wall"]')
            if link:
                m = re.search(r'wall(-?\d+_\d+)', link.get('href', ''))
                if m:
                    post_id = m.group(1)
        if not post_id:
            return None

        # Text: mobile uses .pi_text, .wall_post_text, or .wi_text
        content = ''
        for sel in ('.pi_text', '.wall_post_text', '.wi_text', '.post__text'):
            text_el = el.select_one(sel)
            if text_el:
                content = text_el.get_text(separator=' ', strip=True)
                break

        if not content.strip():
            return None

        # Timestamp from <time> or .wi_date
        ts_el = el.select_one('time[datetime]')
        if ts_el:
            timestamp = _parse_vk_time(ts_el)
        else:
            # .wi_date text like "13 Apr at 14:30" — use now as fallback
            timestamp = datetime.now(timezone.utc).isoformat()

        return {'post_id': str(post_id), 'content': content, 'timestamp': timestamp}
    except Exception as e:
        logger.debug("VK mobile item parse error: %s", e)
        return None


def _parse_desktop_item(el) -> dict | None:
    """Parse a desktop-style VK wall post (fallback)."""
    try:
        post_id = el.get('data-post-id', '')
        if not post_id:
            anchor = el.select_one('a[href*="/wall"]')
            if anchor:
                m = re.search(r'/wall(-?\d+_\d+)', anchor.get('href', ''))
                if m:
                    post_id = m.group(1)
        if not post_id:
            return None

        text_el = el.select_one('.wall_post_text, .post__text, ._post_content')
        content = text_el.get_text(separator=' ', strip=True) if text_el else ''
        if not content.strip():
            return None

        ts_el = el.select_one('time[datetime]')
        timestamp = _parse_vk_time(ts_el)
        return {'post_id': str(post_id), 'content': content, 'timestamp': timestamp}
    except Exception as e:
        logger.debug("VK desktop item parse error: %s", e)
        return None


def _parse_vk_time(el) -> str:
    """Extract timestamp from VK <time datetime="..."> element."""
    if el and el.get('datetime'):
        try:
            raw = el['datetime'].split('+')[0].split('Z')[0]
            dt = datetime.strptime(raw, '%Y-%m-%dT%H:%M:%S')
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect public posts from a VK group/page via the mobile endpoint.
    target: VK group short name (e.g. 'rt_russian').
    Returns count of new posts inserted.

    Uses m.vk.com (server-side rendered) with ?offset=N pagination.
    Yields significantly more posts than desktop scraping.
    """
    cap = config.get('collector_post_cap', 500)
    new_count = 0
    offset = 0
    consecutive_empty = 0

    with db.get_conn() as conn:
        account_id = db.upsert_account(conn, target, 'vk')
    if account_id is None:
        logger.error("VK: failed to create account for %s", target)
        return 0

    while new_count < cap:
        if verbose_cb:
            verbose_cb(f"VK m.vk.com/{target} offset={offset} ({new_count} collected)")

        soup = _fetch_mobile(session, target, offset=offset)
        if not soup:
            break

        posts = _parse_mobile_posts(soup)
        if not posts:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
        else:
            consecutive_empty = 0

        page_new = 0
        with db.get_conn() as conn:
            for post in posts:
                if new_count >= cap:
                    break
                if keyword and keyword.lower() not in post['content'].lower():
                    continue
                if db.insert_post(conn, account_id, 'vk',
                                  post['post_id'], post['content'], post['timestamp']):
                    new_count += 1
                    page_new += 1

        if page_new == 0 and offset > 0:
            break   # Hitting known-posts boundary, stop paginating

        offset += VK_PAGE_STEP
        time.sleep(1.5)

    logger.info("VK: %d new posts from %s (mobile endpoint)", new_count, target)
    return new_count
