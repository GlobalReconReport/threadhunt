"""
VK (VKontakte) collector — public wall scraping via HTML.
VK is JS-heavy; we extract what the initial HTML render provides.
Targets: public groups/pages (e.g. 'rt_russian', 'ria_novosti').
Cap: 500 posts per run.
"""
import re
import time
import logging
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import config
import db
from utils.text import strip_html

logger = logging.getLogger('threadhunt')

VK_WALL_URL  = 'https://vk.com/{group}'
VK_FEED_URL  = 'https://vk.com/feed2'    # Used for some older endpoints


def _fetch_wall(session, group: str, offset: int = 0) -> BeautifulSoup | None:
    """
    Fetch VK public wall page. VK returns partial server-side HTML for
    the first render; additional posts require JS/XHR.
    offset param is passed as ?offset=N for pagination attempts.
    """
    url = VK_WALL_URL.format(group=group)
    params = {'offset': offset} if offset else {}

    try:
        r = session.get(url, params=params,
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser')
        logger.debug("VK %s HTTP %d", group, r.status_code)
    except Exception as e:
        logger.warning("VK fetch error %s: %s", group, e)
    return None


def _parse_posts(soup: BeautifulSoup) -> list:
    """
    Extract posts from VK wall HTML.
    Handles both classic wall items and the newer feed structure.
    Returns list of {post_id, content, timestamp}.
    """
    results = []

    # Classic wall: .wall_item or .post
    for post_el in soup.select('.wall_item, ._post, .post'):
        post = _parse_wall_item(post_el)
        if post:
            results.append(post)

    # Newer feed structure: .wall_post_cont
    if not results:
        for post_el in soup.select('.wall_post_cont'):
            post = _parse_feed_item(post_el)
            if post:
                results.append(post)

    return results


def _parse_wall_item(el) -> dict | None:
    """Parse a classic VK wall post element."""
    try:
        # Post ID: usually in data-post-id or an anchor href like /wall-12345_6789
        post_id = el.get('data-post-id', '')
        if not post_id:
            anchor = el.select_one('a[href*="/wall"]')
            if anchor:
                m = re.search(r'/wall(-?\d+_\d+)', anchor.get('href', ''))
                if m:
                    post_id = m.group(1)
        if not post_id:
            return None

        # Text content
        text_el = el.select_one('.wall_post_text, .post__text, ._post_content')
        content = text_el.get_text(separator=' ', strip=True) if text_el else ''
        if not content.strip():
            return None

        # Timestamp
        ts_el = el.select_one('time[datetime], .rel_date')
        timestamp = _parse_vk_time(ts_el)

        return {'post_id': post_id, 'content': content, 'timestamp': timestamp}
    except Exception as e:
        logger.debug("VK wall item parse error: %s", e)
        return None


def _parse_feed_item(el) -> dict | None:
    """Parse a newer VK feed-style post."""
    try:
        text_el = el.select_one('.wall_post_text')
        content = text_el.get_text(separator=' ', strip=True) if text_el else ''
        if not content.strip():
            return None

        # Look for post ID in parent attributes
        parent = el.parent
        post_id = None
        if parent:
            post_id = parent.get('data-post-id', '')
            if not post_id:
                m = re.search(r'post(-?\d+_\d+)', str(parent.get('id', '')))
                if m:
                    post_id = m.group(1)

        if not post_id:
            # Generate deterministic ID from content hash
            import hashlib
            post_id = 'vk_' + hashlib.md5(content.encode()).hexdigest()[:12]

        ts_el = el.select_one('time[datetime], .rel_date')
        timestamp = _parse_vk_time(ts_el)

        return {'post_id': post_id, 'content': content, 'timestamp': timestamp}
    except Exception as e:
        logger.debug("VK feed item parse error: %s", e)
        return None


def _parse_vk_time(el) -> str:
    """Extract timestamp from VK time element."""
    if not el:
        return datetime.now(timezone.utc).isoformat()

    # <time datetime="2023-01-15T14:30:00">
    if el.get('datetime'):
        try:
            raw = el['datetime'].split('+')[0].split('Z')[0]
            dt = datetime.strptime(raw, '%Y-%m-%dT%H:%M:%S')
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass

    return datetime.now(timezone.utc).isoformat()


def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect public posts from a VK group/page.
    target: VK group short name (e.g. 'rt_russian').
    Returns count of new posts inserted.

    Note: VK renders most content via JavaScript. This collector captures
    the server-side initial render — typically 15–30 visible posts.
    For deeper collection, the VK API (with key) is required.
    """
    cap = config.get('collector_post_cap', 500)
    new_count = 0

    with db.get_conn() as conn:
        account_id = db.upsert_account(conn, target, 'vk')
    if account_id is None:
        logger.error("VK: failed to create account for %s", target)
        return 0

    if verbose_cb:
        verbose_cb(f"VK: fetching vk.com/{target}")

    soup = _fetch_wall(session, target)
    if not soup:
        return 0

    posts = _parse_posts(soup)

    if verbose_cb:
        verbose_cb(f"VK: parsed {len(posts)} posts from initial render")

    with db.get_conn() as conn:
        for post in posts:
            if new_count >= cap:
                break
            if keyword and keyword.lower() not in post['content'].lower():
                continue
            if db.insert_post(conn, account_id, 'vk',
                              post['post_id'], post['content'], post['timestamp']):
                new_count += 1

    logger.info("VK: %d new posts from %s", new_count, target)
    return new_count
