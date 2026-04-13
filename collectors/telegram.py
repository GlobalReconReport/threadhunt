"""
Telegram collector — public channels via t.me/s/{channel} web preview.
No API key. No Telethon. Cap: 500 messages per channel per run.
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

BASE_URL = 'https://t.me/s/{channel}'


def _fetch_page(session, channel: str, before: int = None) -> BeautifulSoup | None:
    """Fetch t.me/s/{channel}?before=N and return parsed HTML."""
    url = BASE_URL.format(channel=channel)
    params = {}
    if before:
        params['before'] = before

    try:
        r = session.get(url, params=params,
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            return BeautifulSoup(r.text, 'html.parser')
        logger.debug("Telegram %s HTTP %d", channel, r.status_code)
    except Exception as e:
        logger.warning("Telegram fetch error %s: %s", channel, e)
    return None


def _parse_messages(soup: BeautifulSoup) -> list:
    """
    Extract messages from a t.me/s/ page.
    Returns list of {post_id, content, timestamp}.
    """
    results = []
    for msg_div in soup.select('.tgme_widget_message'):
        post = _parse_message(msg_div)
        if post:
            results.append(post)
    return results


def _parse_message(msg_div) -> dict | None:
    """Parse a single .tgme_widget_message element."""
    try:
        # Message ID from data-post attribute: "channel/123"
        data_post = msg_div.get('data-post', '')
        m = re.search(r'/(\d+)$', data_post)
        if not m:
            return None
        post_id = m.group(1)

        # Text content
        text_el = msg_div.select_one('.tgme_widget_message_text')
        if text_el:
            content = text_el.get_text(separator=' ', strip=True)
        else:
            # Might be a media-only post — extract caption or description
            content = ''
            for el in msg_div.select('.tgme_widget_message_photo_caption'):
                content += el.get_text(separator=' ', strip=True)

        if not content.strip():
            return None

        # Timestamp from <time datetime="...">
        time_el = msg_div.select_one('time[datetime]')
        if time_el and time_el.get('datetime'):
            try:
                raw_dt = time_el['datetime']   # ISO 8601 with offset
                # Normalize: strip offset, parse as UTC
                raw_dt = raw_dt.split('+')[0].split('Z')[0]
                dt = datetime.strptime(raw_dt, '%Y-%m-%dT%H:%M:%S')
                timestamp = dt.replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                timestamp = datetime.now(timezone.utc).isoformat()
        else:
            timestamp = datetime.now(timezone.utc).isoformat()

        return {'post_id': post_id, 'content': content, 'timestamp': timestamp}

    except Exception as e:
        logger.debug("Telegram message parse error: %s", e)
        return None


def _get_oldest_id(messages: list) -> int | None:
    """Return the smallest post_id integer from a message list."""
    ids = []
    for m in messages:
        try:
            ids.append(int(m['post_id']))
        except Exception:
            pass
    return min(ids) if ids else None


def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect public messages from a Telegram channel.
    target: channel username (without @).
    Returns count of new posts inserted.
    """
    cap = config.get('collector_post_cap', 500)
    new_count = 0
    before_id = None

    with db.get_conn() as conn:
        account_id = db.upsert_account(conn, target, 'telegram')
    if account_id is None:
        logger.error("Telegram: failed to create account for %s", target)
        return 0

    while new_count < cap:
        if verbose_cb:
            verbose_cb(f"@{target} — fetched {new_count} messages so far")

        soup = _fetch_page(session, target, before=before_id)
        if not soup:
            break

        messages = _parse_messages(soup)
        if not messages:
            break

        with db.get_conn() as conn:
            page_new = 0
            for msg in messages:
                if new_count >= cap:
                    break
                if keyword and keyword.lower() not in msg['content'].lower():
                    continue
                if db.insert_post(conn, account_id, 'telegram',
                                  msg['post_id'], msg['content'], msg['timestamp']):
                    new_count += 1
                    page_new += 1

        if page_new == 0:
            break   # Probably hit the known boundary; stop paginating

        oldest = _get_oldest_id(messages)
        if oldest and oldest > 1:
            before_id = oldest
        else:
            break

        time.sleep(1.5)

    logger.info("Telegram: %d new posts from @%s", new_count, target)
    return new_count
