"""
Generic web collector + Wayback Machine archiver.
Used for news sites, blogs, pastebin-style links, and archiving targets
before they get taken down.
"""
import re
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

import config
import db
from utils.text import strip_html, extract_urls

logger = logging.getLogger('threadhunt')

WAYBACK_SAVE_URL  = 'https://web.archive.org/save/{url}'
WAYBACK_CHECK_URL = 'https://archive.org/wayback/available?url={url}'


# ── Wayback Machine ───────────────────────────────────────────────────────────

def archive_url(session, url: str) -> str | None:
    """
    Request Wayback Machine to snapshot a URL.
    Returns the archived URL if successful, None otherwise.
    Useful for capturing evidence before an account or post is deleted.
    """
    save_url = WAYBACK_SAVE_URL.format(url=url)
    try:
        r = session.get(save_url, timeout=30, allow_redirects=True)
        if r.status_code in (200, 302):
            archived = r.headers.get('Content-Location', '')
            if archived:
                return 'https://web.archive.org' + archived
            if 'web.archive.org' in r.url:
                return r.url
    except Exception as e:
        logger.warning("Wayback archive failed for %s: %s", url, e)
    return None


def check_wayback(session, url: str) -> str | None:
    """
    Check if a URL has an existing Wayback snapshot.
    Returns snapshot URL or None.
    """
    check_url = WAYBACK_CHECK_URL.format(url=url)
    try:
        r = session.get(check_url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            snap = data.get('archived_snapshots', {}).get('closest', {})
            if snap.get('available') and snap.get('url'):
                return snap['url']
    except Exception as e:
        logger.debug("Wayback check error for %s: %s", url, e)
    return None


# ── Generic page scraper ──────────────────────────────────────────────────────

def scrape_page(session, url: str) -> dict:
    """
    Scrape a generic web page.
    Returns {title, text, links, url}.
    """
    result = {'url': url, 'title': '', 'text': '', 'links': []}
    try:
        r = session.get(url, timeout=config.get('request_timeout', 10))
        if r.status_code != 200:
            return result
        soup = BeautifulSoup(r.text, 'html.parser')

        # Title
        title_el = soup.select_one('title')
        result['title'] = title_el.get_text(strip=True) if title_el else ''

        # Remove boilerplate
        for tag in soup.select('nav, footer, header, script, style, aside, .ad, #ad'):
            tag.decompose()

        # Main text: prefer article/main, fall back to body
        main_el = soup.select_one('article, main, [role="main"]') or soup.body
        if main_el:
            result['text'] = main_el.get_text(separator=' ', strip=True)[:8000]

        # Extract links for link-graph purposes
        base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        for a in soup.select('a[href]')[:50]:
            href = a.get('href', '')
            if href.startswith('http'):
                result['links'].append(href)
            elif href.startswith('/'):
                result['links'].append(urljoin(base, href))

    except Exception as e:
        logger.warning("Web scrape error %s: %s", url, e)

    return result


def _domain_as_platform(url: str) -> str:
    """Turn a URL into a short platform label."""
    try:
        host = urlparse(url).netloc
        host = re.sub(r'^www\.', '', host)
        return host[:40]
    except Exception:
        return 'web'


def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Scrape a web URL and store extracted text as a post.
    target: full URL (https://...).
    Returns count of new posts inserted (0 or 1 for a single page).
    """
    if verbose_cb:
        verbose_cb(f"Scraping: {target}")

    result = scrape_page(session, target)
    if not result['text']:
        logger.warning("Web: no text extracted from %s", target)
        return 0

    if keyword and keyword.lower() not in result['text'].lower():
        return 0

    platform = _domain_as_platform(target)
    post_id = re.sub(r'[^\w]', '_', target)[:100]
    timestamp = datetime.now(timezone.utc).isoformat()
    title_content = f"{result['title']} | {result['text'][:2000]}"

    with db.get_conn() as conn:
        account_id = db.upsert_account(conn, platform, 'web')
        if account_id is None:
            return 0
        new = db.insert_post(conn, account_id, 'web', post_id,
                             title_content, timestamp)

    if new:
        logger.info("Web: stored page %s", target)
        # Attempt to archive as evidence
        try:
            archived = archive_url(session, target)
            if archived:
                logger.info("Archived to Wayback: %s", archived)
        except Exception:
            pass
    return 1 if new else 0


def archive(session, target: str, verbose_cb=None) -> str | None:
    """
    Archive a URL to Wayback Machine without collecting its content.
    Returns archived URL or None.
    """
    if verbose_cb:
        verbose_cb(f"Archiving: {target}")

    existing = check_wayback(session, target)
    if existing:
        if verbose_cb:
            verbose_cb(f"Existing snapshot: {existing}")
        return existing

    archived = archive_url(session, target)
    if archived and verbose_cb:
        verbose_cb(f"Saved: {archived}")
    return archived
