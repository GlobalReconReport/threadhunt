"""
Playwright-based Nitter collector — optional module.

Activates only when `playwright` is installed and the Firefox browser binary
is present (`playwright install firefox`).  Degrades gracefully to a no-op
when either is missing.

Why Playwright?
  Some Nitter instances (e.g. nitter.net) sit behind Cloudflare or other JS
  challenges that return an empty body to plain HTTP clients.  A real browser
  passes those checks and can render the full timeline/search page.

Usage from nitter.py (automatic — no direct calls needed):
  The collect() function in nitter.py imports this module and attempts
  Playwright paths after all requests-based paths have failed.

Exported API:
  is_available() -> bool
  scrape_timeline_pw(instances, username, cap) -> list[dict]
  scrape_search_pw(instances, query, cap)     -> list[dict]

Each returned dict: {post_id, content, timestamp}  (same shape as nitter.py)
"""
import re
import logging
import urllib.parse
from datetime import datetime, timezone

logger = logging.getLogger('threadhunt')

# ── Optional import ───────────────────────────────────────────────────────────

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    _PW_IMPORT_OK = True
except ImportError:
    _PW_IMPORT_OK = False

# Browser-level singleton — lazily created, reused within a process lifetime
_browser = None
_playwright_ctx = None


def is_available() -> bool:
    """True when playwright is importable and the Firefox binary is present."""
    if not _PW_IMPORT_OK:
        return False
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            # executable_path check without launching — just resolve the path
            browser_type = pw.firefox
            path = browser_type.executable_path
            import os
            return bool(path and os.path.exists(path))
    except Exception:
        return False


# ── Browser lifecycle ─────────────────────────────────────────────────────────

def _get_browser():
    """Lazily launch a headless Firefox instance; reuse across calls."""
    global _browser, _playwright_ctx
    if _browser is not None:
        return _browser
    if not _PW_IMPORT_OK:
        return None
    try:
        from playwright.sync_api import sync_playwright
        _playwright_ctx = sync_playwright().start()
        _browser = _playwright_ctx.firefox.launch(headless=True)
        logger.debug("Playwright: Firefox launched")
        return _browser
    except Exception as e:
        logger.debug("Playwright: could not launch Firefox — %s", e)
        _browser = None
        return None


def shutdown():
    """Close browser and Playwright context.  Call on process exit if needed."""
    global _browser, _playwright_ctx
    try:
        if _browser:
            _browser.close()
    except Exception:
        pass
    try:
        if _playwright_ctx:
            _playwright_ctx.stop()
    except Exception:
        pass
    _browser = None
    _playwright_ctx = None


# ── Page helpers ──────────────────────────────────────────────────────────────

_PAGE_TIMEOUT   = 30_000   # ms — navigation
_ITEM_TIMEOUT   = 10_000   # ms — wait for first .timeline-item to appear
_NITTER_MARKER  = 'nitter'

_BROWSER_HEADERS = {
    'Accept-Language': 'en-US,en;q=0.9',
    'DNT':             '1',
}


def _open_page(browser, url: str):
    """
    Open a new browser page, navigate to url, wait for .timeline-item or
    the search form to appear.  Returns (page, html) or (None, '') on failure.
    """
    page = None
    try:
        page = browser.new_page(extra_http_headers=_BROWSER_HEADERS)
        page.set_default_navigation_timeout(_PAGE_TIMEOUT)
        page.goto(url, wait_until='domcontentloaded')

        # Wait for either a timeline item or the search input to confirm Nitter loaded
        try:
            page.wait_for_selector(
                '.timeline-item, input[name="q"]',
                timeout=_ITEM_TIMEOUT,
            )
        except Exception:
            pass  # page may still have content; let the caller decide

        html = page.content()
        return page, html
    except Exception as e:
        logger.debug("Playwright: page error for %s — %s", url, e)
        if page:
            try:
                page.close()
            except Exception:
                pass
        return None, ''


def _advance_cursor(page, query_prefix: str) -> str | None:
    """
    Read the cursor from the 'Load more' / 'show-more' link currently in the
    DOM and return the next path to navigate to, or None if pagination ended.
    query_prefix: the path+query string up to (not including) &cursor=
    """
    try:
        link = page.query_selector('.show-more a')
        if not link:
            return None
        href = link.get_attribute('href') or ''
        m = re.search(r'cursor=([^&]+)', href)
        if not m:
            return None
        return f'{query_prefix}&cursor={m.group(1)}'
    except Exception:
        return None


# ── HTML parsing (reuses logic from nitter.py) ────────────────────────────────

def _parse_nitter_date(raw: str) -> str:
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


def _parse_items_from_html(html: str) -> list[dict]:
    """
    Parse rendered Nitter HTML with BeautifulSoup.
    Replicates _parse_item() from nitter.py but operates on a full page string.
    Returns list of {post_id, content, timestamp}.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    results = []
    for item in soup.select('.timeline-item:not(.show-more)'):
        try:
            content_el = item.select_one('.tweet-content')
            if not content_el:
                continue
            content = content_el.get_text(separator=' ', strip=True)
            if not content:
                continue

            link = item.select_one('.tweet-link')
            if not link:
                continue
            m = re.search(r'/status/(\d+)', link.get('href', ''))
            if not m:
                continue
            post_id = m.group(1)

            ts_el   = item.select_one('.tweet-date a')
            raw_ts  = (ts_el['title'] if (ts_el and ts_el.get('title')) else '')
            timestamp = _parse_nitter_date(raw_ts) if raw_ts else _now_iso()

            results.append({'post_id': post_id, 'content': content, 'timestamp': timestamp})
        except Exception as e:
            logger.debug("Playwright item parse error: %s", e)
    return results


def _html_is_nitter(html: str) -> bool:
    """Return True if the rendered HTML looks like a Nitter page (not a challenge/redirect)."""
    low = html.lower()
    return _NITTER_MARKER in low and ('<div class="timeline"' in low or 'timeline-item' in low
                                       or 'search-container' in low)


# ── Public scraping functions ─────────────────────────────────────────────────

def scrape_timeline_pw(instances: list, username: str, cap: int) -> list:
    """
    Scrape a user's Nitter timeline using a headless Firefox browser.
    Tries each instance in order until one yields results.
    Returns list of {post_id, content, timestamp} dicts (up to cap).
    """
    browser = _get_browser()
    if not browser:
        return []

    for inst in instances:
        posts: list[dict] = []
        base_path = f'/{username}'
        url       = inst + base_path
        logger.debug("Playwright timeline: trying %s", url)

        page, html = _open_page(browser, url)
        if not page:
            continue

        try:
            if not _html_is_nitter(html):
                logger.debug("Playwright: %s did not serve Nitter HTML", inst)
                continue

            while len(posts) < cap:
                batch = _parse_items_from_html(page.content())
                if not batch:
                    break

                new = 0
                for p in batch:
                    if len(posts) >= cap:
                        break
                    if not any(x['post_id'] == p['post_id'] for x in posts):
                        posts.append(p)
                        new += 1
                if new == 0:
                    break

                # Pagination
                next_path = _advance_cursor(page, f'/{username}?')
                if not next_path:
                    break
                try:
                    page.goto(inst + next_path, wait_until='domcontentloaded')
                    page.wait_for_selector('.timeline-item', timeout=_ITEM_TIMEOUT)
                except Exception:
                    break

            if posts:
                logger.info("Playwright timeline: %d posts for @%s via %s",
                            len(posts), username, inst)
                return posts
        finally:
            try:
                page.close()
            except Exception:
                pass

    return []


def scrape_search_pw(instances: list, query: str, cap: int) -> list:
    """
    Scrape Nitter search results using a headless Firefox browser.
    Tries each instance in order; skips instances that don't serve a search form.
    Returns list of {post_id, content, timestamp} dicts (up to cap).
    """
    browser = _get_browser()
    if not browser:
        return []

    q_enc     = urllib.parse.quote(query)
    base_qs   = f'/search?f=tweets&q={q_enc}'

    for inst in instances:
        posts: list[dict] = []
        url = inst + base_qs
        logger.debug("Playwright search: trying %s", url)

        page, html = _open_page(browser, url)
        if not page:
            continue

        try:
            # Must have the Nitter search form — rejects portal/redirect pages
            if 'name="q"' not in html or 'search-container' not in html.lower():
                logger.debug("Playwright: %s search page missing form — skipping", inst)
                continue

            while len(posts) < cap:
                batch = _parse_items_from_html(page.content())
                if not batch:
                    break

                new = 0
                for p in batch:
                    if len(posts) >= cap:
                        break
                    if not any(x['post_id'] == p['post_id'] for x in posts):
                        posts.append(p)
                        new += 1
                if new == 0:
                    break

                next_path = _advance_cursor(page, f'/search?f=tweets&q={q_enc}')
                if not next_path:
                    break
                try:
                    page.goto(inst + next_path, wait_until='domcontentloaded')
                    page.wait_for_selector('.timeline-item', timeout=_ITEM_TIMEOUT)
                except Exception:
                    break

            if posts:
                logger.info("Playwright search: %d posts for query '%s' via %s",
                            len(posts), query, inst)
                return posts
        finally:
            try:
                page.close()
            except Exception:
                pass

    return []
