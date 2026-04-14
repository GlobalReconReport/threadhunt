"""
Nitter collector — Twitter/X data without the API.

Primary path: rotates through a pool of public Nitter instances.
Fallback A: Nitter search endpoint        (from:{username} when timeline returns 0).
Fallback B: Playwright headless Firefox   (handles JS-challenged instances like nitter.net).
Fallback C: Twitter guest-token API       (api.twitter.com v1.1 with public bearer).
Fallback D: Twitter CDN syndication       (cdn.syndication.twimg.com, ~20 tweets).

All paths are attempted in order.  If all fail, collect() returns 0 and logs.
No API key required — uses Twitter's own unauthenticated web-app bearer token.
Playwright paths only activate when `playwright` + `firefox` binary are installed.

Nitter search also supports pure keyword collection:
  collect(session, target='', keyword='nato')  → /search?f=tweets&q=nato

Health-checks Nitter on first use; dead instances dropped silently.
Cap: 500 posts per target per run (configurable).
"""
import re
import json
import time
import logging
import hashlib
from datetime import datetime, timezone

from bs4 import BeautifulSoup

import config
import db

logger = logging.getLogger('threadhunt')

# ── Optional Playwright backend ───────────────────────────────────────────────
try:
    from collectors import nitter_playwright as _pw
    _PW_AVAILABLE = _pw.is_available()
except Exception:
    _pw = None           # type: ignore[assignment]
    _PW_AVAILABLE = False

# ── Twitter direct-access constants ──────────────────────────────────────────
# Public bearer token used by twitter.com's own web app for unauthenticated
# requests.  Widely documented in open-source scrapers; usable until revoked.
_TW_BEARER = (
    'AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs'
    '%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
)
_TW_GUEST_ACTIVATE = 'https://api.twitter.com/1.1/guest/activate.json'
_TW_TIMELINE_URL   = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
_TW_SYNDICATION    = 'https://cdn.syndication.twimg.com/timeline/profile'

_live_instances: list | None = None
_search_instances: list | None = None   # subset of _live_instances that support /search


# ── Instance management ───────────────────────────────────────────────────────

_HEALTH_HEADERS = {
    'User-Agent':      ('Mozilla/5.0 (X11; Linux x86_64; rv:109.0) '
                        'Gecko/20100101 Firefox/115.0'),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'DNT':             '1',
}

# Marker present in Nitter's own HTML but absent from proxy/portal redirect pages
_NITTER_MARKER = 'nitter'
# Marker in Nitter search results page (the search form or a timeline entry)
_SEARCH_MARKER  = 'timeline-item'


def health_check_instances(session) -> list:
    """
    GET / on each instance with timeout=5.  Return only responsive ones.
    Also probes /search?f=tweets&q=test to build the search-capable subset.
    """
    global _live_instances, _search_instances
    instances = config.get('nitter_instances', [])
    live   = []
    search = []
    for inst in instances:
        try:
            r = session.get(inst + '/', timeout=5, allow_redirects=True,
                            headers=_HEALTH_HEADERS)
            if r.status_code != 200 or _NITTER_MARKER not in r.text.lower():
                continue
            live.append(inst)
            # Probe search endpoint — some instances disable it.
            # A real Nitter search page contains its own search form; a disabled
            # or redirected instance does not (even if "nitter" appears in URLs).
            try:
                sr = session.get(
                    inst + '/search?f=tweets&q=test',
                    timeout=5,
                    allow_redirects=True,
                    headers=_HEALTH_HEADERS,
                )
                if (sr.status_code == 200
                        and 'name="q"' in sr.text
                        and 'search-container' in sr.text.lower()):
                    search.append(inst)
            except Exception:
                pass
        except Exception:
            pass
    _live_instances   = live
    _search_instances = search
    logger.info(
        "Nitter health check: %d/%d live, %d support search",
        len(live), len(instances), len(search),
    )
    return live


def _instances(session) -> list:
    global _live_instances
    if _live_instances is None:
        health_check_instances(session)
    return _live_instances or []


def _search_capable_instances(session) -> list:
    global _search_instances
    if _search_instances is None:
        health_check_instances(session)
    return _search_instances or []


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


def _fetch_search(session, path: str) -> BeautifulSoup | None:
    """Try search-capable instances only. Return parsed HTML or None."""
    for inst in _search_capable_instances(session):
        url = inst + path
        try:
            r = session.get(url, timeout=config.get('request_timeout', 10))
            if r.status_code == 200:
                return BeautifulSoup(r.text, 'html.parser')
            if r.status_code == 429:
                continue
        except Exception as e:
            logger.debug("Nitter search %s error: %s", inst, e)
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


def scrape_search(session, query: str, cap: int) -> list:
    """
    Scrape Nitter's /search?f=tweets&q=QUERY page.
    Only attempts instances confirmed to support search during health check.
    Works for both user searches (query='from:username') and keyword searches.
    Paginates via cursor exactly like scrape_timeline.
    Returns list of {post_id, content, timestamp} dicts (up to cap).
    """
    import urllib.parse
    if not _search_capable_instances(session):
        logger.debug("Nitter search: no search-capable instances available")
        return []

    path = f'/search?f=tweets&q={urllib.parse.quote(query)}'
    posts: list = []

    while len(posts) < cap:
        soup = _fetch_search(session, path)
        if not soup:
            break

        items = soup.select('.timeline-item:not(.show-more)')
        if not items:
            break

        page_count = 0
        for item in items:
            if len(posts) >= cap:
                break
            post = _parse_item(item)
            if post:
                posts.append(post)
                page_count += 1

        if page_count == 0:
            break

        show_more = soup.select_one('.show-more a')
        if not show_more or not show_more.get('href'):
            break
        href = show_more['href']
        m = re.search(r'cursor=([^&]+)', href)
        if not m:
            break
        path = f'/search?f=tweets&q={urllib.parse.quote(query)}&cursor={m.group(1)}'
        time.sleep(1.2)

    logger.info("Nitter search '%s': %d posts", query, len(posts))
    return posts


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


# ── Twitter direct-access fallbacks ──────────────────────────────────────────

def _tw_guest_token(session) -> str | None:
    """
    Acquire a short-lived guest token from Twitter's public activation endpoint.
    Used alongside the bearer token for unauthenticated API calls.
    """
    try:
        r = session.post(
            _TW_GUEST_ACTIVATE,
            headers={'Authorization': f'Bearer {_TW_BEARER}'},
            timeout=config.get('request_timeout', 10),
        )
        if r.status_code == 200:
            return r.json().get('guest_token')
    except Exception as e:
        logger.debug("Twitter guest token error: %s", e)
    return None


def _tw_api_headers(guest_token: str) -> dict:
    return {
        'Authorization':         f'Bearer {_TW_BEARER}',
        'x-guest-token':         guest_token,
        'x-twitter-active-user': 'yes',
        'Accept-Language':       'en-US,en;q=0.9',
    }


def _scrape_twitter_api(session, username: str, cap: int) -> list:
    """
    Fallback A: Twitter v1.1 user_timeline with guest token.
    Returns list of {post_id, content, timestamp} dicts or [] on failure.
    Paginates via max_id until cap reached or no more tweets.
    """
    guest_token = _tw_guest_token(session)
    if not guest_token:
        logger.debug("Twitter direct: could not acquire guest token")
        return []

    headers = _tw_api_headers(guest_token)
    posts   = []
    max_id  = None

    while len(posts) < cap:
        params: dict = {
            'screen_name':    username,
            'count':          200,
            'tweet_mode':     'extended',
            'exclude_replies': 'false',
            'include_rts':    'true',
        }
        if max_id:
            params['max_id'] = max_id

        try:
            r = session.get(
                _TW_TIMELINE_URL,
                params=params,
                headers=headers,
                timeout=config.get('request_timeout', 15),
            )
            if r.status_code == 401:
                logger.debug("Twitter direct API: 401 — guest token rejected")
                break
            if r.status_code == 404:
                logger.debug("Twitter direct API: 404 — account @%s not found", username)
                break
            if r.status_code != 200:
                logger.debug("Twitter direct API: HTTP %d", r.status_code)
                break
            tweets = r.json()
            if not isinstance(tweets, list) or not tweets:
                break
        except Exception as e:
            logger.debug("Twitter direct API error: %s", e)
            break

        page_new = 0
        for tw in tweets:
            if len(posts) >= cap:
                break
            tw_id = str(tw.get('id', ''))
            if not tw_id:
                continue
            # Skip the max_id tweet itself (pagination overlap)
            if max_id and int(tw_id) >= int(max_id):
                continue

            content = (tw.get('full_text') or tw.get('text', '')).strip()
            if not content:
                continue

            ts_raw = tw.get('created_at', '')
            try:
                dt = datetime.strptime(ts_raw, '%a %b %d %H:%M:%S +0000 %Y')
                timestamp = dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                timestamp = _now_iso()

            posts.append({'post_id': tw_id, 'content': content, 'timestamp': timestamp})
            page_new += 1

        if page_new == 0:
            break

        # Next page: oldest tweet id minus 1
        oldest = min(int(tw['id']) for tw in tweets if tw.get('id'))
        max_id = str(oldest - 1)
        time.sleep(1.0)

    logger.info("Twitter direct API: got %d posts for @%s", len(posts), username)
    return posts


def _scrape_twitter_syndication(session, username: str, cap: int) -> list:
    """
    Fallback B: Twitter CDN syndication endpoint used by embedded timelines.
    Less capable than the API (no pagination, ~20 recent tweets) but very
    stable as it's used by Twitter's own embeds on third-party sites.
    Returns list of {post_id, content, timestamp} dicts or [] on failure.
    """
    try:
        r = session.get(
            _TW_SYNDICATION,
            params={
                'screen_name': username,
                'lang':        'en',
                'dnt':         '1',
                'limit':       min(cap, 20),
            },
            headers={
                'User-Agent':    'Mozilla/5.0 (compatible; Googlebot/2.1)',
                'Accept':        'application/json',
                'Referer':       'https://platform.twitter.com/',
            },
            timeout=config.get('request_timeout', 15),
        )
        if r.status_code != 200:
            logger.debug("Twitter syndication: HTTP %d for @%s", r.status_code, username)
            return []
        data = r.json()
    except Exception as e:
        logger.debug("Twitter syndication error: %s", e)
        return []

    posts  = []
    tweets = data.get('timeline', data.get('tweets', []))
    for tw in tweets:
        if len(posts) >= cap:
            break
        tw_id = str(tw.get('id_str', tw.get('id', '')))
        if not tw_id:
            continue

        content = (tw.get('full_text') or tw.get('text', '')).strip()
        if not content:
            continue

        ts_raw = tw.get('created_at', '')
        try:
            dt = datetime.strptime(ts_raw, '%a %b %d %H:%M:%S +0000 %Y')
            timestamp = dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            timestamp = _now_iso()

        posts.append({'post_id': tw_id, 'content': content, 'timestamp': timestamp})

    logger.info("Twitter syndication: got %d posts for @%s", len(posts), username)
    return posts


# ── Public entry point ────────────────────────────────────────────────────────

def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect posts from a Twitter/X username (or keyword search when target is empty).
    Tries paths in order:
      1. Nitter timeline scrape             (primary — profile timeline)
      2. Nitter search: from:{username}     (fallback — when timeline returns 0)
      3. Playwright timeline                (JS browser — handles Cloudflare instances)
      4. Playwright search: from:{username} (JS browser search fallback)
      5. Playwright keyword search          (JS browser — keyword-only mode top-up)
      6. Twitter guest-token API            (direct Twitter v1.1)
      7. Twitter CDN syndication            (~20 tweets, very stable)
    Paths 3-5 only run when playwright + firefox binary are installed.
    Returns count of new posts inserted.
    """
    cap = config.get('collector_post_cap', 500)

    # ── Health check ──────────────────────────────────────────────────────────
    if verbose_cb:
        verbose_cb("Checking Nitter instances...")
    health_check_instances(session)

    raw_posts = []
    path_used = None
    profile: dict = {}

    # ── Keyword-only mode (no username target) ────────────────────────────────
    if not target and keyword:
        if _search_capable_instances(session):
            if verbose_cb:
                verbose_cb(f"Nitter search: keyword '{keyword}'")
            raw_posts = scrape_search(session, keyword, cap=cap)
            if raw_posts:
                path_used = 'nitter_search_keyword'
        if not raw_posts:
            logger.warning("Nitter keyword search: no results for '%s'", keyword)
            if verbose_cb:
                verbose_cb(f"Nitter keyword search: no results for '{keyword}'")
            return 0
        # Ingest under a synthetic account name for keyword searches
        target = f'search:{keyword}'

    # ── Path 1: Nitter timeline ───────────────────────────────────────────────
    if not raw_posts and target and _instances(session):
        if verbose_cb:
            verbose_cb(f"Nitter: {len(_instances(session))} live — scraping @{target}")
        profile = scrape_profile(session, target)
        raw_posts = list(scrape_timeline(session, target, cap=cap))
        if raw_posts:
            path_used = 'nitter'
        else:
            logger.info("Nitter timeline: 0 posts from @%s", target)
            profile = {}

    # ── Path 2: Nitter search from:{username} ────────────────────────────────
    if not raw_posts and target and not target.startswith('search:') and _search_capable_instances(session):
        if verbose_cb:
            verbose_cb(f"Nitter timeline empty — trying search fallback for @{target}")
        raw_posts = scrape_search(session, f'from:{target}', cap=cap)
        if raw_posts:
            path_used = 'nitter_search'
            logger.info("Nitter search fallback: %d posts for @%s", len(raw_posts), target)

    # ── Path 3: Playwright — timeline (JS-capable browser) ───────────────────
    # Tries ALL configured instances, including those blocked to plain HTTP.
    # Only runs when playwright + firefox binary are present.
    if not raw_posts and target and not target.startswith('search:') and _PW_AVAILABLE:
        if verbose_cb:
            verbose_cb(f"Requests paths empty — trying Playwright browser for @{target}")
        all_instances = config.get('nitter_instances', [])
        raw_posts = _pw.scrape_timeline_pw(all_instances, target, cap)
        if raw_posts:
            path_used = 'nitter_playwright'

    # ── Path 4: Playwright — search from:{username} ───────────────────────────
    if not raw_posts and target and not target.startswith('search:') and _PW_AVAILABLE:
        if verbose_cb:
            verbose_cb(f"Playwright timeline empty — trying Playwright search for @{target}")
        all_instances = config.get('nitter_instances', [])
        raw_posts = _pw.scrape_search_pw(all_instances, f'from:{target}', cap)
        if raw_posts:
            path_used = 'nitter_playwright_search'

    # ── Path 5: Playwright — keyword search (keyword-only mode top-up) ────────
    # Keyword-only mode already tried requests-based search above; if that found
    # nothing and Playwright is available, try again with a real browser.
    if not raw_posts and not target and keyword and _PW_AVAILABLE:
        if verbose_cb:
            verbose_cb(f"Playwright keyword search: '{keyword}'")
        all_instances = config.get('nitter_instances', [])
        raw_posts = _pw.scrape_search_pw(all_instances, keyword, cap)
        if raw_posts:
            path_used = 'nitter_playwright_keyword'
            target = f'search:{keyword}'

    # ── Path 6: Twitter guest API ─────────────────────────────────────────────
    if not raw_posts and target and not target.startswith('search:'):
        if verbose_cb:
            verbose_cb(f"Nitter failed — trying Twitter guest API for @{target}")
        raw_posts = _scrape_twitter_api(session, target, cap)
        if raw_posts:
            path_used = 'twitter_api'

    # ── Path 7: Twitter syndication CDN ──────────────────────────────────────
    if not raw_posts and target and not target.startswith('search:'):
        if verbose_cb:
            verbose_cb(f"API failed — trying Twitter syndication for @{target}")
        raw_posts = _scrape_twitter_syndication(session, target, cap)
        if raw_posts:
            path_used = 'twitter_syndication'

    if not raw_posts:
        logger.warning(
            "Twitter/X: all paths failed for @%s (Nitter dead/blocked, "
            "Playwright %s, guest API blocked, syndication unavailable)",
            target,
            'unavailable' if not _PW_AVAILABLE else 'returned 0',
        )
        if verbose_cb:
            verbose_cb(f"@{target}: all collection paths failed")
        return 0

    logger.info("Twitter: collecting @%s via %s (%d posts)", target, path_used, len(raw_posts))

    # ── Ingest ────────────────────────────────────────────────────────────────
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

        for post in raw_posts:
            if keyword and keyword.lower() not in post['content'].lower():
                continue
            if db.insert_post(conn, account_id, 'nitter',
                              post['post_id'], post['content'], post['timestamp']):
                new_count += 1
                if verbose_cb and new_count % 50 == 0:
                    verbose_cb(f"{new_count} new posts so far...")

    logger.info("Twitter/X (%s): %d new posts from @%s", path_used, new_count, target)
    return new_count
