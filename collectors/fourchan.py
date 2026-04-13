"""
4chan collector — uses the official free JSON API. No key required.
Monitors /pol/, /k/, /int/ by default.
Polls catalog every 5 minutes. Matches against watchlist keywords.
Archives matching threads before they 404.
Cap: 1000 posts per run.
"""
import time
import logging
import re
from datetime import datetime, timezone

import config
import db
from utils.text import strip_html

logger = logging.getLogger('threadhunt')

CATALOG_URL = 'https://a.4cdn.org/{board}/catalog.json'
THREAD_URL  = 'https://a.4cdn.org/{board}/thread/{no}.json'
POLL_INTERVAL = 300   # 5 minutes


# ── API helpers ───────────────────────────────────────────────────────────────

def get_catalog(session, board: str) -> list:
    """
    Fetch catalog for a board. Returns flat list of thread stubs.
    Each stub: {no, sub, com, replies, last_modified}
    """
    url = CATALOG_URL.format(board=board)
    try:
        r = session.get(url, timeout=config.get('request_timeout', 10))
        r.raise_for_status()
        pages = r.json()
        threads = []
        for page in pages:
            threads.extend(page.get('threads', []))
        return threads
    except Exception as e:
        logger.warning("4chan catalog error /%s/: %s", board, e)
        return []


def get_thread(session, board: str, thread_no: int) -> list:
    """
    Fetch all posts in a thread. Returns list of post dicts.
    Each post: {no, name, com, time, filename}
    """
    url = THREAD_URL.format(board=board, no=thread_no)
    try:
        r = session.get(url, timeout=config.get('request_timeout', 10))
        if r.status_code == 404:
            logger.debug("4chan thread /%s/%d already 404", board, thread_no)
            return []
        r.raise_for_status()
        return r.json().get('posts', [])
    except Exception as e:
        logger.warning("4chan thread error /%s/%d: %s", board, thread_no, e)
        return []


# ── Keyword matching ──────────────────────────────────────────────────────────

def thread_matches_keywords(thread: dict, keywords: list) -> bool:
    """True if any keyword appears in thread subject or comment."""
    if not keywords:
        return True
    text = ' '.join([
        thread.get('sub', ''),
        strip_html(thread.get('com', '')),
    ]).lower()
    return any(kw.lower() in text for kw in keywords)


def _get_watchlist_keywords(conn) -> list:
    """Pull keyword-type watchlist items from DB."""
    keywords = []
    for row in db.stream_rows(conn, "SELECT value FROM watchlist WHERE type='keyword'"):
        keywords.append(row['value'])
    return keywords


# ── Post ingestion ────────────────────────────────────────────────────────────

def _ingest_post(conn, account_id: int, board: str, post: dict) -> bool:
    """Parse a 4chan post dict and insert into DB. Returns True if new."""
    raw_com = post.get('com', '')
    content = strip_html(raw_com).strip()
    if not content:
        return False

    post_id = str(post.get('no', ''))
    if not post_id:
        return False

    ts_unix = post.get('time')
    if ts_unix:
        timestamp = datetime.fromtimestamp(int(ts_unix), tz=timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    return db.insert_post(conn, account_id, f'4chan/{board}', post_id, content, timestamp)


# ── Main entry point ──────────────────────────────────────────────────────────

def collect(session, target: str = None, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect posts from 4chan.
    target: board name (e.g. 'pol') — if None, uses configured board list.
    keyword: filter string — if None, uses watchlist keywords.
    Returns count of new posts inserted.
    """
    cap = config.get('fourchan_post_cap', 1000)
    boards = [target] if target else config.get('fourchan_boards', ['pol', 'k', 'int'])

    new_total = 0

    for board in boards:
        if new_total >= cap:
            break

        if verbose_cb:
            verbose_cb(f"Fetching /{board}/ catalog")

        catalog = get_catalog(session, board)
        if not catalog:
            continue

        with db.get_conn() as conn:
            keywords = [keyword] if keyword else _get_watchlist_keywords(conn)

        matched_threads = [t for t in catalog if thread_matches_keywords(t, keywords)]

        if verbose_cb:
            verbose_cb(f"/{board}/: {len(matched_threads)} matching threads")

        # Ensure a synthetic account for this board
        with db.get_conn() as conn:
            account_id = db.upsert_account(conn, f'4chan_{board}', f'4chan/{board}')

        for thread in matched_threads:
            if new_total >= cap:
                break

            thread_no = thread.get('no')
            if not thread_no:
                continue

            if verbose_cb:
                verbose_cb(f"Archiving /{board}/ thread #{thread_no}")

            posts = get_thread(session, board, thread_no)
            if not posts:
                continue

            with db.get_conn() as conn:
                for post in posts:
                    if new_total >= cap:
                        break
                    if _ingest_post(conn, account_id, board, post):
                        new_total += 1

            time.sleep(1)  # 4chan API rate limit guidance: 1 req/sec

    logger.info("4chan: %d new posts collected", new_total)
    return new_total


def monitor(session, boards: list = None, poll_interval: int = POLL_INTERVAL,
            verbose_cb=None, stop_event=None):
    """
    Continuous monitor — polls catalogs every poll_interval seconds.
    stop_event: threading.Event to stop the loop.
    Not called by default; used for long-running daemon mode.
    """
    boards = boards or config.get('fourchan_boards', ['pol', 'k', 'int'])
    while not (stop_event and stop_event.is_set()):
        for board in boards:
            collect(session, target=board, verbose_cb=verbose_cb)
        if verbose_cb:
            verbose_cb(f"Next poll in {poll_interval}s")
        for _ in range(poll_interval):
            if stop_event and stop_event.is_set():
                break
            time.sleep(1)
