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
    keyword: optional catalog-level pre-filter (CLI --keyword flag only).
             No keyword filtering at post level — all text posts are stored.
             Analysis engine finds patterns after ingestion.
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
            logger.warning("4chan: /%s/ catalog empty or unreachable", board)
            if verbose_cb:
                verbose_cb(f"/{board}/: catalog empty or unreachable — skipping")
            continue

        logger.info("4chan: /%s/ catalog has %d threads", board, len(catalog))

        # Catalog-level filter only when --keyword is explicitly passed via CLI.
        # Otherwise collect every thread — no filtering at catalog or post level.
        if keyword:
            matched_threads = [t for t in catalog
                               if thread_matches_keywords(t, [keyword])]
            logger.info("4chan: /%s/ %d/%d threads match keyword '%s'",
                        board, len(matched_threads), len(catalog), keyword)
            if verbose_cb:
                verbose_cb(f"/{board}/: {len(matched_threads)}/{len(catalog)} threads match '{keyword}'")
        else:
            matched_threads = catalog
            if verbose_cb:
                verbose_cb(f"/{board}/: collecting all {len(catalog)} threads (no filter)")

        # Ensure a synthetic account for this board
        with db.get_conn() as conn:
            account_id = db.upsert_account(conn, f'4chan_{board}', f'4chan/{board}')

        threads_fetched = 0
        threads_404 = 0
        posts_text = 0
        posts_imageonly = 0

        for thread in matched_threads:
            if new_total >= cap:
                break

            thread_no = thread.get('no')
            if not thread_no:
                continue

            if verbose_cb:
                verbose_cb(f"/{board}/ thread #{thread_no} ({new_total} collected so far)")

            posts = get_thread(session, board, thread_no)
            if not posts:
                threads_404 += 1
                logger.debug("4chan: /%s/%s returned no posts (404 or empty)", board, thread_no)
                continue

            threads_fetched += 1
            with db.get_conn() as conn:
                for post in posts:
                    if new_total >= cap:
                        break
                    raw_com = post.get('com', '')
                    content = strip_html(raw_com).strip()
                    if not content:
                        posts_imageonly += 1
                        continue
                    posts_text += 1
                    if _ingest_post(conn, account_id, board, post):
                        new_total += 1

            time.sleep(1)  # 4chan API rate limit guidance: 1 req/sec

        logger.info(
            "4chan: /%s/ fetched=%d 404=%d text_posts=%d image_only=%d stored=%d",
            board, threads_fetched, threads_404, posts_text, posts_imageonly, new_total
        )

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
