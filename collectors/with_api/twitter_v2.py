"""
Twitter v2 API collector — DISABLED until bearer token is configured.
Activate: threadhunt config --set-key twitter_bearer_token <token>

Provides deep historical pull and full engagement data unavailable
via Nitter. Falls back gracefully if key is absent.
"""
import logging
import time
from datetime import datetime, timezone

import config
import db

logger = logging.getLogger('threadhunt')

SEARCH_URL  = 'https://api.twitter.com/2/tweets/search/recent'
USER_URL    = 'https://api.twitter.com/2/users/by/username/{username}'
TIMELINE_URL = 'https://api.twitter.com/2/users/{id}/tweets'

TWEET_FIELDS  = 'id,text,created_at,lang,author_id,conversation_id'
USER_FIELDS   = 'id,username,name,public_metrics,description,profile_image_url'
EXPANSIONS    = 'author_id'


def is_available() -> bool:
    return config.has_api_key('twitter_bearer_token')


def _headers() -> dict:
    return {'Authorization': f'Bearer {config.get_api_key("twitter_bearer_token")}'}


def _get(session, url: str, params: dict) -> dict | None:
    try:
        r = session.get(url, headers=_headers(), params=params,
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            reset = int(r.headers.get('x-rate-limit-reset', time.time() + 60))
            wait = max(0, reset - int(time.time())) + 2
            logger.warning("Twitter v2 rate limited — waiting %ds", wait)
            time.sleep(wait)
        else:
            logger.warning("Twitter v2 HTTP %d: %s", r.status_code, r.text[:200])
    except Exception as e:
        logger.error("Twitter v2 request error: %s", e)
    return None


def search_recent(session, query: str, max_results: int = 100) -> int:
    """
    Search recent tweets for a query string.
    Returns count of new posts inserted.
    """
    if not is_available():
        logger.info("Twitter v2: no bearer token — skipping")
        return 0

    cap = config.get('collector_post_cap', 500)
    new_count = 0
    next_token = None

    while new_count < min(cap, max_results):
        params = {
            'query':        query,
            'max_results':  min(100, cap - new_count),
            'tweet.fields': TWEET_FIELDS,
            'expansions':   EXPANSIONS,
            'user.fields':  USER_FIELDS,
        }
        if next_token:
            params['next_token'] = next_token

        data = _get(session, SEARCH_URL, params)
        if not data or not data.get('data'):
            break

        users = {u['id']: u for u in data.get('includes', {}).get('users', [])}

        with db.get_conn() as conn:
            for tweet in data['data']:
                author_id = tweet.get('author_id', '')
                author = users.get(author_id, {})
                username = author.get('username', f'unknown_{author_id}')
                metrics = author.get('public_metrics', {})

                account_id = db.upsert_account(
                    conn, username, 'twitter',
                    followers=metrics.get('followers_count', 0),
                    following=metrics.get('following_count', 0),
                    post_count=metrics.get('tweet_count', 0),
                    bio=author.get('description', ''),
                )
                if account_id is None:
                    continue

                created_at = tweet.get('created_at',
                                       datetime.now(timezone.utc).isoformat())
                if db.insert_post(conn, account_id, 'twitter',
                                  tweet['id'], tweet.get('text', ''),
                                  created_at, lang=tweet.get('lang')):
                    new_count += 1

        meta = data.get('meta', {})
        next_token = meta.get('next_token')
        if not next_token:
            break

        time.sleep(1)

    logger.info("Twitter v2: %d new posts for query '%s'", new_count, query)
    return new_count


def collect(session, target: str, keyword: str = None, verbose_cb=None) -> int:
    """
    Collect tweets from a specific user via Twitter v2 API.
    target: Twitter username.
    """
    if not is_available():
        logger.info("Twitter v2: bearer token not configured")
        return 0

    if verbose_cb:
        verbose_cb(f"Twitter v2: resolving @{target}")

    # Resolve username -> user ID
    user_data = _get(session, USER_URL.format(username=target),
                     {'user.fields': USER_FIELDS})
    if not user_data or not user_data.get('data'):
        logger.warning("Twitter v2: user not found: %s", target)
        return 0

    user = user_data['data']
    metrics = user.get('public_metrics', {})
    user_id = user['id']

    with db.get_conn() as conn:
        account_id = db.upsert_account(
            conn, target, 'twitter',
            followers=metrics.get('followers_count', 0),
            following=metrics.get('following_count', 0),
            post_count=metrics.get('tweet_count', 0),
            bio=user.get('description', ''),
        )

    cap = config.get('collector_post_cap', 500)
    new_count = 0
    pagination_token = None

    while new_count < cap:
        params = {
            'max_results':  min(100, cap - new_count),
            'tweet.fields': TWEET_FIELDS,
        }
        if pagination_token:
            params['pagination_token'] = pagination_token

        data = _get(session, TIMELINE_URL.format(id=user_id), params)
        if not data or not data.get('data'):
            break

        with db.get_conn() as conn:
            for tweet in data['data']:
                content = tweet.get('text', '')
                if keyword and keyword.lower() not in content.lower():
                    continue
                created_at = tweet.get('created_at',
                                       datetime.now(timezone.utc).isoformat())
                if db.insert_post(conn, account_id, 'twitter',
                                  tweet['id'], content,
                                  created_at, lang=tweet.get('lang')):
                    new_count += 1

        meta = data.get('meta', {})
        pagination_token = meta.get('next_token')
        if not pagination_token:
            break
        time.sleep(1)

    logger.info("Twitter v2: %d new posts from @%s", new_count, target)
    return new_count
