"""
Cross-platform identity linking.
Three signals:
  1. Username similarity (Levenshtein distance ≤ threshold)
  2. Profile image hash reuse across platforms
  3. Posting time correlation (accounts posting within same 5-min windows)
"""
import logging
from datetime import timezone

import db

logger = logging.getLogger('threadhunt')

try:
    from Levenshtein import distance as lev_distance
except ImportError:
    # Pure-Python fallback (slower but functional)
    def lev_distance(a: str, b: str) -> int:
        if a == b:
            return 0
        if not a:
            return len(b)
        if not b:
            return len(a)
        prev = list(range(len(b) + 1))
        for i, ca in enumerate(a):
            curr = [i + 1]
            for j, cb in enumerate(b):
                curr.append(min(prev[j] + (0 if ca == cb else 1),
                                curr[-1] + 1, prev[j + 1] + 1))
            prev = curr
        return prev[-1]


# ── Username similarity ───────────────────────────────────────────────────────

def find_similar_usernames(username: str, platform: str, conn,
                           max_distance: int = 3, limit: int = 20) -> list:
    """
    Find accounts whose usernames are within max_distance of the target.
    Skips same username+platform combos.
    Returns list of {username, platform, distance} sorted by distance asc.
    """
    username_lower = username.lower()
    matches = []

    for row in db.stream_rows(conn,
        "SELECT username, platform FROM accounts WHERE NOT (username=? AND platform=?)",
        (username, platform)
    ):
        other = row[0].lower()
        dist = lev_distance(username_lower, other)
        if 0 < dist <= max_distance:
            matches.append({
                'username': row[0],
                'platform': row[1],
                'distance': dist,
            })

    matches.sort(key=lambda x: x['distance'])
    return matches[:limit]


# ── Profile image reuse ───────────────────────────────────────────────────────

def find_shared_profile_pics(conn, limit: int = 50) -> list:
    """
    Return groups of accounts sharing the same profile image hash
    (across same or different platforms).
    Returns list of {pic_hash, accounts: [{username, platform}], count}.
    """
    # Find hashes that appear more than once
    shared = []
    for row in db.stream_rows(conn, """
        SELECT profile_pic_hash, COUNT(*) as cnt
        FROM accounts
        WHERE profile_pic_hash IS NOT NULL AND profile_pic_hash != ''
        GROUP BY profile_pic_hash
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT ?
    """, (limit,)):
        pic_hash = row[0]
        count    = row[1]
        accounts = []
        for acct in db.stream_rows(conn,
            "SELECT username, platform FROM accounts WHERE profile_pic_hash=?",
            (pic_hash,)
        ):
            accounts.append({'username': acct[0], 'platform': acct[1]})

        shared.append({
            'pic_hash': pic_hash,
            'accounts': accounts,
            'count':    count,
        })

    return shared


# ── Posting time correlation ──────────────────────────────────────────────────

def find_time_correlated_accounts(conn, window_seconds: int = 300,
                                   min_co_occurrences: int = 3,
                                   lookback_hours: int = 48,
                                   limit: int = 30) -> list:
    """
    Find pairs of accounts that repeatedly post within window_seconds of each other.
    Useful for detecting accounts controlled by the same operator.
    Returns list of {account_a, account_b, co_occurrences, platforms}.

    Streamed in batches — builds a co-occurrence counter dict, not a matrix.
    """
    from datetime import timedelta
    cutoff = _hours_ago_iso(lookback_hours)

    # Load bounded event list
    events = []
    for row in db.stream_rows(conn, """
        SELECT account_id, timestamp, platform
        FROM posts
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
        LIMIT 5000
    """, (cutoff,)):
        dt = _parse_ts(row[1])
        if dt:
            events.append((dt, row[0], row[2]))   # (datetime, account_id, platform)

    if len(events) < 2:
        return []

    co_counts: dict = {}   # (acct_a_id, acct_b_id) -> count
    window_delta = timedelta(seconds=window_seconds)

    for i, (dt_i, acct_i, _) in enumerate(events):
        window_end = dt_i + window_delta
        j = i + 1
        while j < len(events) and events[j][0] <= window_end:
            acct_j = events[j][1]
            if acct_i != acct_j:
                key = (min(acct_i, acct_j), max(acct_i, acct_j))
                co_counts[key] = co_counts.get(key, 0) + 1
            j += 1

    # Filter by minimum co-occurrences
    results = []
    seen_ids = {k for k, v in co_counts.items() if v >= min_co_occurrences}

    if not seen_ids:
        return []

    # Resolve account IDs to usernames
    id_to_acct: dict = {}
    all_ids = set()
    for a, b in seen_ids:
        all_ids.add(a)
        all_ids.add(b)

    for aid in all_ids:
        row = conn.execute(
            "SELECT username, platform FROM accounts WHERE id=?", (aid,)
        ).fetchone()
        if row:
            id_to_acct[aid] = {'username': row[0], 'platform': row[1]}

    for (a_id, b_id), count in sorted(co_counts.items(), key=lambda x: -x[1]):
        if count < min_co_occurrences:
            continue
        if a_id not in id_to_acct or b_id not in id_to_acct:
            continue
        results.append({
            'account_a':      id_to_acct[a_id],
            'account_b':      id_to_acct[b_id],
            'co_occurrences': count,
        })
        if len(results) >= limit:
            break

    return results


# ── Full link report ──────────────────────────────────────────────────────────

def run_identity_linking(conn) -> dict:
    """
    Run all three identity signals. Returns combined report dict.

    Candidate accounts for username linking:
    1. All Telegram/Nitter/Twitter/VK accounts — real usernames with cross-platform signal
    2. Accounts that appear in any active campaign cluster
    The flagged=1 prerequisite is removed — it was a circular dependency that
    prevented linking from ever running.
    """
    username_links  = []
    shared_pics     = find_shared_profile_pics(conn)
    time_correlated = find_time_correlated_accounts(conn)

    # Collect candidates: real-account platforms + campaign members
    candidates = {}  # (username, platform) keyed for dedup

    for row in db.stream_rows(conn, """
        SELECT username, platform FROM accounts
        WHERE platform IN ('telegram', 'nitter', 'twitter', 'vk')
        LIMIT 500
    """):
        candidates[(row[0], row[1])] = True

    for row in db.stream_rows(conn, """
        SELECT DISTINCT a.username, a.platform
        FROM accounts a
        JOIN posts p ON p.account_id = a.id
        JOIN clusters cl ON cl.post_id = p.id
        JOIN campaigns c ON c.id = cl.campaign_id
        WHERE c.active = 1
        LIMIT 200
    """):
        candidates[(row[0], row[1])] = True

    for (username, platform) in candidates:
        matches = find_similar_usernames(username, platform, conn)
        if matches:
            username_links.append({
                'source':  {'username': username, 'platform': platform},
                'matches': matches,
            })

    return {
        'username_links':  username_links,
        'shared_pics':     shared_pics,
        'time_correlated': time_correlated,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(ts: str):
    from datetime import datetime
    if not ts:
        return None
    ts = ts.rstrip('Z').split('+')[0]
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def _hours_ago_iso(hours: int) -> str:
    from datetime import datetime, timedelta
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
