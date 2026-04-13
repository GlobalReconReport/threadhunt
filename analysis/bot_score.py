"""
Heuristic bot scoring — 0.0 to 1.0. No ML. No external deps.
Five weighted factors; each sub-scores 0.0–1.0.
Threshold default: 0.7 (configurable).
"""
import math
import logging
from datetime import datetime, timezone, timedelta

import config
import db
from utils.text import has_digit_suffix, digit_ratio, username_entropy

logger = logging.getLogger('threadhunt')


# ── Factor weights ─────────────────────────────────────────────────────────────
WEIGHTS = {
    'username':      0.20,
    'follower_ratio':0.25,
    'post_frequency':0.25,
    'pic_reuse':     0.15,
    'content_dup':   0.15,
}


def compute_bot_score(account_id: int, conn) -> float:
    """
    Compute bot score for an account using DB data.
    Returns 0.0–1.0. Also updates accounts.bot_score and accounts.flagged.
    """
    row = conn.execute(
        "SELECT * FROM accounts WHERE id=?", (account_id,)
    ).fetchone()
    if not row:
        return 0.0

    sub_scores = {}

    sub_scores['username']       = _username_score(dict(row)['username'])
    sub_scores['follower_ratio'] = _follower_ratio_score(
        dict(row)['followers'], dict(row)['following']
    )
    sub_scores['post_frequency'] = _post_frequency_score(account_id, conn)
    sub_scores['pic_reuse']      = _pic_reuse_score(dict(row)['profile_pic_hash'], conn)
    sub_scores['content_dup']    = _content_duplication_score(account_id, conn)

    total = sum(sub_scores[k] * WEIGHTS[k] for k in WEIGHTS)
    score = min(1.0, max(0.0, total))

    threshold = config.get('bot_score_threshold', 0.7)
    flagged   = 1 if score >= threshold else 0

    conn.execute(
        "UPDATE accounts SET bot_score=?, flagged=? WHERE id=?",
        (round(score, 3), flagged, account_id)
    )
    return score


def _username_score(username: str) -> float:
    """
    Score based on username randomness indicators.
    Digit suffix, high digit ratio, high entropy, unusual length.
    """
    if not username:
        return 0.5

    score = 0.0
    checks = 0

    # Digit suffix (e.g. JohnSmith1234567)
    if has_digit_suffix(username):
        score += 1.0
    checks += 1

    # Digit ratio > 40%
    if digit_ratio(username) > 0.4:
        score += 1.0
    checks += 1

    # Very high entropy (near-random character distribution)
    ent = username_entropy(username)
    if ent > 3.5:
        score += 1.0
    elif ent > 3.0:
        score += 0.5
    checks += 1

    # Unusually long username (>20 chars with digits)
    if len(username) > 20 and any(c.isdigit() for c in username):
        score += 0.5
    checks += 1

    return min(1.0, score / checks) if checks else 0.0


def _follower_ratio_score(followers: int, following: int) -> float:
    """
    Bots often have: very low followers + high following, or
    suspiciously round follower counts, or >1000 following with <10 followers.
    """
    followers = followers or 0
    following = following or 0

    if followers == 0 and following == 0:
        return 0.3   # Unknown — mild suspicion

    if followers == 0:
        return 0.9   # Following many but no followers

    ratio = following / followers

    if ratio > 50:
        return 1.0
    elif ratio > 20:
        return 0.85
    elif ratio > 10:
        return 0.65
    elif ratio > 5:
        return 0.45
    elif followers < 10 and following > 100:
        return 0.75
    else:
        return max(0.0, (ratio - 1) / 10)   # Normal range: low score


def _post_frequency_score(account_id: int, conn) -> float:
    """
    Compute posts/hour for this account over collected data window.
    Abnormally high frequency (>10/hr sustained) → high score.
    """
    # Get timestamps of recent posts
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp FROM posts
        WHERE account_id=?
        ORDER BY timestamp DESC
        LIMIT 500
    """, (account_id,))

    timestamps = []
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        for row in batch:
            ts = row[0]
            if ts:
                timestamps.append(ts)

    if len(timestamps) < 5:
        return 0.1   # Too few posts to assess

    from utils.text import posts_per_hour as _pph
    rate = _pph(timestamps)

    # Scoring thresholds (posts/hour)
    if rate > 50:   return 1.0
    if rate > 20:   return 0.9
    if rate > 10:   return 0.75
    if rate > 5:    return 0.55
    if rate > 2:    return 0.3
    return 0.1


def _pic_reuse_score(pic_hash: str | None, conn) -> float:
    """
    If the same profile image hash appears on 2+ other accounts → likely bot farm.
    """
    if not pic_hash:
        return 0.2   # No pic on file — mild suspicion

    count = conn.execute(
        "SELECT COUNT(*) FROM accounts WHERE profile_pic_hash=?", (pic_hash,)
    ).fetchone()[0]

    if count >= 5:   return 1.0
    if count >= 3:   return 0.8
    if count >= 2:   return 0.6
    return 0.0


def _content_duplication_score(account_id: int, conn) -> float:
    """
    Fraction of this account's posts that are near-duplicates of posts
    from OTHER accounts (simhash distance ≤ 5).
    High cross-account duplication = coordinated content.
    Uses a limited scan: last 200 posts from this account, LIMIT 2000 for comparison.
    """
    from utils.hashing import hamming_distance

    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, simhash FROM posts
        WHERE account_id=? AND simhash != 0
        ORDER BY timestamp DESC
        LIMIT 200
    """, (account_id,))

    own_posts = []
    while True:
        batch = cursor.fetchmany(200)
        if not batch:
            break
        own_posts.extend([(row[0], row[1]) for row in batch])

    if not own_posts:
        return 0.0

    # For each own post, count how many OTHER accounts have a near-dup
    dup_count = 0
    for post_id, sh in own_posts:
        # Check a recent window of foreign posts
        cursor.execute("""
            SELECT simhash FROM posts
            WHERE account_id != ? AND simhash != 0
            ORDER BY timestamp DESC
            LIMIT 2000
        """, (account_id,))

        found_dup = False
        while not found_dup:
            batch = cursor.fetchmany(500)
            if not batch:
                break
            for row in batch:
                if hamming_distance(sh, row[0]) <= 5:
                    found_dup = True
                    break

        if found_dup:
            dup_count += 1

    dup_rate = dup_count / len(own_posts)

    if dup_rate > 0.8: return 1.0
    if dup_rate > 0.5: return 0.8
    if dup_rate > 0.3: return 0.6
    if dup_rate > 0.1: return 0.4
    return 0.0


# ── Bulk scoring ──────────────────────────────────────────────────────────────

def score_all_accounts(progress_cb=None) -> int:
    """
    Recompute bot_score for every account in the DB.
    progress_cb(current, total): optional progress callback.
    Returns count of flagged accounts.
    """
    with db.get_conn() as conn:
        rows = conn.execute("SELECT id FROM accounts").fetchall()
        total = len(rows)
        flagged = 0

        for i, row in enumerate(rows):
            account_id = row['id']
            score = compute_bot_score(account_id, conn)
            if score >= config.get('bot_score_threshold', 0.7):
                flagged += 1
            if progress_cb:
                progress_cb(i + 1, total)

    return flagged
