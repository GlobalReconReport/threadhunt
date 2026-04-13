"""
Narrative clustering — semantic coordination detection.

SEPARATE from simhash campaign detection (campaign_engine.py), which catches
near-duplicate/copy-pasted text (bot amplification).  This module catches
*thematic coordination*: multiple accounts or platforms pushing the same
narrative keywords even when the exact wording differs — the pattern of a
coordinated influence operation narrative-seeding cycle.

Algorithm
---------
1. Extract significant keywords per post (stopword-filtered, ≥4 chars,
   handles Latin + Cyrillic).
2. Greedy keyword-overlap clustering within a configurable time window.
   Cluster centroid (keyword union) expands as posts are added.
3. Clusters spanning ≥3 distinct accounts (or ≥2 distinct platforms)
   are flagged as coordination signals.
4. Clusters whose posts are near-duplicates (median simhash dist ≤ 5)
   are skipped — those belong to the simhash engine.
5. Results stored in the existing campaigns + clusters tables.
   cluster_key prefix: 'narrative_'   (per-platform)
                       'narrative_xp_' (cross-platform)
   This lets alerts and queries distinguish them from simhash clusters.

Config keys
-----------
narrative_time_window_hours   : int  (default 6)
narrative_min_keyword_overlap : int  (default 3)
narrative_min_sources         : int  (default 3 — accounts within one platform)
narrative_min_platforms       : int  (default 2 — for cross-platform detection)
"""
import re
import math
import logging
from collections import Counter
from datetime import datetime, timezone, timedelta

import config
import db

logger = logging.getLogger('threadhunt')

# ── Stopwords ─────────────────────────────────────────────────────────────────
# English + Russian (Cyrillic) common words that carry no narrative signal.
_STOPWORDS = frozenset({
    # English
    'the', 'and', 'that', 'this', 'with', 'have', 'from', 'they', 'will',
    'been', 'were', 'their', 'there', 'when', 'what', 'which', 'about',
    'into', 'your', 'some', 'said', 'also', 'than', 'then', 'such', 'more',
    'very', 'just', 'like', 'over', 'after', 'before', 'where', 'here',
    'even', 'most', 'only', 'much', 'does', 'each', 'many', 'well', 'back',
    'both', 'away', 'being', 'https', 'http', 'www', 'com', 'news', 'post',
    'read', 'source', 'link', 'today', 'time', 'year', 'week', 'days',
    'said', 'says', 'know', 'want', 'people', 'would', 'could', 'should',
    'still', 'first', 'last', 'used', 'make', 'made', 'take', 'another',
    # English discourse / filler words (cross-platform noise)
    'actually', 'really', 'comes', 'come', 'live', 'speak', 'believe',
    'while', 'other', 'think', 'need', 'look', 'good', 'work', 'next',
    'thing', 'going', 'right', 'these', 'those', 'them', 'they', 'very',
    'getting', 'doing', 'talking', 'saying', 'telling', 'something',
    'nothing', 'everything', 'anything', 'anyone', 'someone', 'everyone',
    'never', 'always', 'again', 'maybe', 'really', 'pretty', 'quite',
    'around', 'during', 'while', 'until', 'since', 'though', 'although',
    'however', 'because', 'against', 'without', 'within', 'between',
    # Generic situational / filler context (no narrative specificity)
    'long', 'local', 'situation', 'happened', 'happen', 'happens',
    'going', 'comes', 'come', 'give', 'gave', 'gets', 'getting',
    'place', 'point', 'part', 'case', 'fact', 'side', 'kind',
    'number', 'level', 'area', 'high', 'huge', 'real', 'small',
    # YouTube / media metadata (bleed from titles and descriptions)
    'episode', 'series', 'video', 'youtube', 'watch', 'channel',
    'subscribe', 'playlist', 'stream', 'podcast', 'interview', 'clip',
    'part', 'show', 'full', 'live', 'weekly', 'daily', 'official',
    # Russian (Cyrillic — matched when posts contain Russian text)
    # Grammar / function words
    'что', 'это', 'для', 'как', 'все', 'они', 'или', 'его', 'был', 'при',
    'так', 'уже', 'если', 'даже', 'может', 'быть', 'тому', 'там', 'тоже',
    'того', 'этих', 'себя', 'нет', 'этот', 'меня', 'хотя', 'свою', 'лишь',
    'через', 'когда', 'него', 'после', 'году', 'была', 'чего', 'кого',
    'также', 'более', 'этого', 'него', 'теперь', 'тогда', 'стало', 'самих',
    'наши', 'наша', 'наше', 'наших', 'нашей', 'нашим', 'нашего',
    'которые', 'которая', 'которых', 'которого',
    # Time / date (high-frequency, zero narrative signal)
    'апреля', 'марта', 'января', 'февраля', 'мая', 'июня', 'июля',
    'августа', 'сентября', 'октября', 'ноября', 'декабря',
    'года', 'году', 'годов', 'время', 'сегодня', 'вчера', 'завтра',
    'часов', 'минут', 'утром', 'вечером', 'ночью', 'днём', 'дней',
    # Generic Russian news verbs / nouns (ubiquitous across all channels)
    'будет', 'будут', 'только', 'который', 'именно', 'можно', 'нужно',
    'стало', 'стали', 'заявил', 'заявила', 'сообщил', 'сообщили',
    'сообщает', 'сообщается', 'рассказал', 'отметил', 'добавил',
    'страны', 'страна', 'стране', 'власти', 'властей', 'власть',
    'первый', 'первая', 'первое', 'несколько', 'один', 'одна', 'одно',
    'человек', 'людей', 'люди', 'млрд', 'тысяч', 'тысячи', 'тысяча',
    'процентов', 'процента', 'число', 'около', 'более', 'менее',
    # Channel self-references (account names that bleed into post text)
    'readovka', 'rybar', 'telegram', 'подписаться', 'канале', 'канал',
})


# ── Public API ────────────────────────────────────────────────────────────────

def extract_keywords(text: str, top_n: int = 20) -> list:
    """
    Extract significant keywords from text.
    Handles Latin (a-z) and Cyrillic (Russian) scripts.
    Returns top_n most-frequent tokens after stopword filtering.
    """
    if not text:
        return []
    # Matches both Latin and Cyrillic word characters, min 4 chars
    tokens = re.findall(r'[a-zA-Z\u0400-\u04FF]{4,}', text.lower())
    filtered = [t for t in tokens if t not in _STOPWORDS]
    if not filtered:
        return []
    freq = Counter(filtered)
    return [w for w, _ in freq.most_common(top_n)]


def run(progress_cb=None) -> int:
    """
    Full narrative clustering pass across all platforms.
    Returns count of new narrative clusters saved as campaigns.
    Called by cmd_analyze after campaign_engine.run().
    """
    time_window_hours = config.get('narrative_time_window_hours', 6)
    min_overlap       = config.get('narrative_min_keyword_overlap', 3)
    min_sources       = config.get('narrative_min_sources', 3)
    min_platforms     = config.get('narrative_min_platforms', 2)

    with db.get_conn() as conn:
        platforms = [
            row[0] for row in
            conn.execute("SELECT DISTINCT platform FROM posts WHERE simhash != 0").fetchall()
        ]

    total_new = 0
    n = len(platforms)

    for i, platform in enumerate(platforms):
        if progress_cb:
            progress_cb(i + 1, n + 1)
        with db.get_conn() as conn:
            total_new += _detect_per_platform(
                conn, platform, time_window_hours, min_overlap, min_sources
            )

    # Cross-platform pass (separate step)
    if progress_cb:
        progress_cb(n + 1, n + 1)
    with db.get_conn() as conn:
        total_new += _detect_cross_platform(
            conn, time_window_hours, min_overlap, min_platforms
        )

    logger.info("Narrative clustering: %d new clusters detected", total_new)
    return total_new


# ── Per-platform detection ────────────────────────────────────────────────────

def _detect_per_platform(conn, platform: str, window_hours: int,
                          min_overlap: int, min_sources: int) -> int:
    """
    Within-platform narrative cluster detection.
    Finds groups of posts from ≥min_sources distinct accounts that share
    ≥min_overlap significant keywords in the time window.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()

    posts = []
    for row in db.stream_rows(conn, """
        SELECT p.id, p.account_id, p.simhash, p.content, p.timestamp
        FROM posts p
        WHERE p.platform = ?
          AND p.timestamp >= ?
          AND p.simhash != 0
        ORDER BY p.timestamp ASC
        LIMIT 5000
    """, (platform, cutoff)):
        kws = extract_keywords(row['content'] or '')
        if len(kws) < 3:
            continue
        posts.append({
            'id':         row['id'],
            'account_id': row['account_id'],
            'simhash':    row['simhash'],
            'timestamp':  row['timestamp'],
            'keywords':   set(kws[:20]),
        })

    if len(posts) < min_sources:
        return 0

    clusters = _greedy_cluster(posts, min_overlap)
    new_campaigns = 0

    for cluster, ckws in clusters:
        unique_accounts = {p['account_id'] for p in cluster}
        if len(unique_accounts) < min_sources:
            continue

        # Skip near-duplicates — simhash engine handles those
        if _is_simhash_cluster(cluster):
            logger.debug("Narrative: skipping near-duplicate cluster on %s (%d posts)",
                         platform, len(cluster))
            continue

        top_keywords = _top_keywords(cluster, n=5)
        cluster_key  = 'narrative_' + ','.join(sorted(top_keywords))

        if _narrative_cluster_exists(conn, cluster_key):
            continue

        confidence = _narrative_confidence(
            cluster, unique_accounts, set(), top_keywords, min_overlap, window_hours
        )
        _save_narrative_campaign(
            conn, cluster_key, platform, cluster,
            unique_accounts, confidence, top_keywords
        )
        new_campaigns += 1

    return new_campaigns


# ── Cross-platform detection ──────────────────────────────────────────────────

def _detect_cross_platform(conn, window_hours: int, min_overlap: int,
                             min_platforms: int) -> int:
    """
    Cross-platform narrative detection.

    Finds the same keyword narrative appearing on ≥min_platforms distinct
    platforms within the time window.  This is separate from simhash
    cross-platform detection (which requires near-identical text).

    Strategy:
    - Load recent posts per platform, extract keyword sets
    - Cluster within each platform into narrative "blobs"
    - Compare blobs across platforms: if keyword intersection ≥ min_overlap,
      create a 'narrative_xp_' cross-platform cluster
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()

    # Load all recent posts with keyword extraction
    platform_posts: dict = {}
    for row in db.stream_rows(conn, """
        SELECT p.id, p.account_id, p.platform, p.simhash,
               p.content, p.timestamp
        FROM posts p
        WHERE p.timestamp >= ?
          AND p.simhash != 0
        ORDER BY p.timestamp ASC
        LIMIT 10000
    """, (cutoff,)):
        kws = extract_keywords(row['content'] or '')
        if len(kws) < 3:
            continue
        plat = row['platform']
        if plat not in platform_posts:
            platform_posts[plat] = []
        platform_posts[plat].append({
            'id':         row['id'],
            'account_id': row['account_id'],
            'platform':   plat,
            'simhash':    row['simhash'],
            'timestamp':  row['timestamp'],
            'keywords':   set(kws[:20]),
        })

    if len(platform_posts) < min_platforms:
        return 0

    # Cluster within each platform to get per-platform narrative blobs.
    # We include even single-post blobs so they can contribute to cross-platform
    # matches (one post on Telegram + one post on 4chan about the same topic
    # is already meaningful when it's not near-duplicate).
    platform_blobs: dict = {}
    for plat, posts in platform_posts.items():
        blobs = _greedy_cluster(posts, min_overlap)
        # Keep all blobs with ≥1 post (singletons welcome for cross-platform)
        platform_blobs[plat] = [(ckws, cluster) for cluster, ckws in blobs]

    all_platforms = list(platform_blobs.keys())
    if len(all_platforms) < min_platforms:
        return 0

    new_campaigns = 0
    reported_keys: set = set()

    for i, plat_a in enumerate(all_platforms):
        for plat_b in all_platforms[i + 1:]:
            blobs_a = platform_blobs[plat_a]
            blobs_b = platform_blobs[plat_b]

            for kws_a, posts_a in blobs_a:
                for kws_b, posts_b in blobs_b:
                    overlap_kws = kws_a & kws_b
                    if len(overlap_kws) < min_overlap:
                        continue

                    all_posts = posts_a + posts_b
                    if _is_simhash_cluster(all_posts):
                        continue

                    # Use the top-overlap keywords as the canonical key
                    top_keywords = _top_shared_keywords(posts_a, posts_b, n=5)
                    if len(top_keywords) < min_overlap:
                        top_keywords = sorted(overlap_kws)[:5]

                    cluster_key = 'narrative_xp_' + ','.join(sorted(top_keywords))

                    if cluster_key in reported_keys:
                        continue
                    if _narrative_cluster_exists(conn, cluster_key):
                        reported_keys.add(cluster_key)
                        continue

                    reported_keys.add(cluster_key)
                    unique_accounts  = {p['account_id'] for p in all_posts}
                    unique_platforms = {plat_a, plat_b}

                    confidence = _narrative_confidence(
                        all_posts, unique_accounts, unique_platforms,
                        top_keywords, min_overlap, window_hours
                    )
                    _save_narrative_campaign(
                        conn, cluster_key, 'narrative_multi', all_posts,
                        unique_accounts, confidence, top_keywords
                    )
                    new_campaigns += 1
                    logger.info(
                        "Narrative XP: %s ↔ %s  keywords=%s  accounts=%d  conf=%.2f",
                        plat_a, plat_b, top_keywords[:3], len(unique_accounts), confidence
                    )

    return new_campaigns


# ── Clustering helpers ────────────────────────────────────────────────────────

def _greedy_cluster(posts: list, min_overlap: int) -> list:
    """
    Single-pass greedy clustering by keyword overlap.
    Each post is assigned to the best-matching existing cluster (highest overlap
    ≥ min_overlap) or starts a new one.  The cluster's keyword union grows as
    posts are added, allowing chained thematic drift within a cluster.

    Returns list of (cluster_posts_list, cluster_kw_union_set) tuples.
    """
    cluster_kws   = []   # growing keyword unions
    cluster_posts = []

    for post in posts:
        post_kws = post['keywords']
        best_idx     = None
        best_overlap = 0

        for idx, ckws in enumerate(cluster_kws):
            ov = len(post_kws & ckws)
            if ov >= min_overlap and ov > best_overlap:
                best_overlap = ov
                best_idx     = idx

        if best_idx is None:
            cluster_kws.append(set(post_kws))
            cluster_posts.append([post])
        else:
            cluster_kws[best_idx] |= post_kws
            cluster_posts[best_idx].append(post)

    return list(zip(cluster_posts, cluster_kws))


def _is_simhash_cluster(posts: list, threshold: int = 5) -> bool:
    """
    Return True if median pairwise simhash distance ≤ threshold.
    When True, the cluster is near-duplicate text — let campaign_engine handle it.
    Samples up to 20 posts, evaluates up to 50 pairs for speed.
    """
    from utils.hashing import hamming_distance
    shs = [p['simhash'] for p in posts[:20] if p.get('simhash')]
    if len(shs) < 2:
        return False
    pairs = [
        (shs[i], shs[j])
        for i in range(len(shs))
        for j in range(i + 1, len(shs))
    ][:50]
    if not pairs:
        return False
    distances = sorted(hamming_distance(a, b) for a, b in pairs)
    median = distances[len(distances) // 2]
    return median <= threshold


def _top_keywords(cluster: list, n: int = 5) -> list:
    """Most frequent keywords across all posts in cluster."""
    freq: Counter = Counter()
    for post in cluster:
        freq.update(post['keywords'])
    return [w for w, _ in freq.most_common(n)]


def _top_shared_keywords(posts_a: list, posts_b: list, n: int = 5) -> list:
    """
    Keywords that appear in BOTH groups, ranked by combined frequency.
    Used for cross-platform cluster keys to ensure the key reflects
    keywords that actually bridge the two platforms.
    """
    freq_a: Counter = Counter()
    freq_b: Counter = Counter()
    for p in posts_a:
        freq_a.update(p['keywords'])
    for p in posts_b:
        freq_b.update(p['keywords'])
    shared = set(freq_a.keys()) & set(freq_b.keys())
    combined = {kw: freq_a[kw] + freq_b[kw] for kw in shared}
    return [kw for kw, _ in Counter(combined).most_common(n)]


# ── Confidence scoring ────────────────────────────────────────────────────────

def _narrative_confidence(cluster: list, unique_accounts: set,
                           unique_platforms: set, top_keywords: list,
                           min_overlap: int, window_hours: int) -> float:
    """
    Narrative coordination confidence score [0.0–1.0].

    Components:
    - overlap_factor (30%): keyword overlap count above minimum
    - source_factor  (35%): distinct accounts (logarithmic)
    - platform_factor(25%): multi-platform bonus (max at 3 platforms)
    - time_factor    (10%): temporal compression (tighter = higher)
    """
    # Keyword overlap quality
    overlap_count  = len(top_keywords)
    overlap_factor = min(1.0, (overlap_count - min_overlap + 1) / 5.0)

    # Source count (accounts or platforms as the larger unit)
    n_sources    = max(len(unique_accounts), len(unique_platforms))
    source_factor = min(1.0, math.log(n_sources + 1, 2) / math.log(8, 2))

    # Platform diversity
    n_plats = len(unique_platforms)
    platform_factor = min(1.0, n_plats / 3.0) if n_plats else 0.0

    # Time compression within cluster
    timestamps = [_parse_ts(p['timestamp']) for p in cluster]
    timestamps = [t for t in timestamps if t]
    if len(timestamps) >= 2:
        timestamps.sort()
        spread_hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
        time_factor  = max(0.0, 1.0 - spread_hours / max(window_hours, 1))
    else:
        time_factor = 0.5

    score = (
        overlap_factor  * 0.30 +
        source_factor   * 0.35 +
        platform_factor * 0.25 +
        time_factor     * 0.10
    )
    return round(min(1.0, score), 3)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _narrative_cluster_exists(conn, cluster_key: str) -> bool:
    """True if a cluster row with this key already exists."""
    return conn.execute(
        "SELECT 1 FROM clusters WHERE cluster_key=? LIMIT 1", (cluster_key,)
    ).fetchone() is not None


def _save_narrative_campaign(conn, cluster_key: str, platform: str,
                               cluster: list, unique_accounts: set,
                               confidence: float, top_keywords: list):
    """Insert a narrative campaign + cluster membership rows."""
    now     = datetime.now(timezone.utc).isoformat()
    keyword = top_keywords[0] if top_keywords else ''

    timestamps = sorted(p['timestamp'] for p in cluster if p.get('timestamp'))
    first_seen = timestamps[0]  if timestamps else now
    last_seen  = timestamps[-1] if timestamps else now

    cursor = conn.execute("""
        INSERT INTO campaigns
            (keyword, platform, first_seen, last_seen,
             post_count, account_count, confidence_score, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
    """, (keyword, platform, first_seen, last_seen,
          len(cluster), len(unique_accounts), confidence))
    campaign_id = cursor.lastrowid

    for post in cluster:
        conn.execute("""
            INSERT OR IGNORE INTO clusters (campaign_id, post_id, cluster_key)
            VALUES (?, ?, ?)
        """, (campaign_id, post['id'], cluster_key))

    logger.info(
        "Narrative campaign #%d [%s] keywords=%s accounts=%d posts=%d conf=%.2f",
        campaign_id, platform, top_keywords[:3],
        len(unique_accounts), len(cluster), confidence
    )


# ── Timestamp helper ──────────────────────────────────────────────────────────

def _parse_ts(ts: str):
    if not ts:
        return None
    ts = ts.rstrip('Z').split('+')[0]
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None
