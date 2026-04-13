"""
Campaign detection engine.
Primary algorithm: SimHash clustering within a time window.
Trigger: ≥ N unique accounts post near-identical content within window.
Stores results in campaigns + clusters tables.
"""
import math
import logging
from datetime import datetime, timezone, timedelta

import config
import db
from utils.hashing import hamming_distance

logger = logging.getLogger('threadhunt')


def run(progress_cb=None) -> int:
    """
    Full campaign detection pass. Returns number of new campaigns detected.
    Called by `threadhunt analyze`.
    """
    time_window         = config.get('campaign_time_window_minutes', 30)
    # 4chan uses a longer window — cross-thread coordination can span hours
    fourchan_time_window = config.get('fourchan_campaign_time_window_minutes', 1440)
    min_accounts        = config.get('campaign_min_accounts', 3)
    max_dist            = config.get('campaign_simhash_distance', 5)

    new_campaigns = 0

    # Run per-platform to keep clusters coherent
    with db.get_conn() as conn:
        platforms = [
            row[0] for row in
            conn.execute("SELECT DISTINCT platform FROM posts").fetchall()
        ]

    for i, platform in enumerate(platforms):
        if progress_cb:
            progress_cb(i + 1, len(platforms))
        if platform.startswith('4chan/'):
            new_campaigns += _detect_fourchan(
                platform, fourchan_time_window, min_accounts, max_dist
            )
        else:
            new_campaigns += _detect_platform(
                platform, time_window, min_accounts, max_dist
            )

    # Second pass: cross-platform cluster matching
    with db.get_conn() as conn:
        cross = _detect_cross_platform(conn, max_dist + 5)
    new_campaigns += cross

    logger.info("Campaign engine: %d new campaigns detected", new_campaigns)
    return new_campaigns


def _detect_platform(platform: str, time_window: int, min_accounts: int,
                     max_dist: int) -> int:
    """Run detection for a single platform. Returns new campaign count."""
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=time_window)
    ).isoformat()

    # Stream recent non-trivial posts into memory (capped at 10,000)
    posts = []
    with db.get_conn() as conn:
        for row in db.stream_rows(conn, """
            SELECT p.id, p.account_id, p.simhash, p.timestamp, p.content
            FROM posts p
            WHERE p.platform = ?
              AND p.timestamp >= ?
              AND p.simhash != 0
            ORDER BY p.timestamp DESC
            LIMIT 10000
        """, (platform, cutoff)):
            posts.append(dict(row))

    if len(posts) < min_accounts:
        return 0

    # Greedy single-pass clustering
    # cluster_representatives: list of (simhash_int, cluster_index)
    cluster_reps  = []
    cluster_posts = []   # list of lists of post dicts

    for post in posts:
        sh = post['simhash']
        matched = None
        for idx, rep in enumerate(cluster_reps):
            if hamming_distance(sh, rep) <= max_dist:
                matched = idx
                break
        if matched is None:
            cluster_reps.append(sh)
            cluster_posts.append([post])
        else:
            cluster_posts[matched].append(post)

    new_campaigns = 0

    for idx, cluster in enumerate(cluster_posts):
        unique_accounts = {p['account_id'] for p in cluster}
        if len(unique_accounts) < min_accounts:
            continue

        # Check if this cluster already has an active campaign
        rep_sh = cluster_reps[idx]
        if _cluster_already_tracked(rep_sh, platform):
            # Update existing campaign
            _update_campaign(rep_sh, platform, cluster, unique_accounts)
            continue

        # New campaign
        confidence = _confidence_score(cluster, unique_accounts, max_dist)
        _save_campaign(rep_sh, platform, cluster, unique_accounts, confidence)
        new_campaigns += 1

    return new_campaigns


def _detect_fourchan(platform: str, time_window: int, min_threads: int,
                     max_dist: int) -> int:
    """
    4chan-specific campaign detection.

    On anonymous boards there are no real accounts — instead, coordination
    shows as near-identical content copy-pasted (or lightly rephrased) across
    multiple distinct threads. We use thread_no as the "account" axis:
    a cluster spanning 3+ different threads is a coordinated campaign.

    Requires thread_no to be populated on post rows (fourchan collector
    backfills this on re-collection for any NULL rows).
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=time_window)
    ).isoformat()

    posts = []
    with db.get_conn() as conn:
        for row in db.stream_rows(conn, """
            SELECT p.id, p.thread_no, p.simhash, p.timestamp, p.content
            FROM posts p
            WHERE p.platform = ?
              AND p.timestamp >= ?
              AND p.simhash != 0
              AND p.thread_no IS NOT NULL
            ORDER BY p.timestamp DESC
            LIMIT 10000
        """, (platform, cutoff)):
            posts.append(dict(row))

    if len(posts) < min_threads:
        return 0

    # Greedy single-pass clustering — same algorithm as standard detection
    cluster_reps  = []
    cluster_posts = []

    for post in posts:
        sh = post['simhash']
        matched = None
        for idx, rep in enumerate(cluster_reps):
            if hamming_distance(sh, rep) <= max_dist:
                matched = idx
                break
        if matched is None:
            cluster_reps.append(sh)
            cluster_posts.append([post])
        else:
            cluster_posts[matched].append(post)

    new_campaigns = 0

    for idx, cluster in enumerate(cluster_posts):
        # "Account" axis for 4chan = distinct thread_nos
        unique_threads = {p['thread_no'] for p in cluster if p['thread_no']}
        if len(unique_threads) < min_threads:
            continue

        rep_sh = cluster_reps[idx]
        if _cluster_already_tracked(rep_sh, platform):
            _update_campaign(rep_sh, platform, cluster, unique_threads)
            continue

        confidence = _confidence_score(cluster, unique_threads, max_dist)
        _save_campaign(rep_sh, platform, cluster, unique_threads, confidence)
        new_campaigns += 1

    return new_campaigns


def _detect_cross_platform(conn, max_dist: int) -> int:
    """
    Second-pass detection: find campaigns on different platforms whose simhash
    centroids are within max_dist of each other and whose first_seen timestamps
    are within 24 hours.

    Real influence operations push narratives across platforms in sequence:
    Telegram → Twitter/Nitter → 4chan radicalization pipeline.

    Matching campaigns are linked via a new 'cross_platform_campaign' entry
    in the campaigns table (platform = 'multi') so they surface in reports.
    Returns count of new cross-platform campaign records created.
    """
    # Load all active campaigns with their representative simhash
    campaign_rows = []
    for row in db.stream_rows(conn, """
        SELECT c.id, c.platform, c.first_seen, c.keyword, c.confidence_score,
               cl.cluster_key
        FROM campaigns c
        JOIN clusters cl ON cl.campaign_id = c.id
        WHERE c.active = 1
        GROUP BY c.id
        ORDER BY c.first_seen DESC
        LIMIT 200
    """):
        try:
            key_int = int(row['cluster_key'])
        except (TypeError, ValueError):
            continue
        campaign_rows.append({
            'id':           row['id'],
            'platform':     row['platform'],
            'first_seen':   row['first_seen'],
            'keyword':      row['keyword'],
            'confidence':   row['confidence_score'],
            'key':          key_int,
        })

    if len(campaign_rows) < 2:
        return 0

    new_cross = 0
    reported = set()

    for i, ca in enumerate(campaign_rows):
        for cb in campaign_rows[i + 1:]:
            if ca['platform'] == cb['platform']:
                continue

            dist = hamming_distance(ca['key'], cb['key'])
            if dist > max_dist:
                continue

            # Time constraint: both first_seen within 24h of each other
            ts_a = _parse_ts_local(ca['first_seen'])
            ts_b = _parse_ts_local(cb['first_seen'])
            if ts_a and ts_b:
                gap_hours = abs((ts_a - ts_b).total_seconds()) / 3600
                if gap_hours > 24:
                    continue

            # Canonical dedup key — sort campaign ids
            pair_key = (min(ca['id'], cb['id']), max(ca['id'], cb['id']))
            if pair_key in reported:
                continue
            reported.add(pair_key)

            # Check if this cross-platform link already recorded
            already = conn.execute("""
                SELECT id FROM campaigns
                WHERE platform='multi'
                  AND keyword=?
                  AND first_seen >= ?
            """, (
                ca['keyword'] or cb['keyword'] or '',
                (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            )).fetchone()
            if already:
                continue

            # Determine origin (whichever platform saw it first)
            if ts_a and ts_b:
                origin, amplifier = (ca, cb) if ts_a <= ts_b else (cb, ca)
                delta_mins = int(abs((ts_a - ts_b).total_seconds()) / 60)
            else:
                origin, amplifier = ca, cb
                delta_mins = 0

            keyword  = origin['keyword'] or amplifier['keyword'] or ''
            now      = datetime.now(timezone.utc).isoformat()
            avg_conf = (origin['confidence'] + amplifier['confidence']) / 2

            cursor = conn.execute("""
                INSERT INTO campaigns
                    (keyword, platform, first_seen, last_seen,
                     post_count, account_count, confidence_score, active)
                VALUES (?, 'multi', ?, ?, 0, 0, ?, 1)
            """, (keyword, origin['first_seen'] or now, now,
                  round(min(1.0, avg_conf * 1.2), 3)))  # slight confidence boost
            campaign_id = cursor.lastrowid

            # Link source campaigns via clusters (post_id reused as campaign_id ref)
            for src in (ca, cb):
                conn.execute("""
                    INSERT OR IGNORE INTO clusters (campaign_id, post_id, cluster_key)
                    SELECT ?, post_id, cluster_key FROM clusters
                    WHERE campaign_id=? LIMIT 10
                """, (campaign_id, src['id']))

            logger.info(
                "Cross-platform campaign #%d: %s→%s kw='%s' dist=%d delta=%dm",
                campaign_id, origin['platform'], amplifier['platform'],
                keyword, dist, delta_mins
            )
            new_cross += 1

    return new_cross


def _parse_ts_local(ts: str):
    """Parse ISO timestamp string to datetime (used internally)."""
    if not ts:
        return None
    ts = ts.rstrip('Z').split('+')[0]
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def _cluster_already_tracked(rep_sh: int, platform: str) -> bool:
    """Check if a campaign with this cluster key already exists."""
    key = str(rep_sh)
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM clusters WHERE cluster_key=? LIMIT 1", (key,)
        ).fetchone()
        return row is not None


def _update_campaign(rep_sh: int, platform: str, cluster: list,
                     unique_accounts: set):
    """Update post/account counts on an existing campaign."""
    key = str(rep_sh)
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT campaign_id FROM clusters WHERE cluster_key=? LIMIT 1",
            (key,)
        ).fetchone()
        if not row:
            return
        campaign_id = row[0]

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            UPDATE campaigns
            SET last_seen=?, post_count=?, account_count=?, active=1
            WHERE id=?
        """, (now, len(cluster), len(unique_accounts), campaign_id))


def _save_campaign(rep_sh: int, platform: str, cluster: list,
                   unique_accounts: set, confidence: float):
    """Insert new campaign + cluster membership rows."""
    key = str(rep_sh)
    now = datetime.now(timezone.utc).isoformat()

    # Infer keyword from most common tokens across cluster posts
    keyword = _infer_keyword(cluster)

    timestamps = [p.get('timestamp', now) for p in cluster]
    timestamps.sort()
    first_seen = timestamps[0] if timestamps else now
    last_seen  = timestamps[-1] if timestamps else now

    with db.get_conn() as conn:
        cursor = conn.execute("""
            INSERT INTO campaigns
                (keyword, platform, first_seen, last_seen,
                 post_count, account_count, confidence_score, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (keyword, platform, first_seen, last_seen,
              len(cluster), len(unique_accounts), round(confidence, 3)))
        campaign_id = cursor.lastrowid

        for post in cluster:
            conn.execute("""
                INSERT OR IGNORE INTO clusters (campaign_id, post_id, cluster_key)
                VALUES (?, ?, ?)
            """, (campaign_id, post['id'], key))

    logger.info("New campaign #%d [%s] keyword='%s' accounts=%d posts=%d conf=%.2f",
                campaign_id, platform, keyword,
                len(unique_accounts), len(cluster), confidence)


def _confidence_score(cluster: list, unique_accounts: set, max_dist: int) -> float:
    """
    Heuristic confidence: more accounts + tighter clustering = higher confidence.
    Range: 0.0–1.0.
    """
    from utils.hashing import hamming_distance as hd

    n_accounts = len(unique_accounts)
    n_posts    = len(cluster)

    # Account factor: logarithmic scaling, max contribution at 10+ accounts
    acct_factor = min(1.0, math.log(n_accounts + 1, 10) / math.log(11, 10))

    # Volume factor
    vol_factor = min(1.0, n_posts / 20)

    # Tightness: average pairwise hamming within cluster
    if len(cluster) > 1:
        shs = [p['simhash'] for p in cluster[:50]]  # cap at 50 for speed
        pairs = [(shs[i], shs[j])
                 for i in range(len(shs))
                 for j in range(i + 1, len(shs))]
        avg_dist = sum(hd(a, b) for a, b in pairs) / len(pairs) if pairs else max_dist
        tight_factor = max(0.0, 1.0 - avg_dist / max_dist)
    else:
        tight_factor = 0.5

    return (acct_factor * 0.45 + vol_factor * 0.25 + tight_factor * 0.30)


def _infer_keyword(cluster: list) -> str:
    """Extract the most frequent non-trivial token across cluster posts."""
    import re
    STOPWORDS = {
        'the','a','an','and','or','but','in','on','at','to','for',
        'of','with','by','from','is','was','are','were','be','been',
        'this','that','these','those','i','you','he','she','we','they',
        'it','its','have','has','had','not','rt','https','http',
    }
    freq: dict = {}
    for post in cluster:
        tokens = re.findall(r'\b[a-z]{4,}\b', (post.get('content') or '').lower())
        for t in tokens:
            if t not in STOPWORDS:
                freq[t] = freq.get(t, 0) + 1
    if not freq:
        return ''
    return max(freq, key=freq.get)


def get_active_campaigns(conn) -> list:
    """Return list of active campaign dicts, ordered by last_seen DESC."""
    results = []
    for row in db.stream_rows(conn, """
        SELECT * FROM campaigns WHERE active=1
        ORDER BY last_seen DESC LIMIT 100
    """):
        results.append(dict(row))
    return results


def get_campaign_posts(campaign_id: int, conn, limit: int = 50) -> list:
    """Return posts belonging to a campaign (via clusters table)."""
    results = []
    for row in db.stream_rows(conn, """
        SELECT p.id, p.content, p.timestamp, p.platform,
               a.username, a.bot_score
        FROM clusters c
        JOIN posts p    ON p.id = c.post_id
        JOIN accounts a ON a.id = p.account_id
        WHERE c.campaign_id = ?
        ORDER BY p.timestamp DESC
        LIMIT ?
    """, (campaign_id, limit)):
        results.append(dict(row))
    return results
