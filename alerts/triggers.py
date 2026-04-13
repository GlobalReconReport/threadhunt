"""
Alert trigger system.
Four trigger types:
  1. Coordinated campaign detected (campaign engine)
  2. Bot score above threshold
  3. Narrative re-emergence (keyword silent 48h then reappears)
  4. Keyword spike (sudden volume increase)

All triggers write to the alerts table.
"""
import logging
from datetime import datetime, timezone, timedelta

import config
import db

logger = logging.getLogger('threadhunt')


# ── Dispatch ──────────────────────────────────────────────────────────────────

def check_all(conn) -> int:
    """
    Run every trigger check in sequence.
    Returns total count of new alerts created.
    """
    total = 0
    total += check_new_campaigns(conn)
    total += check_cross_platform_amplification(conn)
    total += check_narrative_alignment(conn)
    total += check_high_bot_scores(conn)
    total += check_scheduled_accounts(conn)
    total += check_narrative_reemergence(conn)
    total += check_keyword_spikes(conn)
    return total


# ── Trigger 1: Coordinated campaign ─────────────────────────────────────────

def check_new_campaigns(conn) -> int:
    """
    Alert on campaigns that have no corresponding alert yet.
    Severity based on confidence_score.
    """
    new_alerts = 0

    for row in db.stream_rows(conn, """
        SELECT c.id, c.keyword, c.platform, c.account_count,
               c.post_count, c.confidence_score, c.first_seen, c.last_seen
        FROM campaigns c
        WHERE c.active = 1
          AND NOT EXISTS (
              SELECT 1 FROM alerts a
              WHERE a.alert_type = 'coordinated_campaign'
                AND a.keyword = c.keyword
                AND a.platform = c.platform
          )
          AND NOT EXISTS (
              SELECT 1 FROM clusters cl
              WHERE cl.campaign_id = c.id
                AND cl.cluster_key LIKE 'narrative_%'
          )
        ORDER BY c.confidence_score DESC
        LIMIT 50
    """):
        severity = _campaign_severity(row['confidence_score'])

        # Collect account IDs in this campaign
        account_ids = []
        for acct_row in db.stream_rows(conn, """
            SELECT DISTINCT p.account_id
            FROM clusters cl
            JOIN posts p ON p.id = cl.post_id
            WHERE cl.campaign_id = ?
            LIMIT 50
        """, (row['id'],)):
            account_ids.append(acct_row[0])

        # Time window duration
        first = row['first_seen'] or ''
        last  = row['last_seen']  or ''
        window_str = _time_window_str(first, last)

        description = (
            f"Coordinated campaign detected\n"
            f"Keyword:     {row['keyword'] or 'N/A'}\n"
            f"Platform:    {row['platform']}\n"
            f"Accounts:    {row['account_count']}\n"
            f"Posts:       {row['post_count']}\n"
            f"Time window: {window_str}\n"
            f"Confidence:  {_confidence_label(row['confidence_score'])}"
        )

        db.create_alert(
            conn,
            alert_type='coordinated_campaign',
            severity=severity,
            description=description,
            platform=row['platform'],
            account_ids=account_ids,
            keyword=row['keyword'],
        )
        new_alerts += 1
        logger.info("Alert: coordinated_campaign [%s] %s", severity, row['keyword'])

    return new_alerts


# ── Trigger 2: Cross-platform narrative amplification ────────────────────────

def check_cross_platform_amplification(conn) -> int:
    """
    Alert on campaigns with platform='multi' that have no alert yet.
    These are created by campaign_engine._detect_cross_platform() when the
    same narrative (simhash cluster) appears on 2+ platforms within 24h.

    Severity is always HIGH — cross-platform coordination is the primary
    mission signal for foreign influence operation detection.
    """
    new_alerts = 0

    for row in db.stream_rows(conn, """
        SELECT c.id, c.keyword, c.first_seen, c.last_seen,
               c.confidence_score, c.post_count
        FROM campaigns c
        WHERE c.platform = 'multi'
          AND c.active = 1
          AND NOT EXISTS (
              SELECT 1 FROM alerts a
              WHERE a.alert_type = 'cross_platform_amplification'
                AND a.keyword = c.keyword
          )
        ORDER BY c.confidence_score DESC
        LIMIT 50
    """):
        # Resolve which platforms contributed
        platform_rows = conn.execute("""
            SELECT DISTINCT ca.platform
            FROM clusters cl
            JOIN campaigns ca ON ca.id = cl.campaign_id
            WHERE ca.id IN (
                SELECT campaign_id FROM clusters
                WHERE post_id IN (
                    SELECT post_id FROM clusters WHERE campaign_id=?
                )
            )
              AND ca.platform != 'multi'
        """, (row['id'],)).fetchall()
        platforms = [r[0] for r in platform_rows] if platform_rows else ['unknown']

        description = (
            f"Cross-platform narrative amplification\n"
            f"Platforms:  {' → '.join(platforms)}\n"
            f"Keyword:    {row['keyword'] or 'N/A'}\n"
            f"First seen: {row['first_seen']}\n"
            f"Confidence: {_confidence_label(row['confidence_score'])}\n"
            f"NOTE: Same narrative cluster detected across multiple platforms.\n"
            f"      Consistent with foreign influence operation pipeline."
        )

        db.create_alert(
            conn,
            alert_type='cross_platform_amplification',
            severity='high',
            description=description,
            platform='multi',
            keyword=row['keyword'],
        )
        new_alerts += 1
        logger.info("Alert: cross_platform_amplification kw='%s' platforms=%s",
                    row['keyword'], platforms)

    return new_alerts


# ── Trigger 2b: Narrative alignment ──────────────────────────────────────────

def check_narrative_alignment(conn) -> int:
    """
    Alert on narrative coordination campaigns created by narrative_clustering.py.
    These use cluster_key LIKE 'narrative_%' to distinguish them from simhash
    clusters.  They represent thematic coordination (same keywords across
    accounts/platforms) rather than near-duplicate text (bot amplification).

    Severity:
    - HIGH  if cross-platform (narrative_multi) OR confidence ≥ 0.7
    - MEDIUM otherwise
    """
    new_alerts = 0

    for row in db.stream_rows(conn, """
        SELECT DISTINCT c.id, c.keyword, c.platform, c.account_count,
               c.post_count, c.confidence_score, c.first_seen, c.last_seen,
               cl.cluster_key
        FROM campaigns c
        JOIN clusters cl ON cl.campaign_id = c.id
        WHERE c.active = 1
          AND cl.cluster_key LIKE 'narrative_%'
          AND NOT EXISTS (
              SELECT 1 FROM alerts a
              WHERE a.alert_type = 'narrative_alignment'
                AND a.keyword    = c.keyword
                AND a.platform   = c.platform
          )
        GROUP BY c.id
        ORDER BY c.confidence_score DESC
        LIMIT 50
    """):
        is_cross = row['platform'] in ('narrative_multi',)
        severity = 'high' if (is_cross or row['confidence_score'] >= 0.7) else 'medium'

        # Resolve contributing platforms from actual post records
        plat_rows = conn.execute("""
            SELECT DISTINCT p.platform
            FROM clusters cl2
            JOIN posts p ON p.id = cl2.post_id
            WHERE cl2.campaign_id = ?
        """, (row['id'],)).fetchall()
        platforms_list = sorted({r[0] for r in plat_rows}) if plat_rows else [row['platform']]

        # Human-readable keyword list from cluster_key
        raw_key = row['cluster_key'] or ''
        kw_display = (raw_key
                      .replace('narrative_xp_', '')
                      .replace('narrative_', '')
                      .replace(',', ', '))

        window_str = _time_window_str(row['first_seen'], row['last_seen'])

        if is_cross:
            coord_note = (
                f"Cross-platform narrative: same themes on {', '.join(platforms_list)}\n"
                f"      Consistent with coordinated influence operation pipeline."
            )
        else:
            coord_note = (
                f"Within-platform: {row['account_count']} accounts pushing same narrative.\n"
                f"      Consistent with coordinated channel messaging."
            )

        description = (
            f"Narrative alignment detected\n"
            f"Keywords:    {kw_display}\n"
            f"Platform(s): {', '.join(platforms_list)}\n"
            f"Accounts:    {row['account_count']}\n"
            f"Posts:       {row['post_count']}\n"
            f"Window:      {window_str}\n"
            f"Confidence:  {_confidence_label(row['confidence_score'])}\n"
            f"{coord_note}"
        )

        account_ids = []
        for acct_row in db.stream_rows(conn, """
            SELECT DISTINCT p.account_id
            FROM clusters cl2
            JOIN posts p ON p.id = cl2.post_id
            WHERE cl2.campaign_id = ?
            LIMIT 50
        """, (row['id'],)):
            account_ids.append(acct_row[0])

        db.create_alert(
            conn,
            alert_type='narrative_alignment',
            severity=severity,
            description=description,
            platform=row['platform'],
            account_ids=account_ids,
            keyword=row['keyword'],
        )
        new_alerts += 1
        logger.info("Alert: narrative_alignment [%s] kw='%s' platforms=%s conf=%.2f",
                    severity, row['keyword'], platforms_list, row['confidence_score'])

    return new_alerts


# ── Trigger 2c: Scheduled posting pattern ────────────────────────────────────

def check_scheduled_accounts(conn) -> int:
    """
    Alert on accounts whose temporal profile shows scheduled/automated posting.
    Criteria: posting_entropy < 2.0 with >= 20 posts = bot/scheduler indicator.
    Additional signal: UTC+3 timezone (Moscow/Minsk) on Telegram/Nitter accounts.
    """
    new_alerts = 0
    threshold_entropy = 2.0
    min_posts = 20

    for row in db.stream_rows(conn, """
        SELECT a.id, a.username, a.platform, a.posting_entropy,
               a.timezone_offset, COUNT(p.id) as post_count
        FROM accounts a
        JOIN posts p ON p.account_id = a.id
        WHERE a.posting_entropy IS NOT NULL
          AND a.posting_entropy < ?
          AND a.platform IN ('telegram', 'nitter', 'twitter', 'vk')
        GROUP BY a.id
        HAVING COUNT(p.id) >= ?
        ORDER BY a.posting_entropy ASC
        LIMIT 50
    """, (threshold_entropy, min_posts)):
        # Avoid re-alerting
        dup = conn.execute("""
            SELECT id FROM alerts
            WHERE alert_type='scheduled_posting'
              AND account_ids LIKE '%' || ? || '%'
              AND created_at >= ?
        """, (str(row['id']),
              (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat())
        ).fetchone()
        if dup:
            continue

        tz_note = ''
        if row['timezone_offset'] is not None:
            tz = row['timezone_offset']
            tz_label = {3: 'Moscow/Minsk', 2: 'Eastern Europe',
                        8: 'Beijing/Shanghai', 0: 'UTC/London'}.get(tz, f'UTC{tz:+d}')
            tz_note = f"\nTimezone:  UTC{tz:+d} ({tz_label})"
            if tz in (3, 2):
                tz_note += '  ← attribution signal'

        description = (
            f"Automated/scheduled posting pattern detected\n"
            f"Account:   @{row['username']} on {row['platform']}\n"
            f"Entropy:   {row['posting_entropy']:.2f} (max 4.58, < 2.0 = scheduled)\n"
            f"Posts:     {row['post_count']}"
            f"{tz_note}"
        )

        severity = 'high' if (row['timezone_offset'] in (3, 2) and
                              row['platform'] in ('telegram', 'nitter')) else 'medium'

        db.create_alert(
            conn,
            alert_type='scheduled_posting',
            severity=severity,
            description=description,
            platform=row['platform'],
            account_ids=[row['id']],
        )
        new_alerts += 1
        logger.info("Alert: scheduled_posting @%s (%s) entropy=%.2f tz=%s",
                    row['username'], row['platform'],
                    row['posting_entropy'], row['timezone_offset'])

    return new_alerts


# ── Trigger 4: Bot score threshold ───────────────────────────────────────────

def check_high_bot_scores(conn) -> int:
    """
    Alert on accounts that crossed the bot score threshold and have no alert yet.
    """
    threshold = config.get('bot_score_threshold', 0.7)
    new_alerts = 0

    for row in db.stream_rows(conn, """
        SELECT a.id, a.username, a.platform, a.bot_score, a.followers, a.following
        FROM accounts a
        WHERE a.flagged = 1
          AND a.bot_score >= ?
          AND NOT EXISTS (
              SELECT 1 FROM alerts al
              WHERE al.alert_type = 'bot_detected'
                AND al.account_ids LIKE '%' || a.id || '%'
          )
        ORDER BY a.bot_score DESC
        LIMIT 100
    """, (threshold,)):
        description = (
            f"High bot score detected\n"
            f"Account:  @{row['username']} on {row['platform']}\n"
            f"Score:    {row['bot_score']:.2f}\n"
            f"Followers: {row['followers']} / Following: {row['following']}"
        )
        db.create_alert(
            conn,
            alert_type='bot_detected',
            severity='high' if row['bot_score'] >= 0.85 else 'medium',
            description=description,
            platform=row['platform'],
            account_ids=[row['id']],
        )
        new_alerts += 1

    if new_alerts:
        logger.info("Alert: %d new bot_detected alerts", new_alerts)
    return new_alerts


# ── Trigger 5: Narrative re-emergence ────────────────────────────────────────

def check_narrative_reemergence(conn) -> int:
    """
    Detect watchlist keywords that were silent for 48h and have now reappeared.
    """
    silence_threshold = timedelta(hours=48)
    now = datetime.now(timezone.utc)
    new_alerts = 0

    for row in db.stream_rows(conn,
        "SELECT id, value, platform FROM watchlist WHERE type='keyword'"
    ):
        keyword = row['value']
        platform = row['platform']

        # Find the last time this keyword appeared in posts
        where_platform = "AND platform=?" if platform else ""
        params = (f'%{keyword}%',) + ((platform,) if platform else ())

        latest_row = conn.execute(f"""
            SELECT MAX(timestamp) FROM posts
            WHERE content LIKE ?
            {where_platform}
        """, params).fetchone()

        if not latest_row or not latest_row[0]:
            continue

        last_seen = _parse_ts(latest_row[0])
        if not last_seen:
            continue

        gap = now - last_seen

        # It was silent (gap > 48h) — check if it just appeared (within last 30 min)
        if gap > silence_threshold:
            continue   # Still silent

        # Check if there was a long gap before this recent activity
        before_cutoff = (last_seen - silence_threshold).isoformat()
        prev_params = (f'%{keyword}%',) + ((before_cutoff,) if True else ()) + \
                      ((platform,) if platform else ())
        prev_row = conn.execute(f"""
            SELECT COUNT(*) FROM posts
            WHERE content LIKE ?
              AND timestamp <= ?
              {where_platform}
        """, prev_params).fetchone()

        had_prior_activity = prev_row and prev_row[0] and prev_row[0] > 0
        if not had_prior_activity:
            continue   # No prior activity to compare against

        # Avoid duplicate alerts: check if we already alerted on this re-emergence
        recent_alert = conn.execute("""
            SELECT id FROM alerts
            WHERE alert_type='narrative_reemergence'
              AND keyword=?
              AND created_at >= ?
        """, (keyword, (now - timedelta(hours=6)).isoformat())).fetchone()

        if recent_alert:
            continue

        description = (
            f"Narrative re-emergence detected\n"
            f"Keyword:   {keyword}\n"
            f"Platform:  {platform or 'all'}\n"
            f"Last seen: {last_seen.isoformat()}\n"
            f"Prior silence: > 48 hours"
        )
        db.create_alert(
            conn,
            alert_type='narrative_reemergence',
            severity='medium',
            description=description,
            platform=platform,
            keyword=keyword,
        )
        new_alerts += 1
        logger.info("Alert: narrative_reemergence — keyword '%s'", keyword)

    return new_alerts


# ── Trigger 6: Keyword spike ──────────────────────────────────────────────────

def check_keyword_spikes(conn) -> int:
    """
    Detect sudden volume increase: keyword appears 3x more in last 30 min
    versus baseline (previous 6-hour average per 30-min window).
    """
    now = datetime.now(timezone.utc)
    window_30m = timedelta(minutes=30)
    baseline_window = timedelta(hours=6)

    new_alerts = 0

    for row in db.stream_rows(conn,
        "SELECT id, value, platform FROM watchlist WHERE type='keyword'"
    ):
        keyword  = row['value']
        platform = row['platform']

        recent_cutoff   = (now - window_30m).isoformat()
        baseline_cutoff = (now - baseline_window).isoformat()
        pre_window_cut  = (now - window_30m).isoformat()

        where_plat = "AND platform=?" if platform else ""
        p_recent   = (f'%{keyword}%', recent_cutoff) + ((platform,) if platform else ())
        p_baseline = (f'%{keyword}%', baseline_cutoff, pre_window_cut) + \
                     ((platform,) if platform else ())

        recent_count = conn.execute(f"""
            SELECT COUNT(*) FROM posts
            WHERE content LIKE ? AND timestamp >= ?
            {where_plat}
        """, p_recent).fetchone()[0]

        # Baseline: posts in 6h window BEFORE the recent 30 min
        baseline_count = conn.execute(f"""
            SELECT COUNT(*) FROM posts
            WHERE content LIKE ?
              AND timestamp >= ? AND timestamp < ?
            {where_plat}
        """, p_baseline).fetchone()[0]

        # Normalize baseline to per-30-min rate
        # 6h / 30min = 12 windows
        baseline_per_window = baseline_count / 12.0 if baseline_count else 0

        # Spike: recent is 3x baseline AND at least 5 posts
        if (recent_count >= 5 and
            baseline_per_window > 0 and
            recent_count >= baseline_per_window * 3):

            # No duplicate in last hour
            dup = conn.execute("""
                SELECT id FROM alerts
                WHERE alert_type='keyword_spike' AND keyword=?
                  AND created_at >= ?
            """, (keyword, (now - timedelta(hours=1)).isoformat())).fetchone()
            if dup:
                continue

            description = (
                f"Keyword spike detected\n"
                f"Keyword:   {keyword}\n"
                f"Platform:  {platform or 'all'}\n"
                f"Recent (30m): {recent_count} posts\n"
                f"Baseline/30m: {baseline_per_window:.1f} posts\n"
                f"Spike factor: {recent_count / max(baseline_per_window, 1):.1f}x"
            )
            db.create_alert(
                conn,
                alert_type='keyword_spike',
                severity='medium',
                description=description,
                platform=platform,
                keyword=keyword,
            )
            new_alerts += 1
            logger.info("Alert: keyword_spike — '%s' %dx baseline", keyword,
                        int(recent_count / max(baseline_per_window, 1)))

    return new_alerts


# ── Helpers ───────────────────────────────────────────────────────────────────

def _campaign_severity(confidence: float) -> str:
    if confidence >= 0.7:  return 'high'
    if confidence >= 0.4:  return 'medium'
    return 'low'


def _confidence_label(score: float) -> str:
    if score >= 0.7:  return 'HIGH'
    if score >= 0.4:  return 'MEDIUM'
    return 'LOW'


def _time_window_str(first: str, last: str) -> str:
    if not first or not last:
        return 'unknown'
    a = _parse_ts(first)
    b = _parse_ts(last)
    if a and b:
        delta = b - a
        mins = int(delta.total_seconds() / 60)
        return f"{mins} min"
    return 'unknown'


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
