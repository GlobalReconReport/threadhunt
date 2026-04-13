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
    total += check_high_bot_scores(conn)
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


# ── Trigger 2: Bot score threshold ───────────────────────────────────────────

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


# ── Trigger 3: Narrative re-emergence ────────────────────────────────────────

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


# ── Trigger 4: Keyword spike ──────────────────────────────────────────────────

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
