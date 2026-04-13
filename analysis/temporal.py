"""
Temporal analysis.
- Posting hour distribution per account / platform.
- Entropy scoring (low entropy = scheduled posting = bot indicator).
- Synchronized activity burst detection.
- Timezone inference from posting patterns.
"""
import math
import logging
from datetime import datetime, timezone, timedelta

import db

logger = logging.getLogger('threadhunt')


# ── Posting hour distribution ─────────────────────────────────────────────────

def hour_distribution(account_id: int, conn) -> list:
    """
    Return list of 24 ints — count of posts per UTC hour for this account.
    Uses last 1000 posts.
    """
    counts = [0] * 24
    for row in db.stream_rows(conn, """
        SELECT timestamp FROM posts
        WHERE account_id=?
        ORDER BY timestamp DESC
        LIMIT 1000
    """, (account_id,)):
        hour = _hour_from_ts(row[0])
        if hour is not None:
            counts[hour] += 1
    return counts


def posting_entropy(hour_counts: list) -> float:
    """
    Shannon entropy of the 24-bin hour distribution.
    Max = log2(24) ≈ 4.58 (perfectly spread across all hours — organic).
    Low entropy (< 2.0) = posting concentrated in a few hours — scheduled/bot.
    """
    total = sum(hour_counts)
    if total == 0:
        return 0.0
    entropy = 0.0
    for count in hour_counts:
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)
    return entropy


def infer_timezone_offset(hour_counts: list) -> int | None:
    """
    Infer likely UTC offset by finding the trough (least active hours).
    Humans typically sleep between 01:00–07:00 local time.
    Returns UTC offset in hours (e.g. +3 for Moscow time), or None.
    """
    if sum(hour_counts) < 20:
        return None

    # Find the 6-hour window with least activity (the "sleep" window)
    min_activity = float('inf')
    best_start = 0
    for start in range(24):
        window = sum(hour_counts[(start + i) % 24] for i in range(6))
        if window < min_activity:
            min_activity = window
            best_start = start

    # Sleep trough starts around 01:00 local, so:
    # best_start (UTC) = 01:00 local → local = best_start - 1
    # UTC offset = local - UTC = (best_start - 1) - best_start = -1
    # More precisely: if trough at UTC hour X, local 01:00 = X, so offset = X - 1
    offset = (best_start - 1) % 24
    if offset > 12:
        offset -= 24   # Convert to signed offset
    return offset


# ── Synchronized burst detection ─────────────────────────────────────────────

def detect_synchronized_bursts(conn, platform: str,
                                window_minutes: int = 15,
                                min_accounts: int = 5,
                                lookback_hours: int = 48) -> list:
    """
    Find time windows where >= min_accounts posted within window_minutes.
    Returns list of burst dicts: {start, end, account_count, post_count, platform}.
    Streamed in batches — no full table load.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    window_delta = timedelta(minutes=window_minutes)

    # Build an in-memory event list (bounded by lookback window)
    events = []   # (datetime, account_id)
    for row in db.stream_rows(conn, """
        SELECT account_id, timestamp FROM posts
        WHERE platform=? AND timestamp >= ?
        ORDER BY timestamp ASC
        LIMIT 5000
    """, (platform, cutoff)):
        dt = _parse_ts(row[1])
        if dt:
            events.append((dt, row[0]))

    if len(events) < min_accounts:
        return []

    bursts = []
    reported_starts = set()

    for i, (dt_i, _) in enumerate(events):
        window_end = dt_i + window_delta

        accounts_in = set()
        posts_in = 0
        j = i
        while j < len(events) and events[j][0] <= window_end:
            accounts_in.add(events[j][1])
            posts_in += 1
            j += 1

        if len(accounts_in) >= min_accounts:
            key = dt_i.replace(second=0, microsecond=0).isoformat()
            if key not in reported_starts:
                reported_starts.add(key)
                bursts.append({
                    'start':         dt_i.isoformat(),
                    'end':           window_end.isoformat(),
                    'account_count': len(accounts_in),
                    'post_count':    posts_in,
                    'platform':      platform,
                })

    return bursts


# ── Account-level temporal profile ───────────────────────────────────────────

def account_temporal_profile(account_id: int, conn) -> dict:
    """
    Full temporal profile for a single account.
    Returns dict suitable for display and alert scoring.
    """
    dist    = hour_distribution(account_id, conn)
    entropy = posting_entropy(dist)
    tz      = infer_timezone_offset(dist)

    # Active hours: top 3 UTC hours by post count
    indexed = sorted(enumerate(dist), key=lambda x: x[1], reverse=True)
    active_hours = [h for h, _ in indexed[:3] if dist[h] > 0]

    return {
        'hour_distribution': dist,
        'entropy':           round(entropy, 3),
        'likely_tz_offset':  tz,
        'active_hours_utc':  active_hours,
        'total_posts':       sum(dist),
        'is_scheduled':      entropy < 2.0 and sum(dist) >= 10,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hour_from_ts(ts: str) -> int | None:
    """Extract UTC hour from ISO timestamp string."""
    if not ts:
        return None
    try:
        dt = _parse_ts(ts)
        return dt.hour if dt else None
    except Exception:
        return None


def _parse_ts(ts: str) -> datetime | None:
    if not ts:
        return None
    ts = ts.rstrip('Z').split('+')[0]
    formats = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None
