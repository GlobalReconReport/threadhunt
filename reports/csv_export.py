"""
CSV export — flat tabular format for spreadsheet analysis or ingestion into
external OSINT platforms.
Exports: posts, accounts, campaigns, alerts as separate CSV files in a directory.
"""
import csv
import logging
import os
from datetime import datetime, timezone

import db

logger = logging.getLogger('threadhunt')


def export(output_dir: str, platform: str = None,
           keyword: str = None, time_window: str = None) -> dict:
    """
    Write four CSV files to output_dir.
    Returns dict of {filename: row_count}.
    """
    os.makedirs(output_dir, exist_ok=True)
    ts_slug = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    results = {}

    results['accounts'] = _export_accounts(output_dir, ts_slug, platform)
    results['posts']    = _export_posts(output_dir, ts_slug, platform, keyword, time_window)
    results['campaigns']= _export_campaigns(output_dir, ts_slug, platform, keyword)
    results['alerts']   = _export_alerts(output_dir, ts_slug, platform, keyword)

    return results


def _export_accounts(out_dir: str, ts: str, platform: str) -> int:
    path = os.path.join(out_dir, f'accounts_{ts}.csv')
    fields = ['id', 'username', 'platform', 'followers', 'following',
              'post_count', 'bot_score', 'flagged', 'bio', 'updated_at']
    count = 0

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        with db.get_conn() as conn:
            q = "SELECT * FROM accounts"
            p = ()
            if platform:
                q += " WHERE platform=?"
                p = (platform,)
            q += " ORDER BY bot_score DESC LIMIT 10000"
            for row in db.stream_rows(conn, q, p):
                writer.writerow(dict(row))
                count += 1

    logger.info("CSV: %d accounts → %s", count, path)
    return count


def _export_posts(out_dir: str, ts: str, platform: str,
                  keyword: str, time_window: str) -> int:
    path = os.path.join(out_dir, f'posts_{ts}.csv')
    fields = ['id', 'account_id', 'platform', 'post_id', 'content',
              'simhash', 'timestamp', 'lang']
    count = 0

    where = []
    params = []
    if platform:
        where.append("p.platform=?")
        params.append(platform)
    if keyword:
        where.append("p.content LIKE ?")
        params.append(f'%{keyword}%')
    if time_window:
        cutoff = _parse_tw(time_window)
        if cutoff:
            where.append("p.timestamp >= ?")
            params.append(cutoff)

    where_str = ' AND '.join(where) if where else '1=1'

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, f"""
                SELECT p.id, p.account_id, p.platform, p.post_id,
                       p.content, p.simhash, p.timestamp, p.lang
                FROM posts p
                WHERE {where_str}
                ORDER BY p.timestamp DESC
                LIMIT 50000
            """, tuple(params)):
                writer.writerow(dict(row))
                count += 1

    logger.info("CSV: %d posts → %s", count, path)
    return count


def _export_campaigns(out_dir: str, ts: str, platform: str, keyword: str) -> int:
    path = os.path.join(out_dir, f'campaigns_{ts}.csv')
    fields = ['id', 'keyword', 'platform', 'first_seen', 'last_seen',
              'post_count', 'account_count', 'confidence_score', 'active']
    count = 0

    where = []
    params = []
    if platform:
        where.append("platform=?")
        params.append(platform)
    if keyword:
        where.append("keyword LIKE ?")
        params.append(f'%{keyword}%')

    where_str = ' AND '.join(where) if where else '1=1'

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, f"""
                SELECT * FROM campaigns WHERE {where_str}
                ORDER BY last_seen DESC LIMIT 5000
            """, tuple(params)):
                writer.writerow(dict(row))
                count += 1

    logger.info("CSV: %d campaigns → %s", count, path)
    return count


def _export_alerts(out_dir: str, ts: str, platform: str, keyword: str) -> int:
    path = os.path.join(out_dir, f'alerts_{ts}.csv')
    fields = ['id', 'alert_type', 'severity', 'description', 'platform',
              'keyword', 'acknowledged', 'created_at']
    count = 0

    where = []
    params = []
    if platform:
        where.append("platform=?")
        params.append(platform)
    if keyword:
        where.append("keyword LIKE ?")
        params.append(f'%{keyword}%')

    where_str = ' AND '.join(where) if where else '1=1'

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, f"""
                SELECT id, alert_type, severity, description, platform,
                       keyword, acknowledged, created_at
                FROM alerts WHERE {where_str}
                ORDER BY created_at DESC LIMIT 5000
            """, tuple(params)):
                writer.writerow(dict(row))
                count += 1

    logger.info("CSV: %d alerts → %s", count, path)
    return count


def _parse_tw(tw: str) -> str | None:
    from datetime import timedelta
    try:
        if tw.endswith('h'):
            d = timedelta(hours=int(tw[:-1]))
        elif tw.endswith('d'):
            d = timedelta(days=int(tw[:-1]))
        else:
            return None
        return (datetime.now(timezone.utc) - d).isoformat()
    except Exception:
        return None
