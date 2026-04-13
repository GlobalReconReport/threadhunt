"""
JSON export — full state dump for sharing or importing into other tools.
Streams from DB; never loads entire tables into memory.
"""
import json
import logging
from datetime import datetime, timezone

import db

logger = logging.getLogger('threadhunt')


def export(output_path: str, platform: str = None,
           keyword: str = None, time_window: str = None) -> int:
    """
    Export campaigns, alerts, and accounts to a JSON file.
    Returns count of records written.
    """
    filters = _build_filters(platform, keyword, time_window)
    count = 0

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('{\n')
        f.write(f'  "exported_at": "{datetime.now(timezone.utc).isoformat()}",\n')

        # Campaigns
        f.write('  "campaigns": [\n')
        first = True
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, _campaigns_query(filters),
                                      filters.get('params', ())):
                campaign = dict(row)
                campaign['posts'] = _get_campaign_posts(
                    conn, campaign['id'], keyword
                )
                if not first:
                    f.write(',\n')
                f.write('    ' + json.dumps(campaign, ensure_ascii=False))
                first = False
                count += 1
        f.write('\n  ],\n')

        # Alerts
        f.write('  "alerts": [\n')
        first = True
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, _alerts_query(filters),
                                      filters.get('alert_params', ())):
                alert = dict(row)
                if not first:
                    f.write(',\n')
                f.write('    ' + json.dumps(alert, ensure_ascii=False))
                first = False
        f.write('\n  ],\n')

        # Flagged accounts
        f.write('  "flagged_accounts": [\n')
        first = True
        with db.get_conn() as conn:
            q = "SELECT * FROM accounts WHERE flagged=1 ORDER BY bot_score DESC LIMIT 1000"
            if platform:
                q = "SELECT * FROM accounts WHERE flagged=1 AND platform=? ORDER BY bot_score DESC LIMIT 1000"
                params = (platform,)
            else:
                params = ()
            for row in db.stream_rows(conn, q, params):
                if not first:
                    f.write(',\n')
                f.write('    ' + json.dumps(dict(row), ensure_ascii=False))
                first = False
        f.write('\n  ]\n')

        f.write('}\n')

    logger.info("JSON export: %d campaigns written to %s", count, output_path)
    return count


def _build_filters(platform, keyword, time_window) -> dict:
    params = []
    alert_params = []
    where_clauses = []
    alert_where = []

    if platform:
        where_clauses.append("c.platform=?")
        params.append(platform)
        alert_where.append("platform=?")
        alert_params.append(platform)
    if keyword:
        where_clauses.append("c.keyword LIKE ?")
        params.append(f'%{keyword}%')
        alert_where.append("keyword LIKE ?")
        alert_params.append(f'%{keyword}%')
    if time_window:
        # time_window = "24h" or "7d" etc.
        cutoff = _parse_time_window(time_window)
        if cutoff:
            where_clauses.append("c.last_seen >= ?")
            params.append(cutoff)
            alert_where.append("created_at >= ?")
            alert_params.append(cutoff)

    return {
        'where':        ' AND '.join(where_clauses) if where_clauses else '1=1',
        'params':       tuple(params),
        'alert_where':  ' AND '.join(alert_where) if alert_where else '1=1',
        'alert_params': tuple(alert_params),
    }


def _campaigns_query(filters: dict) -> str:
    return f"""
        SELECT c.*
        FROM campaigns c
        WHERE {filters['where']}
        ORDER BY c.last_seen DESC
        LIMIT 500
    """


def _alerts_query(filters: dict) -> str:
    return f"""
        SELECT * FROM alerts
        WHERE {filters['alert_where']}
        ORDER BY created_at DESC
        LIMIT 500
    """


def _get_campaign_posts(conn, campaign_id: int, keyword_filter: str = None) -> list:
    posts = []
    for row in db.stream_rows(conn, """
        SELECT p.post_id, p.content, p.timestamp, p.platform, p.lang,
               a.username, a.bot_score
        FROM clusters cl
        JOIN posts p    ON p.id = cl.post_id
        JOIN accounts a ON a.id = p.account_id
        WHERE cl.campaign_id = ?
        ORDER BY p.timestamp DESC
        LIMIT 50
    """, (campaign_id,)):
        post = dict(row)
        if keyword_filter and keyword_filter.lower() not in (post.get('content') or '').lower():
            continue
        posts.append(post)
    return posts


def _parse_time_window(tw: str) -> str | None:
    """Convert '24h' / '7d' to ISO cutoff timestamp."""
    from datetime import timedelta
    try:
        if tw.endswith('h'):
            delta = timedelta(hours=int(tw[:-1]))
        elif tw.endswith('d'):
            delta = timedelta(days=int(tw[:-1]))
        else:
            return None
        return (datetime.now(timezone.utc) - delta).isoformat()
    except Exception:
        return None
