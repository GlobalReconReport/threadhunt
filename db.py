"""
SQLite database layer.
All reads use cursor.fetchmany(BATCH_SIZE) — never loads full tables.
stream_rows() is the canonical way to iterate over query results.
"""
import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone

import config

logger = logging.getLogger('threadhunt')

BATCH_SIZE = 500
_DB_PATH: Path | None = None


def _get_db_path() -> Path:
    global _DB_PATH
    if _DB_PATH is None:
        _DB_PATH = config.DATA_DIR / 'threadhunt.db'
    return _DB_PATH


@contextmanager
def get_conn():
    """
    Yield a sqlite3 connection with WAL mode.
    Commits on clean exit, rolls back on exception.
    """
    db_path = _get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row

    # Performance pragmas tuned for low-RAM USB environments
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-8000")   # 8 MB page cache
    conn.execute("PRAGMA mmap_size=0")        # Disable mmap on slow USB
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create schema and indexes. Safe to call multiple times."""
    with get_conn() as conn:
        conn.executescript("""
        -- ── Accounts ──────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS accounts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT    NOT NULL,
            platform         TEXT    NOT NULL,
            followers        INTEGER DEFAULT 0,
            following        INTEGER DEFAULT 0,
            post_count       INTEGER DEFAULT 0,
            profile_pic_hash TEXT,
            bio              TEXT,
            bot_score        REAL    DEFAULT 0.0,
            flagged          INTEGER DEFAULT 0,
            timezone_offset  INTEGER DEFAULT NULL,
            posting_entropy  REAL    DEFAULT NULL,
            created_at       TEXT,
            updated_at       TEXT,
            UNIQUE(username, platform)
        );

        -- ── Posts ─────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS posts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id   INTEGER,
            platform     TEXT    NOT NULL,
            post_id      TEXT    NOT NULL,
            content      TEXT,
            content_hash TEXT,
            simhash      INTEGER DEFAULT 0,
            timestamp    TEXT,
            lang         TEXT,
            collected_at TEXT,
            thread_no    INTEGER DEFAULT NULL,
            UNIQUE(platform, post_id),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );

        -- ── Social graph edges (stored flat, no in-memory graph) ─────────────
        CREATE TABLE IF NOT EXISTS relationships (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            source_account TEXT NOT NULL,
            target_account TEXT NOT NULL,
            type           TEXT,
            platform       TEXT,
            UNIQUE(source_account, target_account, type)
        );

        -- ── Campaigns ─────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS campaigns (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword          TEXT,
            platform         TEXT,
            first_seen       TEXT,
            last_seen        TEXT,
            post_count       INTEGER DEFAULT 0,
            account_count    INTEGER DEFAULT 0,
            confidence_score REAL    DEFAULT 0.0,
            active           INTEGER DEFAULT 1
        );

        -- ── Cluster membership ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS clusters (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER,
            post_id     INTEGER,
            cluster_key TEXT,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
            FOREIGN KEY (post_id)     REFERENCES posts(id)
        );

        -- ── Alerts ────────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type   TEXT NOT NULL,
            severity     TEXT NOT NULL,
            description  TEXT,
            platform     TEXT,
            account_ids  TEXT,
            post_ids     TEXT,
            keyword      TEXT,
            acknowledged INTEGER DEFAULT 0,
            created_at   TEXT
        );

        -- ── Watchlist ─────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS watchlist (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            type     TEXT NOT NULL,
            value    TEXT NOT NULL,
            platform TEXT,
            added_at TEXT,
            UNIQUE(type, value, platform)
        );

        -- ── Session tracking ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS sessions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at       TEXT,
            ended_at         TEXT,
            posts_collected  INTEGER DEFAULT 0,
            alerts_triggered INTEGER DEFAULT 0,
            platform         TEXT
        );

        -- ── Indexes ───────────────────────────────────────────────────────────
        CREATE INDEX IF NOT EXISTS idx_posts_simhash    ON posts(simhash);
        CREATE INDEX IF NOT EXISTS idx_posts_timestamp  ON posts(timestamp);
        CREATE INDEX IF NOT EXISTS idx_posts_platform   ON posts(platform);
        CREATE INDEX IF NOT EXISTS idx_posts_account    ON posts(account_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_bscore  ON accounts(bot_score);
        CREATE INDEX IF NOT EXISTS idx_accounts_flagged ON accounts(flagged);
        CREATE INDEX IF NOT EXISTS idx_alerts_ack       ON alerts(acknowledged);
        CREATE INDEX IF NOT EXISTS idx_alerts_severity  ON alerts(severity);
        CREATE INDEX IF NOT EXISTS idx_clusters_camp    ON clusters(campaign_id);
        CREATE INDEX IF NOT EXISTS idx_clusters_key     ON clusters(cluster_key);
        """)

        # Migration: add temporal columns to accounts (existing DBs)
        for col_def in [
            "ALTER TABLE accounts ADD COLUMN timezone_offset INTEGER DEFAULT NULL",
            "ALTER TABLE accounts ADD COLUMN posting_entropy REAL DEFAULT NULL",
        ]:
            try:
                conn.execute(col_def)
            except Exception:
                pass  # column already exists

        # Migration: add thread_no column to existing DBs that predate it
        try:
            conn.execute("ALTER TABLE posts ADD COLUMN thread_no INTEGER DEFAULT NULL")
            logger.debug("DB migration: added thread_no column to posts")
        except Exception:
            pass  # column already exists — safe to ignore
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_thread_no ON posts(thread_no)")
        except Exception:
            pass  # index already exists

    logger.info("DB initialized: %s", _get_db_path())


# ── Stream helpers ────────────────────────────────────────────────────────────

def stream_rows(conn, query: str, params: tuple = (), batch_size: int = BATCH_SIZE):
    """Generator: yields sqlite3.Row objects from query, batch_size at a time."""
    cursor = conn.cursor()
    cursor.execute(query, params)
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield from batch


# ── Write helpers ─────────────────────────────────────────────────────────────

def upsert_account(conn, username: str, platform: str, **kwargs) -> int | None:
    """
    Insert or update account record.
    Returns the account's integer id.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO accounts
            (username, platform, followers, following, post_count,
             profile_pic_hash, bio, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(username, platform) DO UPDATE SET
            followers        = excluded.followers,
            following        = excluded.following,
            post_count       = excluded.post_count,
            profile_pic_hash = COALESCE(excluded.profile_pic_hash, profile_pic_hash),
            bio              = COALESCE(excluded.bio, bio),
            updated_at       = excluded.updated_at
    """, (
        username, platform,
        kwargs.get('followers', 0),
        kwargs.get('following', 0),
        kwargs.get('post_count', 0),
        kwargs.get('profile_pic_hash'),
        kwargs.get('bio'),
        now, now,
    ))

    row = conn.execute(
        "SELECT id FROM accounts WHERE username=? AND platform=?",
        (username, platform)
    ).fetchone()
    return int(row['id']) if row else None


def insert_post(conn, account_id: int, platform: str, post_id: str,
                content: str, timestamp: str, lang: str = None,
                thread_no: int = None) -> bool:
    """
    Insert post if not already present (platform, post_id unique).
    Returns True if the row was new, False if it was a duplicate.
    """
    from utils.hashing import simhash as compute_simhash, content_hash

    now = datetime.now(timezone.utc).isoformat()
    chash = content_hash(content or '')
    sh = compute_simhash(content or '')

    conn.execute("""
        INSERT OR IGNORE INTO posts
            (account_id, platform, post_id, content, content_hash,
             simhash, timestamp, lang, collected_at, thread_no)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (account_id, platform, post_id, content, chash, sh, timestamp, lang, now,
          thread_no))

    changed = conn.execute("SELECT changes()").fetchone()[0]

    # Backfill thread_no on existing rows that predate this column (INSERT OR IGNORE
    # skips the whole row if it already exists, so patch it separately).
    if not changed and thread_no is not None:
        conn.execute("""
            UPDATE posts SET thread_no = ?
            WHERE platform = ? AND post_id = ? AND thread_no IS NULL
        """, (thread_no, platform, post_id))

    return changed > 0


def create_alert(conn, alert_type: str, severity: str, description: str,
                 platform: str = None, account_ids: list = None,
                 post_ids: list = None, keyword: str = None) -> int:
    """Insert an alert row. Returns new alert id."""
    import json as _json
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute("""
        INSERT INTO alerts
            (alert_type, severity, description, platform,
             account_ids, post_ids, keyword, acknowledged, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
    """, (
        alert_type, severity, description, platform,
        _json.dumps(account_ids or []),
        _json.dumps(post_ids or []),
        keyword, now,
    ))
    return cursor.lastrowid


def start_session(conn, platform: str = None) -> int:
    """Create a session record, return session id."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO sessions (started_at, platform) VALUES (?, ?)",
        (now, platform)
    )
    return cursor.lastrowid


def end_session(conn, session_id: int, posts_collected: int, alerts_triggered: int):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE sessions
        SET ended_at=?, posts_collected=?, alerts_triggered=?
        WHERE id=?
    """, (now, posts_collected, alerts_triggered, session_id))
