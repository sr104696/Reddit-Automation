# Hardened database helpers with local imports and env-based db path resolution
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_DB_CANDIDATES = [
    os.environ.get('SHARED_DB_PATH', ''),
    os.environ.get('DATABASE_PATH', ''),
    './reddit_automation.db',
    './data/shared_db/reddit_automation.db',
    '/data/shared_db/reddit_automation.db'
]


def _resolve_db_path() -> str:
    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate and Path(candidate).exists():
            return str(Path(candidate))
    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate:
            parent = Path(candidate).parent
            if str(parent) not in ('', '.'):
                parent.mkdir(parents=True, exist_ok=True)
            return str(Path(candidate))
    return './reddit_automation.db'


DB_PATH = _resolve_db_path()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_all(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def search_posts(subreddit: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    limit_val = max(1, min(int(limit), 1000))
    if subreddit:
        return _fetch_all(
            'SELECT * FROM posts WHERE subreddit = ? ORDER BY created_utc DESC LIMIT ?',
            (subreddit, limit_val)
        )
    return _fetch_all('SELECT * FROM posts ORDER BY created_utc DESC LIMIT ?', (limit_val,))


def search_comments(subreddit: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    limit_val = max(1, min(int(limit), 1000))
    if subreddit:
        return _fetch_all(
            'SELECT * FROM comments WHERE subreddit = ? ORDER BY created_utc DESC LIMIT ?',
            (subreddit, limit_val)
        )
    return _fetch_all('SELECT * FROM comments ORDER BY created_utc DESC LIMIT ?', (limit_val,))


def get_all_subreddits() -> List[str]:
    rows = _fetch_all('SELECT DISTINCT subreddit FROM posts WHERE subreddit IS NOT NULL ORDER BY subreddit')
    return [row['subreddit'] for row in rows if row.get('subreddit')]


def get_subreddit_stats() -> List[Dict[str, Any]]:
    return _fetch_all(
        'SELECT subreddit, COUNT(*) AS post_count, AVG(score) AS avg_score FROM posts '
        'WHERE subreddit IS NOT NULL GROUP BY subreddit ORDER BY post_count DESC'
    )


def get_job_history(limit: int = 100) -> List[Dict[str, Any]]:
    limit_val = max(1, min(int(limit), 1000))
    return _fetch_all('SELECT * FROM job_history ORDER BY started_at DESC LIMIT ?', (limit_val,))


def get_job_stats() -> Dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        stats = {}
        for label, query in {
            'posts': 'SELECT COUNT(*) FROM posts',
            'comments': 'SELECT COUNT(*) FROM comments',
            'jobs': 'SELECT COUNT(*) FROM job_history'
        }.items():
            try:
                stats[label] = cur.execute(query).fetchone()[0]
            except sqlite3.Error:
                stats[label] = 0
        return stats
    finally:
        conn.close()


def get_database_info() -> Dict[str, Any]:
    conn = get_connection()
    try:
        tables = _fetch_all("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return {
            'db_path': DB_PATH,
            'exists': Path(DB_PATH).exists(),
            'tables': [row['name'] for row in tables]
        }
    finally:
        conn.close()
