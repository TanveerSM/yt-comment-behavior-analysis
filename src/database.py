import sqlite3
import os
import statistics
from datetime import datetime


# Calculate the absolute path
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "comments.db"))

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        comment_id TEXT PRIMARY KEY,
        video_id TEXT,
        author_id TEXT,
        text TEXT,
        sentiment REAL,
        published_at TEXT,
        fetched_at TEXT
    )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS window_metrics(
            video_id TEXT,
            window_start TEXT,
            total_comments INTEGER,
            unique_authors INTEGER,
            avg_length REAL,
            avg_sentiment REAL,
            sentiment_variance REAL,
            coordination_score REAL,
            PRIMARY KEY (video_id, window_start)
        )
        """)

    conn.commit()
    conn.close()

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def insert_comment(comment):
    # 1. Normalize timestamps to your preferred ISO format before inserting
    # This ensures your BETWEEN queries in get_window_metrics always find matches.
    comment["published_at"] = normalize_window(comment["published_at"])
    comment["fetched_at"] = normalize_window(comment["fetched_at"])

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
        INSERT INTO comments (
            comment_id,
            video_id,
            author_id,
            text,
            published_at,
            fetched_at,
            sentiment
        )
        VALUES (
            :comment_id, 
            :video_id, 
            :author_id, 
            :text, 
            :published_at, 
            :fetched_at, 
            :sentiment
        )
        """, comment)

        conn.commit() # Safely saves the changes

    except sqlite3.IntegrityError:
        # Handles duplicate comment_ids if you fetch the same data twice
        pass

    finally:
        conn.close()



def get_window_metrics(start_time, end_time, video_id=None):
    # Ensure start/end are both in the same ISO format
    norm_start = normalize_window(start_time)
    norm_end = normalize_window(end_time)

    conn = get_connection()
    cur = conn.cursor()

    where_clause = "WHERE published_at BETWEEN ? AND ?"
    params = [norm_start, norm_end]

    if video_id:
        where_clause += " AND video_id = ?"
        params.append(video_id)

    query = f"""
        SELECT
            COUNT(*) AS total_comments,
            COUNT(DISTINCT author_id) AS unique_authors,
            AVG(LENGTH(text)) AS avg_length,
            AVG(sentiment) AS avg_sentiment,
            AVG(sentiment * sentiment) - (AVG(sentiment) * AVG(sentiment)) AS sentiment_variance
        FROM comments
        {where_clause}
    """

    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()

    # Handle the case where no comments exist in the window
    if not row or row[0] == 0:
        return {"total_comments": 0, "unique_authors": 0, "avg_length": 0, "avg_sentiment": 0, "sentiment_variance": 0}

    return {
        "total_comments": row[0],
        "unique_authors": row[1],
        "avg_length": row[2] or 0,
        "avg_sentiment": row[3] or 0,
        "sentiment_variance": max(0.0, row[4]) if row[4] is not None else 0.0
    }


def get_all_window_metrics(video_id=None, polling_rate=600):
    conn = get_connection()
    cur = conn.cursor()

    where_clause = "WHERE video_id = ?" if video_id else ""
    params = (video_id,) if video_id else ()

    # Inside get_all_window_metrics, change your window_expr to:
    window_expr = f"datetime((unixepoch(published_at) / {polling_rate}) * {polling_rate}, 'unixepoch')"
    # To keep it ISO, wrap it in strftime:
    window_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ', (unixepoch(published_at) / {polling_rate}) * {polling_rate}, 'unixepoch')"

    # Calculate variance using: AVG(x*x) - AVG(x)*AVG(x)
    query = f"""
        SELECT
            video_id,
            {window_expr} AS window,
            COUNT(*) AS total_comments,
            COUNT(DISTINCT author_id) AS unique_authors,
            AVG(LENGTH(text)) AS avg_length,
            AVG(sentiment) AS avg_sentiment,
            AVG(sentiment * sentiment) - (AVG(sentiment) * AVG(sentiment)) AS sentiment_variance
        FROM comments
        {where_clause}
        GROUP BY video_id, window
        ORDER BY window
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [{
        "video_id": r[0],
        "window": r[1],
        "total_comments": r[2],
        "unique_authors": r[3],
        "avg_length": r[4],
        "avg_sentiment": r[5],
        "sentiment_variance": max(0, r[6]) # max(0,...) prevents tiny floating point errors (< 0)
    } for r in rows]



def insert_window_metrics(metrics):
    """
    Insert or update window metrics.

    Expects metrics dict with keys:
      video_id
      window
      total_comments
      unique_authors
      avg_length
      avg_sentiment
      sentiment_variance
      coordination_score
    """

    conn = get_connection()
    cur = conn.cursor()
    normalized_window = normalize_window(metrics["window"])
    cur.execute("""
    INSERT INTO window_metrics (
        video_id,
        window_start,
        total_comments,
        unique_authors,
        avg_length,
        avg_sentiment,
        sentiment_variance,
        coordination_score
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id, window_start) DO UPDATE SET
        total_comments = excluded.total_comments,
        unique_authors = excluded.unique_authors,
        avg_length = excluded.avg_length,
        avg_sentiment = excluded.avg_sentiment,
        sentiment_variance = excluded.sentiment_variance,
        coordination_score = excluded.coordination_score;
    """, (
        metrics["video_id"],
        normalized_window,
        metrics["total_comments"],
        metrics["unique_authors"],
        metrics["avg_length"],
        metrics["avg_sentiment"],
        metrics["sentiment_variance"],
        metrics.get("coordination_score"),
    ))

    conn.commit()
    conn.close()

def normalize_window(window_str):
    try:
        # Standardize everything to a UTC datetime object
        dt = datetime.fromisoformat(window_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(window_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return window_str

    # Return as '2026-02-26T18:10:00+00:00'
    return dt.isoformat(timespec='seconds')

