import sqlite3
import os
from datetime import datetime


# Calculate the absolute path
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "comments.db"))

METRIC_COLUMNS = """
    COUNT(*) AS total_comments,
    COUNT(DISTINCT author_id) AS unique_authors,
    AVG(LENGTH(text)) AS avg_length,
    AVG(sentiment) AS avg_sentiment,
    AVG(sentiment * sentiment) - (AVG(sentiment) * AVG(sentiment)) AS sentiment_variance,
    AVG(gap) AS avg_gap,
    AVG(gap * gap) - (AVG(gap) * AVG(gap)) AS gap_variance
"""



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
            avg_gap REAL,           
            gap_variance REAL,      
            coordination_score REAL,
            PRIMARY KEY (video_id, window_start)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vid_time ON comments (video_id, published_at)")
    conn.commit()
    conn.close()

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")  # Enables concurrent Readers/Writers
    conn.execute("PRAGMA synchronous=NORMAL;") # Reduces fsync() calls
    return conn


def insert_comments_batch(comments):
    """Inserts a list of comments in a single transaction"""
    conn = get_connection()
    cur = conn.cursor()

    # Normalization happens in a list comprehension
    for c in comments:
        c["published_at"] = normalize_window(c["published_at"])
        c["fetched_at"] = normalize_window(c["fetched_at"])

    try:
        # IGNORE handles the IntegrityError (duplicates) automatically in SQL
        cur.executemany("""
                        INSERT OR IGNORE INTO comments (comment_id, video_id, author_id, text, published_at, fetched_at,
                                                        sentiment)
                        VALUES (:comment_id, :video_id, :author_id, :text, :published_at, :fetched_at, :sentiment)
                        """, comments)
        conn.commit()
    finally:
        conn.close()


def get_window_metrics(start_time, end_time, video_id=None):
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
        WITH Gaps AS (
            SELECT 
                video_id,
                author_id,
                sentiment,
                LENGTH(text) as text_len,
                unixepoch(published_at) - LAG(unixepoch(published_at)) OVER (ORDER BY published_at) AS gap
            FROM comments
            {where_clause}
        )
        SELECT 
            video_id,                                   -- Index 0
            '{norm_start}',                             -- Index 1 (The window label)
            COUNT(*) AS total_comments,                 -- Index 2
            COUNT(DISTINCT author_id) AS unique_authors, -- Index 3
            AVG(text_len),                              -- Index 4
            AVG(sentiment),                             -- Index 5
            MAX(0.0, AVG(sentiment * sentiment) - (AVG(sentiment) * AVG(sentiment))), -- Index 6
            AVG(gap),                                   -- Index 7
            MAX(0.0, AVG(gap * gap) - (AVG(gap) * AVG(gap))) -- Index 8
        FROM Gaps
    """

    cur.execute(query, params)
    r = cur.fetchone()
    conn.close()

    # Safety check: If no comments found
    if not r or r[2] == 0:
        return {
            "video_id": video_id, "window": norm_start, "total_comments": 0,
            "unique_authors": 0, "avg_length": 0, "avg_sentiment": 0,
            "sentiment_variance": 0, "avg_gap": 0, "gap_variance": 0
        }


    return {
        "video_id": r[0],
        "window": r[1],
        "total_comments": r[2],
        "unique_authors": r[3],
        "avg_length": r[4] or 0,
        "avg_sentiment": r[5] or 0,
        "sentiment_variance": max(0.0, r[6]) if r[6] is not None else 0.0,
        "avg_gap": r[7] or 0,
        "gap_variance": max(0.0, r[8]) if r[8] is not None else 0.0
    }




def get_all_window_metrics(video_id=None, polling_rate=600):
    conn = get_connection()
    cur = conn.cursor()

    where_clause = "WHERE video_id = ?" if video_id else ""
    params = (video_id,) if video_id else ()

    window_expr = f"strftime('%Y-%m-%dT%H:%M:%SZ', (unixepoch(published_at) / {polling_rate}) * {polling_rate}, 'unixepoch')"

    query = f"""
        WITH TimedComments AS (
            SELECT 
                video_id, 
                author_id,
                sentiment,
                LENGTH(text) as text_len,
                unixepoch(published_at) as ts,
                unixepoch(published_at) - LAG(unixepoch(published_at)) OVER (
                    PARTITION BY video_id ORDER BY published_at
                ) AS gap,
                strftime('%Y-%m-%dT%H:%M:%SZ', (unixepoch(published_at) / {polling_rate}) * {polling_rate}, 'unixepoch') AS window
            FROM comments
            {where_clause}
        )
        SELECT
            video_id,
            window,
            COUNT(*) as total_comments,
            COUNT(DISTINCT author_id) as unique_authors,
            AVG(text_len) as avg_length,
            AVG(sentiment) as avg_sentiment,
            MAX(0.0, AVG(sentiment * sentiment) - (AVG(sentiment) * AVG(sentiment))) as sentiment_variance,
            AVG(gap) as avg_gap,
            MAX(0.0, AVG(gap * gap) - (AVG(gap) * AVG(gap))) as gap_variance
        FROM TimedComments
        GROUP BY video_id, window
        ORDER BY window ASC
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [{
        "video_id": r[0],
        "window": r[1],
        "total_comments": r[2],
        "unique_authors": r[3],
        "avg_length": r[4] or 0,
        "avg_sentiment": r[5] or 0,
        "sentiment_variance": max(0.0, r[6]) if r[6] is not None else 0.0,
        "avg_gap": r[7] or 0,
        "gap_variance": max(0.0, r[8]) if r[8] is not None else 0.0
    } for r in rows]


def insert_window_metrics(metrics):
    """
    Insert or update window metrics using named placeholders for clarity.
    """
    conn = get_connection()
    cur = conn.cursor()

    # 1. Prepare a clean copy of the dictionary for the SQL execution
    # This ensures we have all keys even if the input metrics dict is missing some
    data = {
        "video_id": metrics.get("video_id"),
        "window_start": normalize_window(metrics.get("window", "")),
        "total_comments": metrics.get("total_comments", 0),
        "unique_authors": metrics.get("unique_authors", 0),
        "avg_length": metrics.get("avg_length", 0),
        "avg_sentiment": metrics.get("avg_sentiment", 0),
        "sentiment_variance": metrics.get("sentiment_variance", 0),
        "avg_gap": metrics.get("avg_gap", 0),
        "gap_variance": metrics.get("gap_variance", 0),
        "coordination_score": metrics.get("coordination_score")
    }

    try:
        cur.execute("""
        INSERT INTO window_metrics (
            video_id,
            window_start,
            total_comments,
            unique_authors,
            avg_length,
            avg_sentiment,
            sentiment_variance,
            avg_gap,
            gap_variance,
            coordination_score
        )
        VALUES (
            :video_id, 
            :window_start, 
            :total_comments, 
            :unique_authors, 
            :avg_length, 
            :avg_sentiment, 
            :sentiment_variance, 
            :avg_gap, 
            :gap_variance, 
            :coordination_score
        )
        ON CONFLICT(video_id, window_start) DO UPDATE SET
            total_comments = excluded.total_comments,
            unique_authors = excluded.unique_authors,
            avg_length = excluded.avg_length,
            avg_sentiment = excluded.avg_sentiment,
            sentiment_variance = excluded.sentiment_variance,
            avg_gap = excluded.avg_gap,
            gap_variance = excluded.gap_variance,
            coordination_score = excluded.coordination_score;
        """, data)

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in insert_window_metrics: {e}")
    finally:
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

