import sqlite3
import os
import statistics

DB_PATH = "../data/comments.db"


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
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def insert_comment(comment):
    """
    Insert a comment dict into the database.
    Expects keys:
      comment_id, video_id, author_id, text, published_at, fetched_at
    """
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

        conn.commit()

    except sqlite3.IntegrityError:
        # duplicate comment_id — ignore
        pass

    finally:
        conn.close()


def get_window_metrics(start_time, end_time, video_id=None):
    """
    Aggregate metrics for comments in a time window.

    If video_id is provided, filter metrics to that video.

    Returns:
        dict with:
        - total_comments
        - unique_authors
        - avg_length
        - avg_sentiment
        - sentiment_variance
    """

    conn = get_connection()
    cur = conn.cursor()

    where_clause = "AND video_id = ?" if video_id else ""
    params = (start_time, end_time, video_id) if video_id else (start_time, end_time)

    cur.execute(f"""
        SELECT
            COUNT(*) AS total_comments,
            COUNT(DISTINCT author_id) AS unique_authors,
            AVG(LENGTH(text)) AS avg_length,
            AVG(sentiment) AS avg_sentiment
        FROM comments
        WHERE published_at BETWEEN ? AND ?
        {where_clause}
    """, params)

    row = cur.fetchone()

    # variance
    cur.execute(f"""
        SELECT sentiment
        FROM comments
        WHERE published_at BETWEEN ? AND ?
        AND sentiment IS NOT NULL
        {where_clause}
    """, params)

    rows = cur.fetchall()
    conn.close()

    sentiments = [r[0] for r in rows]
    sentiment_variance = statistics.pvariance(sentiments) if len(sentiments) > 1 else 0

    return {
        "total_comments": row[0] or 0,
        "unique_authors": row[1] or 0,
        "avg_length": row[2] or 0,
        "avg_sentiment": row[3] or 0,
        "sentiment_variance": sentiment_variance
    }
def get_all_window_metrics(video_id=None, polling_rate=600):
    conn = get_connection()
    cur = conn.cursor()

    where_clause = "WHERE video_id = ?" if video_id else ""
    params = (video_id,) if video_id else ()

    # build window expression with polling_rate
    window_expr = (
        f"datetime((strftime('%s', published_at) / {polling_rate}) * {polling_rate}, 'unixepoch')"
    )

    query = f"""
        SELECT
            video_id,
            {window_expr} AS window,
            COUNT(*) AS total_comments,
            COUNT(DISTINCT author_id) AS unique_authors,
            AVG(LENGTH(text)) AS avg_length,
            AVG(sentiment) AS avg_sentiment
        FROM comments
        {where_clause}
        GROUP BY video_id, window
        ORDER BY window
    """

    cur.execute(query, params)
    rows = cur.fetchall()

    results = []

    for r in rows:
        video = r[0]
        window = r[1]

        # same window expression for sentiment lookup
        sentiment_window_expr = window_expr

        cur.execute(f"""
            SELECT sentiment
            FROM comments
            WHERE video_id = ?
              AND {sentiment_window_expr} = ?
              AND sentiment IS NOT NULL
        """, (video, window))

        sentiments = [s[0] for s in cur.fetchall()]
        variance = statistics.pvariance(sentiments) if len(sentiments) > 1 else 0

        results.append({
            "video_id": video,
            "window": window,
            "total_comments": r[2],
            "unique_authors": r[3],
            "avg_length": r[4],
            "avg_sentiment": r[5],
            "sentiment_variance": variance
        })

    conn.close()
    return results

    conn.close()
    return results


def fetch_comments_by_video(video_id):
    conn = get_connection()
    cur = conn.cursor()


    cur.execute("""
        SELECT comment_id, text
        FROM comments
        WHERE video_id = ?
    """, (video_id,))

    rows = cur.fetchall()
    conn.close()

    return [
        {"comment_id": row[0], "text": row[1]}
        for row in rows
    ]

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
    """
    Normalize window timestamp to SQLite-friendly format:
    'YYYY-MM-DD HH:MM:SS'

    Accepts ISO timestamps with T/Z or SQLite format.
    """
    try:
        # handle ISO with timezone: 2026-02-26T20:38:35+00:00
        dt = datetime.fromisoformat(window_str.replace("Z", "+00:00"))
    except Exception:
        try:
            # already SQLite-style
            dt = datetime.strptime(window_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return window_str  # fallback (should rarely happen)

    return dt.strftime("%Y-%m-%d %H:%M:%S")
