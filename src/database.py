import sqlite3
import os
import statistics

DB_PATH = "../data/comments.db"


def init_db():
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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

    conn.commit()
    conn.close()


def insert_comment(comment):
    """
    Insert a comment dict into the database.
    Expects keys:
      comment_id, video_id, author_id, text, published_at, fetched_at
    """
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        cur.execute("""
        INSERT INTO comments (
            comment_id,
            video_id,
            author_id,
            text,
            published_at,
            fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            comment["comment_id"],
            comment["video_id"],
            comment["author_id"],
            comment["text"],
            comment["published_at"],
            comment["fetched_at"],
        ))

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

    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
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
def get_all_window_metrics(video_id=None):
    DB_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    where_clause = "WHERE video_id = ?" if video_id else ""
    params = (video_id,) if video_id else ()

    query = f"""
        SELECT
            video_id,
            datetime((strftime('%s', published_at) / 300) * 300, 'unixepoch') AS window,
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

        # fetch sentiments for this window only
        cur.execute("""
            SELECT sentiment
            FROM comments
            WHERE video_id = ?
              AND datetime((strftime('%s', published_at) / 300) * 300, 'unixepoch') = ?
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


def fetch_comments_by_video(video_id):
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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


def update_comment_sentiment(comment_id, sentiment):
    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE comments
        SET sentiment = ?
        WHERE comment_id = ?
    """, (sentiment, comment_id))

    conn.commit()
    conn.close()

