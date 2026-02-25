import sqlite3
import os

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

def get_window_metrics(start_time, end_time):
    """
    Aggregate metrics for comments in a time window.

    Returns:
        dict with:
        - total_comments
        - unique_authors
        - avg_length
    """

    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    SELECT
        COUNT(*) AS total_comments,
        COUNT(DISTINCT author_id) AS unique_authors,
        AVG(LENGTH(text)) AS avg_length
    FROM comments
    WHERE fetched_at BETWEEN ? AND ?
    """, (start_time, end_time))

    row = cur.fetchone()
    conn.close()

    return {
        "total_comments": row[0] or 0,
        "unique_authors": row[1] or 0,
        "avg_length": row[2] or 0
    }

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