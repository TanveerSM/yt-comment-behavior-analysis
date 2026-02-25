import os
import sqlite3

def perUserBehavioralSummary(videoid):

    DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    DB_PATH = os.path.abspath(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    SELECT
    author_id,
    COUNT(*) AS total_comments,
    AVG(sentiment) AS avg_sentiment,
    MIN(sentiment) AS min_sentiment,
    MAX(sentiment) AS max_sentiment,
    COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', published_at)) AS active_minutes
    FROM comments
    WHERE video_id = ?
    GROUP BY author_id
    ORDER BY total_comments DESC;
    """,(videoid,))

    rows = cur.fetchall()
    conn.close()

    return rows

import sqlite3
import os

def activityBurstPerUser(videoid):
    DB_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            author_id,
            text,  -- use your actual column name here
            COUNT(*) AS freq
        FROM comments
        WHERE video_id = ?
        GROUP BY author_id, text
        HAVING freq > 1;
    """, (videoid,))  # <-- tuple is required

    rows = cur.fetchall()
    conn.close()

    return rows

def activityBurstPerUser(videoid):
    DB_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "comments.db")
    )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT author_id,
       strftime('%Y-%m-%d %H:%M', published_at) as minute,
       COUNT(*) as count
    FROM comments
    WHERE video_id = ?
    GROUP BY author_id, minute
    HAVING count >= 3;
    """, (videoid,))  # <-- tuple is required

    rows = cur.fetchall()
    conn.close()

    return rows

# # activty burst per user per minute
# SELECT author_id,
#        strftime('%Y-%m-%d %H:%M', published_at) as minute,
#        COUNT(*) as count
# FROM comments
# WHERE video_id = ''
# GROUP BY author_id, minute
# HAVING count >= 3;



print(perUserBehavioralSummary("B4PmR-bPMFk"))
#

# To calibrate baseline
# SELECT
#     COUNT(*) AS comments_per_user
# FROM comments
# WHERE video_id = 'YOUR_VIDEO_ID'
# GROUP BY author_channel_id;

# Activity Burst Per User
# SELECT author_channel_id,
#        strftime('%Y-%m-%d %H:%M', published_at) as minute,
#        COUNT(*) as count
# FROM comments
# WHERE video_id = ?
# GROUP BY author_channel_id, minute
# HAVING count >= 3;

