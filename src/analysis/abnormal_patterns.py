from src.database import get_connection


def detect_abnormal_patterns(z, metrics, video_id):
    """
    Uses Z-scores and raw metrics to identify 4 specific types of
    coordinated or robotic behavior.
    """
    if not z or not metrics:
        return

    total = metrics.get("total_comments", 0)
    window_time = metrics.get("window", "Unknown Time")

    # 1. VOLUME GUARD
    # We ignore windows with very few comments because Z-scores
    # fluctuate too wildly on tiny samples.
    if total < 5:
        return

    alerts = []

    # 2. PATTERN: THE "METRONOME" (Robotic Timing)
    # If gap_var_z is a deep negative (e.g., -2.0), it means the
    # timing has become unnaturally consistent compared to history.
    if z.get("gap_var_z", 0) < -1.5 and metrics.get("gap_variance", 99) < 0.5:
        alerts.append("🤖 Rhythmic Pulse: Timing is significantly more consistent than usual.")

    # 3. PATTERN: THE "SCRIPTED NARRATIVE" (Coordinated Opinion)
    # High sentiment shift + Low sentiment diversity
    if abs(z["sentiment_z"]) > 2.0 and z["sentiment_var_z"] < -1.0:
        alerts.append("🚩 Scripted Narrative: Uniform sentiment with unusually low diversity.")

    # 4. PATTERN: THE "BOT FLOOD" (Volume vs. People)
    # High comment count spike + Low unique author spike
    if z["count_z"] > 2.0 and z["author_z"] < 1.0:
        alerts.append("👥 Bot Flood: Massive volume spike from a suspiciously small group.")

    # 5. PATTERN: THE "RAPID REPETITION" (Spamming)
    if z["concentration_z"] > 2.5:
        alerts.append("🔁 Interaction Density: Individual users are spamming multiple times.")

    # OUTPUT SECTION
    if alerts:
        print(f"\n[ALERT - {video_id}] @ {window_time}")
        for a in alerts:
            print(f"  {a}")

        # Display the stats that triggered the alert for quick verification
        print(f"  Stats -> Score: {metrics.get('coordination_score', 0):.2f} | "
              f"Gap_Var_Z: {z.get('gap_var_z', 0):.2f} | "
              f"Sent_Var_Z: {z['sentiment_var_z']:.2f}")


def get_comments_for_context(video_id, window_time, limit=10):
    """Fetches the first few comments from a window to show in the alert."""

    conn = get_connection()
    cur = conn.cursor()

    # We want the exact time and author to spot 'bursts'
    cur.execute("""
        SELECT published_at, author_id, text 
        FROM comments 
        WHERE video_id = ? AND published_at >= ? 
        ORDER BY published_at ASC 
        LIMIT ?
    """, (video_id, window_time, limit))

    rows = cur.fetchall()
    conn.close()
    return rows





