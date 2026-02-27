from src.database import get_connection

def detect_abnormal_patterns(z, metrics, video_id):
    """
    Uses Z-scores and raw metrics to identify 5 specific types of
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
    if z.get("gap_var_z", 0) < -1.5:
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

        # 1. Print the HIGH-LEVEL categories triggered
        for a in alerts:
            print(f"  {a}")

        # 2. Print TARGETED evidence based on the highest Z-score
        # If the biggest weirdness is concentration (spam), show the spammers
        if z.get("concentration_z", 0) > 2.5:
            print(f"\n  --- Forensic Evidence: Top Repeat Commenters ---")
            # Inside the alert loop
            spammers = get_spammer_context(video_id, window_time)
            for auth, count, concat_text in spammers:
                print(f"    User {auth[:8]} (Count: {count})")

                # Split the concatenated string back into individual comment samples
                individual_samples = concat_text.split(' | ')
                for i, sample in enumerate(individual_samples[:3]):  # Show first 3
                    print(f"      - {sample[:70]}...")


        # Otherwise, show the chronological timeline for timing/narrative alerts
        else:
            print(f"\n  --- Forensic Evidence: Window Timeline ---")
            samples = get_comments_for_context(video_id, window_time)
            for ts, auth, txt in samples:
                print(f"    [{ts}] {auth[:8]}: {txt[:80]}...")

        # 3. Print the RAW MATH for the technical screener
        print(f"\n  --- Technical Metrics ---")
        print(f"  Coordination Score: {metrics.get('coordination_score', 0):.2f}")
        print(
            f"  Z-Scores -> Count: {z['count_z']:.1f} | Gap_Var: {z['gap_var_z']:.1f} | Conc: {z['concentration_z']:.1f}")


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


def get_spammer_context(video_id, window_start, polling_rate=600, limit=5):
    """
    Finds authors who posted multiple times WITHIN the specific 10-minute window.
    """
    # Calculate the exact end of the 10-minute block
    import datetime
    start_dt = datetime.datetime.fromisoformat(window_start.replace("Z", "+00:00"))
    end_dt = start_dt + datetime.timedelta(seconds=polling_rate)
    window_end = end_dt.isoformat()

    conn = get_connection()
    cur = conn.cursor()

    # Logic Change: Use BETWEEN to lock the evidence to that specific window
    cur.execute("""
                SELECT author_id, COUNT(*) as comment_count, GROUP_CONCAT(text, ' | ')
                FROM comments
                WHERE video_id = ?
                  AND published_at BETWEEN ? AND ?
                GROUP BY author_id
                HAVING comment_count > 1
                ORDER BY comment_count DESC
                LIMIT ?
                """, (video_id, window_start, window_end, limit))

    rows = cur.fetchall()
    conn.close()
    return rows







