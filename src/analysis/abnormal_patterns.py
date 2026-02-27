def detect_abnormal_patterns(z, video_id, metrics):
    """
    Analyzes Z-scores to identify specific types of coordinated activity.
    A Z-score > 2.0 means the activity is in the top 2.5% of 'unusual'.
    A Z-score > 3.0 is a massive outlier.
    """
    if not z or not metrics:
        return

    total = metrics.get("total_comments", 0)
    if total < 5:
        return
    alerts = []

    # 1. THE "SCRIPTED NARRATIVE" (High Sentiment Shift + Low Variance)
    # If sentiment is way up/down but everyone is saying the same thing (low variance)
    if abs(z["sentiment_z"]) > 2.0 and z["sentiment_var_z"] < -1.0:
        alerts.append("🚩 ALERT: Scripted Narrative detected. Uniform sentiment with low diversity.")

    # 2. THE "BOT FLOOD" (High Volume + Low Unique Authors)
    # If the number of comments spikes way faster than the number of people
    if z["count_z"] > 2.0 and z["author_z"] < 1.0:
        alerts.append("🤖 ALERT: Potential Bot Flood. High volume from a small group of accounts.")

    # 3. THE "DOGPILE / BRIGADE" (High Volume + High Author Count)
    if z["count_z"] > 3.0 and z["author_z"] > 3.0:
        alerts.append("🔥 ALERT: Massive Organic Spike or External Brigade.")

    # 4. THE "REPETITIVE ATTACK" (High Concentration)
    if z["concentration_z"] > 2.5:
        alerts.append("🔁 ALERT: High Interaction Density. Users are posting multiple times rapidly.")

    # Print the findings
    if alerts:
        print(f"\n[ANALYSIS REPORT - {video_id}]")
        for a in alerts:
            print(a)
        print(f"Metrics: Count_Z: {z['count_z']:.2f}, Var_Z: {z['sentiment_var_z']:.2f}\n")
