from datetime import datetime, timezone
from database import init_db, insert_comment, get_window_metrics, get_all_window_metrics, insert_window_metrics
from ingestion import fetch_all_comments, parse_comment
from config import YTAPI
from analysis.rollingbaseline import RollingBaseline
from analysis.sentiment import sentiment_pipeline, sentiment_score
from analysis.abnormal_patterns import detect_abnormal_patterns
import time

API_KEY = YTAPI
VIDEOS = ["SKUJHX5o0j4"]

POLL_INTERVAL = 600  # 10 minutes


def main(test_mode=False):
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment")

    init_db()
    baseline = RollingBaseline()
    latest_ids = {}  # Initialize empty dictionary

    # --- STEP 1: INITIAL HISTORICAL POPULATION ---
    print("Performing initial historical fetch and replay...")
    for v in VIDEOS:
        # 1. Fetch EVERYTHING
        items = fetch_all_comments(API_KEY, v)

        if items:
            # Save the NEWEST ID now so the while-loop doesn't fetch history again
            latest_ids[v] = items[0]['id']

            # 2. Process and save
            process_and_save_comments(items, v)

        # 3. Replay (Must return MULTIPLE windows to work correctly)
        replay_historical(baseline, video_id=v)

    if test_mode:
        return

    last_window_start = datetime.now(timezone.utc)

    # --- STEP 2: LIVE MONITORING LOOP ---
    while True:
        # We use a fixed window end so all videos in this "round" share the same timeframe
        window_end = datetime.now(timezone.utc)

        for video_id in VIDEOS:
            # Use the ID we caught during the historical fetch (or previous loop)
            stop_id = latest_ids.get(video_id)
            items = fetch_all_comments(API_KEY, video_id, stop_at_id=stop_id)

            if items:
                latest_ids[video_id] = items[0]['id']
                process_and_save_comments(items, video_id)

            # Compute metrics for the specific time since the last loop
            metrics = get_window_metrics(
                last_window_start.isoformat(),
                window_end.isoformat(),
                video_id=video_id  # IMPORTANT: ensure this function filters by video!
            )

            # Only evaluate and update if there's actually data in this window
            # (prevents polluting baseline with empty '0' windows if video is quiet)
            if metrics["total_comments"] > 0:
                z = baseline.evaluate(metrics)
                if z:
                    detect_abnormal_patterns(z, video_id, metrics)


                    score = baseline.coordination_score(z)
                else:
                    score = None

                metrics["coordination_score"] = score
                insert_window_metrics({
                    "video_id": video_id,
                    "window": last_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                    "total_comments": metrics["total_comments"],
                    "unique_authors": metrics["unique_authors"],
                    "avg_length": metrics.get("avg_length", 0),
                    "avg_sentiment": metrics.get("avg_sentiment", 0),
                    "sentiment_variance": metrics.get("sentiment_variance", 0),
                    "coordination_score": score
                })
                baseline.update(metrics)

        last_window_start = window_end  # Move the window forward
        if test_mode: break
        time.sleep(POLL_INTERVAL)


def replay_historical(baseline, video_id=None):
    """
    Reprocess historical comments into window metrics
    and populate the rolling baseline.

    This analyzes data window-by-window so the baseline
    reflects historical behavior before live data.
    """
    print("Starting historical replay...")

    windows = get_all_window_metrics(video_id,POLL_INTERVAL)

    if not windows:
        print("No historical windows found.")
        return

    for w in windows:
        # w contains:
        # video_id, window, total_comments, unique_authors,
        # avg_length, avg_sentiment, sentiment_variance


        z = baseline.evaluate(w)

        if z:
            # --- TEST THE ALERTS AGAINST HISTORY ---
            # This will print alerts for past events as the script 're-lives' them
            detect_abnormal_patterns(z, video_id, w)
            # ----------------------------------------

            score = baseline.coordination_score(z)
        else:
            score = None

        print(f"Replayed window {w['window']}: total_comments={w['total_comments']} unique_authors={w['unique_authors']} score={score}")
        w["coordination_score"] = score
        insert_window_metrics({
            "video_id": w["video_id"],
            "window": w["window"],
            "total_comments": w["total_comments"],
            "unique_authors": w["unique_authors"],
            "avg_length": w["avg_length"],
            "avg_sentiment": w["avg_sentiment"],
            "sentiment_variance": w["sentiment_variance"],
            "coordination_score": score
        })
        baseline.update(w)

    print("Historical replay complete.")


def process_and_save_comments(items, video_id):
    """Parses, analyzes, and saves a batch of comments."""
    # 1. Parse raw API items into your comment dicts
    comments = [parse_comment(item, video_id) for item in items]

    if not comments:
        return []

    # 2. Batch-process sentiment using your GPU pipeline
    texts = [c["text"] for c in comments]
    results = sentiment_pipeline(texts, batch_size=32, truncation=True, max_length=512)

    for comment, result in zip(comments, results):
        # 3. Add the score to the dict and save to DB
        comment["sentiment"] = sentiment_score(result)
        insert_comment(comment)

    return comments


if __name__ == "__main__":
    main()