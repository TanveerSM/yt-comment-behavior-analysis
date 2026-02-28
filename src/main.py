from datetime import datetime, timezone
from database import init_db, insert_comments_batch, get_window_metrics, get_all_window_metrics, insert_window_metrics
from ingestion import fetch_all_comments, parse_comment
from config import YTAPI, POLL_INTERVAL
from analysis.rollingbaseline import RollingBaseline
from analysis.sentiment import sentiment_pipeline, sentiment_score
from analysis.abnormal_patterns import detect_abnormal_patterns
import time

API_KEY = YTAPI
VIDEOS = ["VgsC_aBquUE"]

def main(test_mode=False):
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment")

    init_db()
    baselines = {v: RollingBaseline() for v in VIDEOS}
    latest_ids = {}

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
        replay_historical(baselines[v], video_id=v)

    if test_mode:
        return

    last_window_start = datetime.now(timezone.utc)

    # --- STEP 2: LIVE MONITORING LOOP ---
    try:
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
                    z = baselines[video_id].evaluate(metrics)
                    if z:
                        detect_abnormal_patterns(z, metrics, video_id)


                        score = baselines[video_id].coordination_score(z)
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
                        "avg_gap": metrics.get("avg_gap", 0),
                        "gap_variance": metrics.get("gap_variance", 0),
                        "coordination_score": score
                    })

                    baselines[video_id].update(metrics)

            last_window_start = window_end  # Move the window forward
            if test_mode: break
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nShutting down live monitoring cleanly...")
    finally:
        # If you have any final cleanup, it goes here
        pass


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
        # 1. INITIALIZE SCORE (Prevents the UnboundLocalError)
        score = 0.0

        z = baseline.evaluate(w)

        if z:
            # 2. CALCULATE SCORE
            score = baseline.coordination_score(z)

            # 3. RUN ALERTS (Pass the score to the metrics dict so the alert can see it)
            w["coordination_score"] = score
            detect_abnormal_patterns(z, w, video_id)

        # 4. SAVE & UPDATE (Score is now guaranteed to be at least 0.0)
        w["coordination_score"] = score
        insert_window_metrics(w)
        baseline.update(w)

    print("Historical replay complete.")


def process_and_save_comments(items, video_id):
    # 1. Parse API items
    comments = [parse_comment(item, video_id) for item in items]
    comments = [c for c in comments if c is not None]

    if not comments:
        return []

    # 2. Batch-process sentiment
    valid_comments = [c for c in comments if c["text"].strip()]
    if valid_comments:
        texts = [c["text"] for c in valid_comments]
        results = sentiment_pipeline(texts, batch_size=32, truncation=True, max_length=512)

        for comment, result in zip(valid_comments, results):
            comment["sentiment"] = sentiment_score(result)

    # 3. Ensure every comment has a sentiment key before DB insert, even those that were invalid
    for comment in comments:
        comment.setdefault("sentiment", 0.0)

    # 4. ONE database trip for the entire batch (Way faster!)
    insert_comments_batch(comments)

    return comments


if __name__ == "__main__":
    main()