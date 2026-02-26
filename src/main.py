
from datetime import datetime, timezone
from database import init_db, insert_comment, get_window_metrics, get_all_window_metrics, insert_window_metrics
from ingestion import fetch_all_comments, parse_comment
from config import YTAPI
from analysis.rollingbaseline import RollingBaseline
from analysis.statistics import deviation_scores
from scoring import anomaly_score
from analysis.sentiment import sentiment_pipeline, sentiment_score
import time

API_KEY = YTAPI
VIDEOS = ["1o3TFeagGO4"]

POLL_INTERVAL = 600  # 10 minutes

def main(test_mode=False):
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment")

    init_db()
    baseline = RollingBaseline()

    for v in VIDEOS:
        replay_historical(baseline, video_id=v)

    if test_mode:
        print("Test mode: replay only.")
        return

    last_window_start = datetime.now(timezone.utc)

    while True:

        window_start = last_window_start
        window_end = datetime.now(timezone.utc)

        # 1) fetch and store comments
        for video_id in VIDEOS:
            items = fetch_all_comments(API_KEY, video_id)

            comments = [parse_comment(item, video_id) for item in items]

            if comments:
                texts = [c["text"] for c in comments]

                results = sentiment_pipeline(texts, batch_size=32)

                for comment, result in zip(comments, results):
                    comment["sentiment"] = sentiment_score(result)

                    insert_comment(comment)


        # 2) compute metrics for this window
        metrics = get_window_metrics(
            window_start.isoformat(),
            window_end.isoformat()
        )
        baseline.update(metrics)
        z = baseline.evaluate(metrics)
        if z:
            score = baseline.coordination_score(z)
        else:
            score = None
        print("coordination score:", score)
        print(f"Window {window_start} → {window_end}")
        print(metrics)
        print("---")
        metrics["coordination_score"] = score
        insert_window_metrics({
            "video_id": video_id,
            "window": window_start.strftime('%Y-%m-%d %H:%M:%S'),
            "total_comments": metrics["total_comments"],
            "unique_authors": metrics["unique_authors"],
            "avg_length": metrics["avg_length"],
            "avg_sentiment": metrics["avg_sentiment"],
            "sentiment_variance": metrics["sentiment_variance"],
            "coordination_score": metrics["coordination_score"]
        })

        # 3) update window and sleep
        last_window_start = window_end
        if test_mode:
            break  # run only once
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

        baseline.update(w)
        z = baseline.evaluate(w)

        score = baseline.coordination_score(z) if z else None
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

    print("Historical replay complete.")


if __name__ == "__main__":
    main()