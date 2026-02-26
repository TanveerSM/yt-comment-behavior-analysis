
from datetime import datetime, timezone
from database import init_db, insert_comment, get_window_metrics
from ingestion import fetch_all_comments, parse_comment
from config import YTAPI
from analysis.rollingbaseline import RollingBaseline
from analysis.statistics import deviation_scores
from scoring import anomaly_score
import time

API_KEY = YTAPI
VIDEOS = ["7swlkU_JfN4"]

POLL_INTERVAL = 600  # 10 minutes

def main(test_mode=False):
    if not API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment")

    init_db()
    last_window_start = datetime.now(timezone.utc)
    baseline = RollingBaseline()

    while True:

        window_start = last_window_start
        window_end = datetime.now(timezone.utc)

        # 1) fetch and store comments
        for video_id in VIDEOS:
            items = fetch_all_comments(API_KEY, video_id)

            for item in items:
                comment = parse_comment(item, video_id)
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

        # 3) update window and sleep
        last_window_start = window_end
        if test_mode:
            break  # run only once
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()