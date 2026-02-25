# sentiment.py

from transformers import pipeline
from src.database import fetch_comments_by_video, update_comment_sentiment

# Load once globally (important)
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    device=0,
    truncation=True,
    max_length=512
)

def sentiment_score(result):
    """
    Convert transformer result to numeric sentiment.
    Label POSITIVE -> positive score
    Label NEGATIVE -> negative score
    """
    label = result["label"]
    score = result["score"]
    return score if label == "POSITIVE" else -score


def add_sentiment_to_video(video_id):
    comments = fetch_comments_by_video(video_id)

    if not comments:
        print("no comments found for video")
        return

    # batch texts
    texts = [c["text"] for c in comments]

    # GPU batch inference
    results = sentiment_pipeline(texts, batch_size=32)

    # update DB with results
    for comment, result in zip(comments, results):
        sentiment = sentiment_score(result)
        update_comment_sentiment(comment["comment_id"], sentiment)

    print(f"updated sentiment for {len(comments)} comments")