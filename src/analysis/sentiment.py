# sentiment.py

from transformers import pipeline
import torch

# Load once globally (important)
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    device=0 if torch.cuda.is_available() else -1,
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

