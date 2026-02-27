from transformers import pipeline, AutoTokenizer

my_tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    tokenizer=my_tokenizer,
    device=0
)  # type: ignore


def sentiment_score(result):
    """
    Convert transformer result to numeric sentiment.
    Label POSITIVE -> positive score
    Label NEGATIVE -> negative score
    """
    label = result["label"]
    score = result["score"]
    return score if label == "POSITIVE" else -score

