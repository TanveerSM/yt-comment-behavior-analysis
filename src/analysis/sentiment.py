from transformers import pipeline, AutoTokenizer

my_tokenizer = AutoTokenizer.from_pretrained(
    "cardiffnlp/twitter-roberta-base-sentiment-latest",
    #token="hf_YOUR_TOKEN_HERE"
)

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    tokenizer=my_tokenizer,
    device=0,
    truncation=True,
    max_length=512,
    batch_size=32,
    use_safetensors=True,  # Forces the secure file format
    #token="hf_YOUR_TOKEN_HERE"
)

def sentiment_score(result):
    """
    Maps transformer output to a continuous [-1.0, 1.0] scale.
    Eliminates the discontinuity between -0.5 and +0.5.
    """
    label = result["label"]
    score = result["score"]  # Usually 0.5 to 1.0

    # Normalize to 0.0 (Negative) -> 1.0 (Positive)
    # If Model says "POSITIVE" with 0.9 conf -> val = 0.9
    # If Model says "NEGATIVE" with 0.9 conf -> val = 0.1
    val = score if label == "POSITIVE" else (1.0 - score)

    # Stretch to [-1, 1] range
    # 0.0 -> -1.0
    # 0.5 ->  0.0
    # 1.0 -> +1.0
    return (val - 0.5) * 2.0


