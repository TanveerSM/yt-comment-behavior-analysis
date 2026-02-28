from sentence_transformers import SentenceTransformer, util
import torch
from sklearn.feature_extraction.text import TfidfVectorizer

# This model is tiny (~80MB) and optimized for exactly this task
sim_model = SentenceTransformer('all-MiniLM-L6-v2', device='cuda:0')


def calculate_window_similarity(texts):
    """
    Takes a list of strings and calculates the average linguistic
    similarity across the entire group.
    Returns a float between 0.0 (completely different) and 1.0 (identical).
    """
    # If there's 0 or 1 comment, there's nothing to compare
    if len(texts) < 2:
        return 0.0

    # 1. Convert texts to 384-dimensional mathematical vectors
    embeddings = sim_model.encode(texts, convert_to_tensor=True)

    # 2. Compute the cosine similarity matrix for all pairs
    cosine_scores = util.cos_sim(embeddings, embeddings)

    # 3. We only want the upper triangle of the matrix, excluding the diagonal
    # (because comparing Comment A to Comment A is always 100% identical, which skews the math)
    mask = torch.triu(torch.ones_like(cosine_scores), diagonal=1) == 1

    # Calculate the average similarity of all unique pairs
    avg_sim = cosine_scores[mask].mean().item()

    return round(avg_sim, 4)



def extract_top_keywords(texts, top_n=3):
    """
    Extracts the most defining keywords from a batch of text,
    filtering out common English stop words.
    """
    if len(texts) < 2:
        return []

    # Initialize TF-IDF, automatically filtering out 'the', 'and', 'is', etc.
    vectorizer = TfidfVectorizer(stop_words='english', max_df=0.95)

    try:
        tfidf_matrix = vectorizer.fit_transform(texts)

        # Sum the TF-IDF scores for each word across all comments in this window
        summed_tfidf = tfidf_matrix.sum(axis=0)

        # Pair each word with its total score
        words_freq = [(word, summed_tfidf[0, idx]) for word, idx in vectorizer.vocabulary_.items()]

        # Sort by score (highest first)
        words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)

        # Return just the top N words
        return [word[0] for word in words_freq[:top_n]]

    except ValueError:
        # This triggers if the comments contained absolutely no usable words
        # (e.g., they just spammed emojis or punctuation)
        return []