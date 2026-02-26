from collections import deque
import statistics


class RollingBaseline:
    def __init__(self, max_windows=20, warmup=10):
        self.max_windows = max_windows
        self.warmup = warmup

        self.counts = deque(maxlen=max_windows)
        self.authors = deque(maxlen=max_windows)
        self.lengths = deque(maxlen=max_windows)
        self.sentiments = deque(maxlen=max_windows)
        self.concentration = deque(maxlen=max_windows)
        self.sentiment_var = deque(maxlen=max_windows)

    def update(self, metrics):
        total = metrics["total_comments"]
        authors = max(metrics["unique_authors"], 1)
        avg_length = metrics["avg_length"]
        avg_sentiment = metrics["avg_sentiment"]
        sentiment_variance = metrics["sentiment_variance"]

        self.counts.append(total)
        self.authors.append(authors)
        self.lengths.append(avg_length)
        self.sentiments.append(avg_sentiment)
        self.concentration.append(total / authors)
        self.sentiment_var.append(sentiment_variance)

    def _safe_z(self, value, series):
        if len(series) < 2:
            return 0

        mean = statistics.mean(series)
        stdev = statistics.pstdev(series)

        if stdev == 0:
            return 0

        return (value - mean) / stdev

    def evaluate(self, metrics):
        if len(self.counts) < self.warmup:
            return None  # not enough history yet

        total = metrics["total_comments"]
        authors = max(metrics["unique_authors"], 1)
        avg_length = metrics["avg_length"]
        avg_sentiment = metrics["avg_sentiment"]
        sentiment_variance = metrics["sentiment_variance"]

        return {
            "count_z": self._safe_z(total, self.counts),
            "author_z": self._safe_z(authors, self.authors),
            "length_z": self._safe_z(avg_length, self.lengths),
            "sentiment_z": self._safe_z(avg_sentiment, self.sentiments),
            "concentration_z": self._safe_z(total / authors, self.concentration),
            "sentiment_var_z": self._safe_z(sentiment_variance, self.sentiment_var),
        }

    def coordination_score(self, z):
        if z is None:
            return None

        return (
            abs(z["concentration_z"]) * 0.35 +
            abs(z["sentiment_z"]) * 0.25 +
            abs(z["sentiment_var_z"]) * 0.20 +
            abs(z["count_z"]) * 0.15 +
            abs(z["length_z"]) * 0.05
        )