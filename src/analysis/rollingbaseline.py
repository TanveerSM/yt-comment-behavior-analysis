from collections import deque
import statistics


class RollingBaseline:
    def __init__(self, max_windows=20, warmup=10):
        self.max_windows = max_windows
        self.warmup = warmup
        # Using a dictionary to manage deque dynamically is cleaner
        self.history = {
            "counts": deque(maxlen=max_windows),
            "authors": deque(maxlen=max_windows),
            "lengths": deque(maxlen=max_windows),
            "sentiments": deque(maxlen=max_windows),
            "concentration": deque(maxlen=max_windows),
            "sentiment_var": deque(maxlen=max_windows),
        }

    def update(self, metrics):
        authors = max(metrics.get("unique_authors", 0), 1)
        # Use .get(key, 0) to ensure we never append None to the deques
        self.history["counts"].append(metrics.get("total_comments", 0))
        self.history["authors"].append(authors)
        self.history["lengths"].append(metrics.get("avg_length", 0))
        self.history["sentiments"].append(metrics.get("avg_sentiment", 0))
        self.history["concentration"].append(metrics.get("total_comments", 0) / authors)
        self.history["sentiment_var"].append(metrics.get("sentiment_variance", 0))

    @staticmethod # Fixes the PyCharm warning
    def _safe_z(value, series):
        if len(series) < 2:
            return 0
        stdev = statistics.pstdev(series)
        if stdev == 0:
            return 0
        return (value - statistics.mean(series)) / stdev

    def evaluate(self, metrics):
        # Use history['counts'] to check warmup
        if len(self.history["counts"]) < self.warmup:
            return None

        total = metrics["total_comments"]
        authors = max(metrics["unique_authors"], 1)

        return {
            "count_z": self._safe_z(total, self.history["counts"]),
            "author_z": self._safe_z(authors, self.history["authors"]),
            "length_z": self._safe_z(metrics["avg_length"], self.history["lengths"]),
            "sentiment_z": self._safe_z(metrics["avg_sentiment"], self.history["sentiments"]),
            "concentration_z": self._safe_z(total / authors, self.history["concentration"]),
            "sentiment_var_z": self._safe_z(metrics["sentiment_variance"], self.history["sentiment_var"]),
        }

    @staticmethod
    def coordination_score(z):
        if z is None:
            return None
        return (
                abs(z["concentration_z"]) * 0.35 +
                abs(z["sentiment_z"]) * 0.25 +
                abs(z["sentiment_var_z"]) * 0.20 +
                abs(z["count_z"]) * 0.15 +
                abs(z["length_z"]) * 0.05
        )