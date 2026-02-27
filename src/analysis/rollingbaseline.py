from collections import deque
import statistics


class RollingBaseline:
    def __init__(self, max_windows=20, warmup=10):
        self.max_windows = max_windows
        self.warmup = warmup
        self.history = {
            "counts": deque(maxlen=max_windows),
            "authors": deque(maxlen=max_windows),
            "lengths": deque(maxlen=max_windows),
            "sentiments": deque(maxlen=max_windows),
            "concentration": deque(maxlen=max_windows),
            "sentiment_var": deque(maxlen=max_windows),
            # NEW: Timing deques
            "avg_gaps": deque(maxlen=max_windows),
            "gap_vars": deque(maxlen=max_windows),
        }

    def update(self, metrics):
        authors = max(metrics.get("unique_authors", 0), 1)
        self.history["counts"].append(metrics.get("total_comments", 0))
        self.history["authors"].append(authors)
        self.history["lengths"].append(metrics.get("avg_length", 0))
        self.history["sentiments"].append(metrics.get("avg_sentiment", 0))
        self.history["concentration"].append(metrics.get("total_comments", 0) / authors)
        self.history["sentiment_var"].append(metrics.get("sentiment_variance", 0))
        # NEW: Append timing metrics
        self.history["avg_gaps"].append(metrics.get("avg_gap", 0))
        self.history["gap_vars"].append(metrics.get("gap_variance", 0))

    @staticmethod
    def _safe_z(value, series):
        if len(series) < 2:
            return 0
        stdev = statistics.pstdev(series)
        if stdev == 0:
            return 0
        return (value - statistics.mean(series)) / stdev

    def evaluate(self, metrics):
        if len(self.history["counts"]) < self.warmup:
            return None

        total = metrics.get("total_comments", 0)
        authors = max(metrics.get("unique_authors", 1), 1)

        return {
            "count_z": self._safe_z(total, self.history["counts"]),
            "author_z": self._safe_z(authors, self.history["authors"]),
            "length_z": self._safe_z(metrics.get("avg_length", 0), self.history["lengths"]),
            "sentiment_z": self._safe_z(metrics.get("avg_sentiment", 0), self.history["sentiments"]),
            "concentration_z": self._safe_z(total / authors, self.history["concentration"]),
            "sentiment_var_z": self._safe_z(metrics.get("sentiment_variance", 0), self.history["sentiment_var"]),
            "gap_z": self._safe_z(metrics.get("avg_gap", 0), self.history["avg_gaps"]),
            "gap_var_z": self._safe_z(metrics.get("gap_variance", 0), self.history["gap_vars"]),
        }

    @staticmethod
    def coordination_score(z):
        if z is None:
            return None

        # Refined weights including the 'Robot' (gap_var) factor
        # A deep negative gap_var_z (timing becoming too perfect) heavily flags coordination
        return (
                abs(z["concentration_z"]) * 0.30 +
                abs(z["sentiment_var_z"]) * 0.20 +
                abs(z["gap_var_z"]) * 0.20 +
                abs(z["sentiment_z"]) * 0.15 +
                abs(z["count_z"]) * 0.10 +
                abs(z["length_z"]) * 0.05
        )
