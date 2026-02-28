from collections import deque
import statistics
from config import WEIGHTS, NOISE_FLOOR, ROBOTIC_PENALTY_MULTIPLIER, ROBOTIC_THRESHOLD, MAX_WINDOWS, WARMUP_PERIOD


class RollingBaseline:
    def __init__(self, max_windows=MAX_WINDOWS, warmup=WARMUP_PERIOD):
        """
        Initializes the sliding window memory using values from config.py.
        """
        self.max_windows = max_windows
        self.warmup = warmup

        # Using a dictionary of deques to track the rolling state of each metric
        self.history = {
            "counts": deque(maxlen=self.max_windows),
            "authors": deque(maxlen=self.max_windows),
            "lengths": deque(maxlen=self.max_windows),
            "sentiments": deque(maxlen=self.max_windows),
            "concentration": deque(maxlen=self.max_windows),
            "sentiment_var": deque(maxlen=self.max_windows),
            "avg_gaps": deque(maxlen=self.max_windows),
            "gap_vars": deque(maxlen=self.max_windows),
        }

    def update(self, metrics):
        authors = max(metrics.get("unique_authors", 0), 1)
        self.history["counts"].append(metrics.get("total_comments", 0))
        self.history["authors"].append(authors)
        self.history["lengths"].append(metrics.get("avg_length", 0))
        self.history["sentiments"].append(metrics.get("avg_sentiment", 0))
        self.history["concentration"].append(metrics.get("total_comments", 0) / authors)
        self.history["sentiment_var"].append(metrics.get("sentiment_variance", 0))
        self.history["avg_gaps"].append(metrics.get("avg_gap", 0))
        self.history["gap_vars"].append(metrics.get("gap_variance", 0))

    @staticmethod
    def _safe_z(value, series, noise_floor=0.01):
        if len(series) < 3: return 0

        median = statistics.median(series)
        deviations = [abs(x - median) for x in series]
        mad = statistics.median(deviations)
        consistent_mad = mad * 1.4826

        # --- THE FIX ---
        # If the historical variance is unreasonably tiny, force it to the noise_floor.
        # This prevents normal human micro-fluctuations from exploding into massive Z-scores.
        if consistent_mad < noise_floor:
            consistent_mad = noise_floor

        raw_z = (value - median) / consistent_mad

        # Cap to prevent composite score blowout
        return max(-20.0, min(20.0, raw_z))


    def evaluate(self, metrics):
        if len(self.history["counts"]) < self.warmup:
            return None

        total = metrics.get("total_comments", 0)
        authors = max(metrics.get("unique_authors", 1), 1)

        return {
            "count_z": self._safe_z(total, self.history["counts"], noise_floor=2.0),
            "author_z": self._safe_z(authors, self.history["authors"], noise_floor=2.0),
            "length_z": self._safe_z(metrics.get("avg_length", 0), self.history["lengths"], noise_floor=10.0),
            "sentiment_z": self._safe_z(metrics.get("avg_sentiment", 0), self.history["sentiments"], noise_floor=0.1),

            # A floor of 0.15 means the ratio has to jump to at least 1.38 before
            # hitting a Z-score of 2.5 (the trigger for your alert).
            "concentration_z": self._safe_z(total / authors, self.history["concentration"], noise_floor=0.15),

            "sentiment_var_z": self._safe_z(metrics.get("sentiment_variance", 0), self.history["sentiment_var"],
                                            noise_floor=0.05),
            "gap_z": self._safe_z(metrics.get("avg_gap", 0), self.history["avg_gaps"], noise_floor=5.0),
            "gap_var_z": self._safe_z(metrics.get("gap_variance", 0), self.history["gap_vars"], noise_floor=10.0),
        }

    @staticmethod
    def _dampen(val):
        """Reduces the impact of 'normal' fluctuations (Z < NOISE_FLOOR)."""
        abs_val = abs(val)
        return abs_val if abs_val > NOISE_FLOOR else (abs_val * 0.1)

    def coordination_score(self, z):
        if z is None:
            return None

        # 1. Process Gap Variance (The 'Robot' vs 'Chaos' logic)
        gap_z = z["gap_var_z"]
        gap_signal = self._dampen(gap_z)

        # Apply the "Robotic Bias": boost only if it's significantly negative
        if gap_z < ROBOTIC_THRESHOLD:
            gap_signal *= ROBOTIC_PENALTY_MULTIPLIER

        # 2. Calculate Weighted Composite Score
        # Using .get() ensures the code doesn't crash if a weight is missing
        score = (
                self._dampen(z["concentration_z"]) * WEIGHTS.get("concentration", 0.4) +
                gap_signal * WEIGHTS.get("gap_variance", 0.3) +
                self._dampen(z["sentiment_var_z"]) * WEIGHTS.get("sentiment_var", 0.2) +
                self._dampen(z["count_z"]) * WEIGHTS.get("count", 0.1)
        )

        return round(score, 4)

