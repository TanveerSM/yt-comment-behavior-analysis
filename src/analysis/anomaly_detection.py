
from rollingbaseline import RollingBaseline

baseline = RollingBaseline(max_windows=20, warmup=10)
import numpy as np
scored_windows = []
chronological_windows = [...]
for window in chronological_windows:

    z = baseline.evaluate(window)

    if z:
        score = baseline.coordination_score(z)

        scored_windows.append({
            "window": window["window"],
            "score": score,
            "z": z
        })

    baseline.update(window)



scores = [w["score"] for w in scored_windows]
if len(scores) < 20:
    print("Not enough windows for reliable percentile detection")
threshold = np.percentile(scores, 95)
anomalies = [
    w for w in scored_windows
    if w["score"] >= threshold
]