def anomaly_score(deviations):
    """
    Compute simple anomaly score from deviation z-scores.

    deviations:
        {
            "count_z": ...,
            "author_z": ...,
            "length_z": ...
        }

    Returns:
        score between 0 and 100 (approx)
    """

    # absolute deviations (magnitude matters)
    count = abs(deviations.get("count_z", 0))
    authors = abs(deviations.get("author_z", 0))
    length = abs(deviations.get("length_z", 0))

    # weighted sum
    raw_score = (
        (count * 0.5) +
        (authors * 0.3) +
        (length * 0.2)
    )

    # scale to 0–100 (rough)
    score = min(raw_score * 10, 100)

    return score