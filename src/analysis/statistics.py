def z_score(value, mean, stdev):
    """
    Compute standard z-score.

    Returns 0 if stdev is zero (no variation).
    """
    if stdev == 0:
        return 0
    return (value - mean) / stdev


def deviation_scores(metrics, baseline_stats):
    """
    Compute z-scores for window metrics.

    metrics:
        {
            "total_comments": ...,
            "unique_authors": ...,
            "avg_length": ...
        }

    baseline_stats:
        {
            "mean_count": ...,
            "stdev_count": ...,
            "mean_authors": ...,
            "stdev_authors": ...,
            "mean_length": ...,
            "stdev_length": ...
        }

    Returns:
        dict of z-scores
    """
    return {
        "count_z": z_score(
            metrics["total_comments"],
            baseline_stats["mean_count"],
            baseline_stats["stdev_count"]
        ),
        "author_z": z_score(
            metrics["unique_authors"],
            baseline_stats["mean_authors"],
            baseline_stats["stdev_authors"]
        ),
        "length_z": z_score(
            metrics["avg_length"],
            baseline_stats["mean_length"],
            baseline_stats["stdev_length"]
        )
    }