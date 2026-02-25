import statistics


class BaselineModel:
    def __init__(self):
        self.counts = []
        self.authors = []
        self.lengths = []

    def update(self, metrics):
        """
        Add window metrics to baseline history.
        metrics expects:
        {
            "total_comments": ...,
            "unique_authors": ...,
            "avg_length": ...
        }
        """
        self.counts.append(metrics["total_comments"])
        self.authors.append(metrics["unique_authors"])
        self.lengths.append(metrics["avg_length"])

    def stats(self):
        """
        Return baseline summary statistics.
        """
        if not self.counts:
            return {
                "mean_count": 0,
                "stdev_count": 0,
                "mean_authors": 0,
                "stdev_authors": 0,
                "mean_length": 0,
                "stdev_length": 0
            }

        return {
            "mean_count": statistics.mean(self.counts),
            "stdev_count": statistics.pstdev(self.counts),

            "mean_authors": statistics.mean(self.authors),
            "stdev_authors": statistics.pstdev(self.authors),

            "mean_length": statistics.mean(self.lengths),
            "stdev_length": statistics.pstdev(self.lengths)
        }
