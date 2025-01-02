from collections import defaultdict
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


class CommentAggregator:
    def __init__(self):
        self.daily_comments: dict[str, list[GitHubComment]] = defaultdict(list)

    def add_comment(self, comment: GitHubComment) -> None:
        """Add a single comment to the aggregator."""
        comment_dict = comment.to_dict()
        date_str = comment_dict["created_at"].split()[0]  # Get YYYY-MM-DD part
        self.daily_comments[date_str].append(comment)

    def add_multiple_comments(self, comments: list[GitHubComment]) -> None:
        """Add multiple comments at once."""
        for comment in comments:
            self.add_comment(comment)

    def get_daily_comments(self, date: str = None) -> dict[str, list[GitHubComment]]:
        """Get comments for a specific date or all dates."""
        if date:
            return (
                {date: self.daily_comments[date]} if date in self.daily_comments else {}
            )
        return self.daily_comments
