from collections import defaultdict

from hivemind_etl_helpers.src.db.github.aggregator.utils import get_day_timestamp
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


class CommentAggregator:
    def __init__(self):
        self.daily_comments: dict[float, list[GitHubComment]] = defaultdict(list)

    def add_comment(self, comment: GitHubComment) -> None:
        """
        Add a single comment to the aggregator.
        Parameters
        ----------
        comment : GitHubComment
            The comment to be added.
        """
        date = get_day_timestamp(comment.created_at)
        self.daily_comments[date].append(comment)

    def add_multiple_comments(self, comments: list[GitHubComment]) -> None:
        """
        Add multiple comments at once.

        Parameters
        ----------
        comments : list of GitHubComment
            A list of GitHubComment objects to be added.
        """
        for comment in comments:
            self.add_comment(comment)

    def get_daily_comments(
        self, date: float | None = None
    ) -> dict[float, list[GitHubComment]]:
        """
        Get comments for a specific date or all dates.

        Parameters
        ----------
        date : float, optional
            The specific date timestamp to retrieve comments for, by default None.

        Returns
        -------
        daily_comments : dict[float, list[GitHubComment]]
            A dictionary where the key is the date and the value is a list of GitHubComment objects.
            If a date is provided and exists in daily_comments, returns comments for that date.
            Otherwise, returns all comments.
        """
        if date:
            return (
                {date: self.daily_comments[date]} if date in self.daily_comments else {}
            )
        return self.daily_comments
