from collections import defaultdict
from hivemind_etl_helpers.src.db.github.schema import GitHubComment


class CommentAggregator:
    def __init__(self):
        self.daily_comments: dict[str, list[GitHubComment]] = defaultdict(list)

    def add_comment(self, comment: GitHubComment) -> None:
        """
        Add a single comment to the aggregator.
        Parameters
        ----------
        comment : GitHubComment
            The comment to be added.
        """
        comment_dict = comment.to_dict()
        date_str = comment_dict["created_at"].split()[0]  # Get YYYY-MM-DD part
        self.daily_comments[date_str].append(comment)

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

    def get_daily_comments(self, date: str = None) -> dict[str, list[GitHubComment]]:
        """
        Get comments for a specific date or all dates.

        Parameters
        ----------
        date : str, optional
            The specific date to retrieve comments for, by default None.
        Returns
        -------
        daily_comments : dict[str, list[GitHubComment]]
            A dictionary where the key is the date and the value is a list of GitHubComment objects.
            If a date is provided and exists in daily_comments, returns comments for that date.
            Otherwise, returns all comments.
        """
        if date:
            return (
                {date: self.daily_comments[date]} if date in self.daily_comments else {}
            )
        return self.daily_comments
