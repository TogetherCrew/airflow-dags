from collections import defaultdict
from datetime import datetime

from hivemind_etl_helpers.src.db.github.schema import GitHubCommit


class CommitAggregator:
    def __init__(self):
        self.daily_commits: dict[float, list[GitHubCommit]] = defaultdict(list)

    def add_commit(self, commit: GitHubCommit) -> None:
        """
        Add a single commit to the aggregator.

        Parameters
        -------------
        commit : GitHubCommit
            The commit object to be added.
        """
        date = datetime.fromtimestamp(commit.created_at).date()
        self.daily_commits[
            datetime.combine(date, datetime.min.time()).timestamp()
        ].append(commit)

    def add_multiple_commits(self, commits: list[GitHubCommit]) -> None:
        """
        Add multiple commits at once.

        Parameters
        ----------
        commits : list of GitHubCommit
            List of GitHubCommit objects to be added.
        """
        for commit in commits:
            self.add_commit(commit)

    def get_daily_commits(self, date: float = None) -> dict[float, list[GitHubCommit]]:
        """
        Get commits for a specific date or all dates.

        Parameters
        ----------
        date : float, optional
            The date timestamp for which to retrieve commits
            If not provided, all commits are returned.
        Returns
        -------
        daily_commits : dict[float, list[GitHubCommit]]
            A dictionary where the key is the date
            and the value is a list of GitHubCommit objects for that date.
        """
        if date:
            return (
                {date: self.daily_commits[date]} if date in self.daily_commits else {}
            )
        return self.daily_commits
