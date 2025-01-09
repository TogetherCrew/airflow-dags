from collections import defaultdict

from hivemind_etl_helpers.src.db.github.aggregator.utils import get_day_timestamp
from hivemind_etl_helpers.src.db.github.schema import GitHubPullRequest


class PullRequestAggregator:
    def __init__(self):
        self.daily_prs: dict[float, list[GitHubPullRequest]] = defaultdict(list)

    def add_pr(self, pr: GitHubPullRequest) -> None:
        """
        Add a single pull request to the aggregator.

        Parameters
        -------------
        pr : GitHubPullRequest
            The GitHubPullRequest object to be added.
        """
        date = get_day_timestamp(pr.created_at)
        self.daily_prs[date].append(pr)

    def add_multiple_prs(self, prs: list[GitHubPullRequest]) -> None:
        """
        Add multiple pull requests at once.

        Parameters
        ----------
        prs : list of GitHubPullRequest
            List of GitHubPullRequest objects to be added.
        """
        for pr in prs:
            self.add_pr(pr)

    def get_daily_prs(
        self, date: float | None = None
    ) -> dict[float, list[GitHubPullRequest]]:
        """
        Get pull requests for a specific date or all dates.

        Parameters
        ----------
        date : float, optional
            The date timestamp for which to retrieve prs
            If not provided, all prs are returned.

        Returns
        -------
        daily_prs : dict[float, list[GitHubPullRequest]]
            A dictionary where the key is the date
            and the value is a list of GitHubPullRequest objects for that date.
        """
        if date:
            return {date: self.daily_prs[date]} if date in self.daily_prs else {}
        return self.daily_prs
