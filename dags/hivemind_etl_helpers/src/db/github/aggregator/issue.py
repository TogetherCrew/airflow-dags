from collections import defaultdict

from hivemind_etl_helpers.src.db.github.aggregator.utils import get_day_timestamp
from hivemind_etl_helpers.src.db.github.schema import GitHubIssue


class IssueAggregator:
    def __init__(self):
        # a dict with timestamp keys
        self.daily_issues: dict[float, list[GitHubIssue]] = defaultdict(list)

    def add_issue(self, issue: GitHubIssue) -> None:
        """
        Add a single issue to the aggregator.

        Parameters
        -------------
        issue : GitHubIssue
            The issue object to be added.
        """
        date = get_day_timestamp(issue.created_at)
        self.daily_issues[date].append(issue)

    def add_multiple_issues(self, issues: list[GitHubIssue]) -> None:
        """
        Add multiple issues at once.

        Parameters
        ----------
        issues : list of GitHubIssue
            List of GitHubIssue objects to be added.
        """
        for issue in issues:
            self.add_issue(issue)

    def get_daily_issues(
        self, date: float | None = None
    ) -> dict[float, list[GitHubIssue]]:
        """
        Get issues for a specific date or all dates.

        Parameters
        ----------
        date : float, optional
            The date timestamp for which to retrieve issues
            If not provided, all issues are returned.

        Returns
        -------
        daily_issues : dict[float, list[GitHubIssue]]
            A dictionary where the key is the date
            and the value is a list of GitHubIssue objects for that date.
        """
        if date:
            return {date: self.daily_issues[date]} if date in self.daily_issues else {}
        return self.daily_issues
