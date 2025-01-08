from collections import defaultdict

from hivemind_etl_helpers.src.db.github.schema import GitHubIssue


class IssueAggregator:
    def __init__(self):
        self.daily_issues: dict[str, list[GitHubIssue]] = defaultdict(list)

    def add_issue(self, issue: GitHubIssue) -> None:
        """
        Add a single issue to the aggregator.

        Parameters
        -------------
        issue : GitHubIssue
            The issue object to be added.
        """
        issue_dict = issue.to_dict()
        date_str = issue_dict["created_at"].split()[0]
        self.daily_issues[date_str].append(issue)

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

    def get_daily_issues(self, date: str = None) -> dict[str, list[GitHubIssue]]:
        """
        Get issues for a specific date or all dates.

        Parameters
        ----------
        date : str, optional
            The date for which to retrieve commits in 'YYYY-MM-DD' format.
            If not provided, all commits are returned.
        Returns
        -------
        daily_issues : dict[str, list[GitHubIssue]]
            A dictionary where the key is the date
            and the value is a list of GitHubIssue objects for that date.
        """
        if date:
            return {date: self.daily_issues[date]} if date in self.daily_issues else {}
        return self.daily_issues
