from collections import defaultdict

from hivemind_etl_helpers.src.db.github.schema import GitHubIssue


class IssueAggregator:
    def __init__(self):
        self.daily_issues: dict[str, list[dict]] = defaultdict(list)

    def add_issue(self, issue: GitHubIssue) -> None:
        """Add a single issue to the aggregator."""
        issue_dict = issue.to_dict()
        date_str = issue_dict["created_at"].split()[0]
        self.daily_issues[date_str].append(issue_dict)

    def add_multiple_issues(self, issues: list[GitHubIssue]) -> None:
        """Add multiple issues at once."""
        for issue in issues:
            self.add_issue(issue)

    def get_daily_issues(self, date: str = None) -> dict[str, list[dict]]:
        """Get issues for a specific date or all dates."""
        if date:
            return {date: self.daily_issues[date]} if date in self.daily_issues else {}
        return dict(self.daily_issues)
