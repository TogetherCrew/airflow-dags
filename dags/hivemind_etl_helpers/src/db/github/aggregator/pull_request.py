from collections import defaultdict

from hivemind_etl_helpers.src.db.github.schema import GitHubPullRequest


class PullRequestAggregator:
    def __init__(self):
        self.daily_prs: dict[str, list[GitHubPullRequest]] = defaultdict(list)

    def add_pr(self, pr: GitHubPullRequest) -> None:
        """Add a single pull request to the aggregator."""
        pr_dict = pr.to_dict()
        date_str = pr_dict["created_at"].split()[0]
        self.daily_prs[date_str].append(pr)

    def add_multiple_prs(self, prs: list[GitHubPullRequest]) -> None:
        """Add multiple pull requests at once."""
        for pr in prs:
            self.add_pr(pr)

    def get_daily_prs(self, date: str = None) -> dict[str, list[GitHubPullRequest]]:
        """Get pull requests for a specific date or all dates."""
        if date:
            return {date: self.daily_prs[date]} if date in self.daily_prs else {}
        return self.daily_prs
