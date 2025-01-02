from collections import defaultdict

from hivemind_etl_helpers.src.db.github.schema import GitHubCommit


class CommitAggregator:
    def __init__(self):
        self.daily_commits: dict[str, list[GitHubCommit]] = defaultdict(list)

    def add_commit(self, commit: GitHubCommit) -> None:
        """Add a single commit to the aggregator."""
        commit_dict = commit.to_dict()
        date_str = commit_dict["created_at"].split()[0]
        self.daily_commits[date_str].append(commit)

    def add_multiple_commits(self, commits: list[GitHubCommit]) -> None:
        """Add multiple commits at once."""
        for commit in commits:
            self.add_commit(commit)

    def get_daily_commits(self, date: str = None) -> dict[str, list[GitHubCommit]]:
        """Get commits for a specific date or all dates."""
        if date:
            return (
                {date: self.daily_commits[date]} if date in self.daily_commits else {}
            )
        return self.daily_commits
