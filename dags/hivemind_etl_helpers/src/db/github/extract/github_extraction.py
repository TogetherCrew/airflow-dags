from datetime import datetime

from hivemind_etl_helpers.src.db.github.extract import GithubIssueExtraction
from hivemind_etl_helpers.src.db.github.schema import GitHubIssue

class GithubExtraction:
    def __init__(self):
        # to be uncommented once other pull requests
        # regarding `extraction` are ready
        # self.commits_extraction = GithubCommitExtraction()
        # self.pull_requests_extraction = GithubPullRequestsExtraction()
        # self.comment_extraction = GitHubCommentExtraction()
        self.issue_extraction = GithubIssueExtraction()

    def fetch_issues(
            self,
            repository_id: list[int],
            from_date: datetime | None = None,
            **kwargs
    ) -> list[GitHubIssue]:
        return self.issue_extraction.fetch_issues(
            repository_id, from_date, **kwargs
        )