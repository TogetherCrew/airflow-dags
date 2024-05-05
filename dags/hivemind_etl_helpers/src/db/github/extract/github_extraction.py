from datetime import datetime

import neo4j
from hivemind_etl_helpers.src.db.github.extract.issues import GithubIssueExtraction
from hivemind_etl_helpers.src.db.github.schema import GitHubIssue


class GithubExtraction:
    def __init__(self):
        # to be uncommented once other pull requests
        # regarding `extraction` are ready
        # self.commits_extraction = GithubCommitExtraction()
        # self.pull_requests_extraction = GithubPullRequestsExtraction()
        # self.comment_extraction = GitHubCommentExtraction()
        self.issue_extraction = GithubIssueExtraction()

    def _fetch_raw_issues(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[neo4j._data.Record]:
        return self.issue_extraction._fetch_raw_issues(
            repository_id, from_date, **kwargs
        )

    def fetch_issues(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubIssue]:
        return self.issue_extraction.fetch_issues(repository_id, from_date, **kwargs)

    def _fetch_raw_issue_ids(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[neo4j._data.Record]:
        return self.issue_extraction._fetch_raw_issue_ids(
            repository_id, from_date, **kwargs
        )

    def fetch_issue_ids(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubIssue]:
        return self.issue_extraction.fetch_issue_ids(repository_id, from_date, **kwargs)
