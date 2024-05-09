from datetime import datetime

from hivemind_etl_helpers.src.db.github.extract.comment import GitHubCommentExtraction
from hivemind_etl_helpers.src.db.github.extract.commit import GithubCommitExtraction
from hivemind_etl_helpers.src.db.github.schema import GitHubComment, GitHubCommit


class GithubExtraction:
    def __init__(self):
        self.comment_extraction = GitHubCommentExtraction()
        self.commit_extraction = GithubCommitExtraction()

    def fetch_comments(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubComment]:
        return self.comment_extraction.fetch_comments(
            repository_id, from_date, **kwargs
        )

    def fetch_commits(
        self,
        repository_id: list[int],
        from_date: datetime | None = None,
    ) -> list[GitHubCommit]:
        return self.commit_extraction.fetch_commits(
            repository_id=repository_id,
            from_date=from_date,
        )
