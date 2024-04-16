from datetime import datetime

from dags.hivemind_etl_helpers.src.db.github.extract.comment import (
    GitHubCommentExtraction,
)
from dags.hivemind_etl_helpers.src.db.github.schema.comment import GitHubComment


class GithubExtraction:
    def __init__(self):
        self.comment_extraction = GitHubCommentExtraction()

    def fetch_comments(
        self, repository_id: list[int], from_date: datetime | None = None, **kwargs
    ) -> list[GitHubComment]:
        return self.comment_extraction.fetch_comments(
            repository_id, from_date, **kwargs
        )
