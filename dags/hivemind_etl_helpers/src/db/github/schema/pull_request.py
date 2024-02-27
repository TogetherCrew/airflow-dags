from hivemind_etl_helpers.src.db.github.schema.utils import (
    parse_date_variables,
)


class GitHubPullRequest:
    def __init__(
        self,
        author_name: str,
        repository_id: int,
        repository_name: str,
        issue_url: str,
        created_at: str,
        title: str,
        id: int,
        closed_at: str | None,
        merged_at: str | None,
        state: str,
        url: str,
        latest_saved_at: str,
    ) -> None:
        self.author_name = author_name
        self.repository_id = repository_id
        self.repository_name = repository_name
        self.issue_url = issue_url
        self.created_at = parse_date_variables(created_at)
        self.title = title
        self.id = id
        self.closed_at = parse_date_variables(closed_at)
        self.merged_at = parse_date_variables(merged_at)
        self.state = state
        self.url = url
        self.latest_saved_at = parse_date_variables(latest_saved_at)

    @classmethod
    def from_dict(cls, data: dict[str, int | str | None]) -> "GitHubPullRequest":
        return cls(
            author_name=data["author_name"],
            repository_id=data["repository_id"],
            repository_name=data["repository_name"],
            issue_url=data["issue_url"],
            created_at=data["created_at"],
            title=data["title"],
            id=data["id"],
            closed_at=data["closed_at"],
            merged_at=data["merged_at"],
            state=data["state"],
            url=data["url"],
            latest_saved_at=data["latest_saved_at"],
        )

    def to_dict(self) -> dict[str, int | str | None]:
        return {
            "author_name": self.author_name,
            "repository_id": self.repository_id,
            "repository_name": self.repository_name,
            "issue_url": self.issue_url,
            "created_at": self.created_at,
            "title": self.title,
            "id": self.id,
            "closed_at": self.closed_at,
            "merged_at": self.merged_at,
            "state": self.state,
            "url": self.url,
            "latest_saved_at": self.latest_saved_at,
            "type": "pull_request",
        }
