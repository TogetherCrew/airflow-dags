from hivemind_etl_helpers.src.db.github.schema.utils import parse_date_variables


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
            author_name=data["author_name"],  # type: ignore
            repository_id=data["repository_id"],  # type: ignore
            repository_name=data["repository_name"],  # type: ignore
            issue_url=data["issue_url"],  # type: ignore
            created_at=data["created_at"],  # type: ignore
            title=data["title"],  # type: ignore
            id=data["id"],  # type: ignore
            closed_at=data["closed_at"],  # type: ignore
            merged_at=data["merged_at"],  # type: ignore
            state=data["state"],  # type: ignore
            url=data["url"],  # type: ignore
            latest_saved_at=data["latest_saved_at"],  # type: ignore
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
