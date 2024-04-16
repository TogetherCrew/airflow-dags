from hivemind_etl_helpers.src.db.github.schema.utils import parse_date_variables


class GitHubIssue:
    def __init__(
        self,
        id: int,
        author_name: str,
        title: str,
        text: str,
        state: str,
        state_reason: str | None,
        created_at: str,
        updated_at: str,
        latest_saved_at: str,
        url: str,
        repository_id: str,
        repository_name: str,
    ) -> None:
        self.id = id
        self.author_name = author_name
        self.title = title
        self.text = text
        self.state = state
        self.state_reason = state_reason
        self.created_at = parse_date_variables(created_at)
        self.updated_at = parse_date_variables(updated_at)
        self.latest_saved_at = parse_date_variables(latest_saved_at)
        self.url = url
        self.repository_id = repository_id
        self.repository_name = repository_name

    @classmethod
    def from_dict(cls, issue: dict[str, str | int]) -> "GitHubIssue":
        return cls(
            id=issue["id"],  # type: ignore
            author_name=issue["author_name"],  # type: ignore
            title=issue["title"],  # type: ignore
            text=issue["text"],  # type: ignore
            state=issue["state"],  # type: ignore
            state_reason=issue["state_reason"],  # type: ignore
            created_at=issue["created_at"],  # type: ignore
            updated_at=issue["updated_at"],  # type: ignore
            latest_saved_at=issue["latest_saved_at"],  # type: ignore
            url=issue["url"],  # type: ignore
            repository_id=issue["repository_id"],  # type: ignore
            repository_name=issue["repository_name"],  # type: ignore
        )

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "id": self.id,
            "author_name": self.author_name,
            "title": self.title,
            "text": self.text,
            "state": self.state,
            "state_reason": self.state_reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "latest_saved_at": self.latest_saved_at,
            "url": self.url,
            "repository_id": self.repository_id,
            "repository_name": self.repository_name,
            "type": "issue",
        }


class GitHubIssueID(GitHubIssue):
    def __init__(
        self,
        id: int,
    ) -> None:
        self.id = id

    @classmethod
    def from_dict(cls, issue: dict[str, str | int]) -> "GitHubIssueID":
        return cls(
            id=issue["id"],  # type: ignore
        )

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "id": self.id,
        }
