from hivemind_etl_helpers.src.db.github.utils.schema.parse_time import (
    parse_date_variables,
)


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
        self.created_at = created_at
        self.updated_at = updated_at
        self.latest_saved_at = latest_saved_at
        self.url = url
        self.repository_id = repository_id
        self.repository_name = repository_name

    @classmethod
    def from_dict(cls, issue: dict[str, str | int]) -> "GitHubIssue":
        created_at = parse_date_variables(issue["created_at"])
        updated_at = parse_date_variables(issue["updated_at"])
        latest_saved_at = parse_date_variables(issue["latest_saved_at"])

        return cls(
            id=issue["id"],
            author_name=issue["author_name"],
            title=issue["title"],
            text=issue["text"],
            state=issue["state"],
            state_reason=issue["state_reason"],
            created_at=created_at,
            updated_at=updated_at,
            latest_saved_at=latest_saved_at,
            url=issue["url"],
            repository_id=issue["repository_id"],
            repository_name=issue["repository_name"],
        )

    def to_dict(self) -> dict[str, str | int]:
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
