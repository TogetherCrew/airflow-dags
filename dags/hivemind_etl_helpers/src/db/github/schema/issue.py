class GitHubIssue:
    def __init__(
        self,
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
        return cls(
            issue["author_name"],
            issue["title"],
            issue["text"],
            issue["state"],
            issue["state_reason"],
            issue["created_at"],
            issue["updated_at"],
            issue["latest_saved_at"],
            issue["url"],
            issue["repository_id"],
            issue["repository_name"],
        )

    def to_dict(self) -> dict[str, str | int]:
        return {
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
