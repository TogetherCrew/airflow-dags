class GitHubPullRequest:
    def __init__(
        self,
        repository_id: int,
        repository_name: str,
        issues_url: list[str],
        created_at: str,
        title: str,
        pr_id: str,
        closed_at: str | None,
        merged_at: str | None,
        state: str,
        url: str,
    ) -> None:
        self.repository_id = repository_id
        self.repository_name = repository_name
        self.issues_url = issues_url
        self.created_at = created_at
        self.title = title
        self.id = pr_id
        self.closed_at = closed_at
        self.merged_at = merged_at
        self.state = state
        self.url = url

    @classmethod
    def from_dict(cls, pr_data: dict[str, str | None]) -> "GitHubPullRequest":
        return cls(
            pr_data["repository_id"],
            pr_data["repository_name"],
            pr_data["issues_url"],
            pr_data["created_at"],
            pr_data["title"],
            pr_data["id"],
            pr_data["closed_at"],
            pr_data["merged_at"],
            pr_data["state"],
            pr_data["url"],
        )

    def to_dict(self) -> dict[str | None]:
        return {
            "repository_id": self.repository_id,
            "repository_name": self.repository_name,
            "issues_url": self.issues_url,
            "created_at": self.created_at,
            "title": self.title,
            "id": self.id,
            "closed_at": self.closed_at,
            "merged_at": self.merged_at,
            "state": self.state,
            "url": self.url,
        }
