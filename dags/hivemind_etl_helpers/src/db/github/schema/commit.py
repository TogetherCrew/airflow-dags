from hivemind_etl_helpers.src.db.github.schema.utils import parse_date_variables


class GitHubCommit:
    def __init__(
        self,
        author_name: str,
        message: str,
        api_url: str,
        html_url: str,
        repository_id: int,
        repository_name: str,
        sha: str,
        latest_saved_at: str,
        created_at: str,
        verification: str,
    ) -> None:
        """
        GitHub commit data serialized into a class.
        The verification is a dictionary with keys as the verification type
        and values as the corresponding verification status.
        """
        self.author_name = author_name
        self.message = message
        self.api_url = api_url
        self.html_url = html_url
        self.repository_id = repository_id
        self.repository_name = repository_name
        self.sha = sha
        self.latest_saved_at = parse_date_variables(latest_saved_at)
        self.created_at = parse_date_variables(created_at)
        self.verification = verification

    @classmethod
    def from_dict(cls, data: dict[str, str | int]) -> "GitHubCommit":
        # TODO: Update these when data gets updated
        return cls(
            author_name=data["author_name"],  # type: ignore
            message=data["message"],  # type: ignore
            api_url=data["api_url"],  # type: ignore
            html_url=data["html_url"],  # type: ignore
            repository_id=data["repository_id"],  # type: ignore
            repository_name=data["repository_name"],  # type: ignore
            sha=data["sha"],  # type: ignore
            latest_saved_at=data["latest_saved_at"],  # type: ignore
            created_at=data["created_at"],  # type: ignore
            verification=data["verification"],  # type: ignore
        )

    def to_dict(self) -> dict[str, str | int]:
        return {
            "author_name": self.author_name,
            "message": self.message,
            "api_url": self.api_url,
            "html_url": self.html_url,
            "repository_id": self.repository_id,
            "repository_name": self.repository_name,
            "sha": self.sha,
            "latest_saved_at": self.latest_saved_at,
            "created_at": self.created_at,
            "verification": self.verification,
            "type": "commit",
        }
