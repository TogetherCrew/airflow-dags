class GitHubComment:
    def __init__(
        self,
        id: int,
        repository_name: str,
        url: list[str],
        created_at: str,
        updated_at: str | None,
        related_title: str,
        related_node: str,
        text: str,
        latest_saved_at: str,
        reactions: dict[str, int],
    ) -> None:
        """
        the github comment data serialized into a class
        the ractions is a dictionary with keys as
        the emoji name and values the count of the reaction
        """
        self.id = id
        self.repository_name = repository_name
        self.created_at = created_at
        self.related_title = related_title
        self.related_node = related_node
        self.updated_at = updated_at
        self.url = url
        self.text = text
        self.latest_saved_at = latest_saved_at
        self.reactions = reactions

    @classmethod
    def from_dict(
        cls, pr_data: dict[str, int | str | dict[str, int]]
    ) -> "GitHubComment":
        # TODO: Update these when data got updated
        return cls(
            id=pr_data["id"],
            repository_name=pr_data["repository_name"],
            url=pr_data["url"],
            created_at=pr_data["created_at"],
            updated_at=pr_data["updated_at"],
            related_title=pr_data["related_title"],
            related_node=pr_data["related_node"],
            text=pr_data["text"],
            latest_saved_at=pr_data["latest_saved_at"],
            reactions=pr_data["reactions"],
        )

    def to_dict(self) -> dict[str, int | str | dict[str, int]]:
        return {
            "id": self.id,
            "repository_name": self.repository_name,
            "url": self.url,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "related_title": self.related_title,
            "related_node": self.related_node,
            "text": self.text,
            "latest_saved_at": self.latest_saved_at,
            "reactions": self.reactions,
        }
