from hivemind_etl_helpers.src.db.github.schema.utils import parse_date_variable


class GitHubComment:
    def __init__(
        self,
        author_name: str,
        id: int,
        repository_name: str,
        url: str,
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
        self.author_name = author_name
        self.id = id
        self.repository_name = repository_name
        self.created_at = parse_date_variable(created_at)
        self.related_title = related_title
        self.related_node = related_node
        self.updated_at = parse_date_variable(updated_at)
        self.url = url
        self.text = text
        self.latest_saved_at = parse_date_variable(latest_saved_at)
        self.reactions = reactions

    @classmethod
    def from_dict(cls, data: dict[str, int | str | dict[str, int]]) -> "GitHubComment":
        # TODO: Update these when data got updated
        return cls(
            author_name=data["author_name"],  # type: ignore
            id=data["id"],  # type: ignore
            repository_name=data["repository_name"],  # type: ignore
            url=data["url"],  # type: ignore
            created_at=data["created_at"],  # type: ignore
            updated_at=data["updated_at"],  # type: ignore
            related_title=data["related_title"],  # type: ignore
            related_node=data["related_node"],  # type: ignore
            text=data["text"],  # type: ignore
            latest_saved_at=data["latest_saved_at"],  # type: ignore
            reactions=data["reactions"],  # type: ignore
        )

    def to_dict(self) -> dict[str, int | str | dict[str, int]]:
        return {
            "author_name": self.author_name,
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
            "type": "comment",
        }
