from hivemind_etl_helpers.src.db.github.utils.schema.parse_time import (
    parse_date_variables,
)


class GitHubComment:
    def __init__(
        self,
        author_name: str,
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
        self.author_name = author_name
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
    def from_dict(cls, data: dict[str, int | str | dict[str, int]]) -> "GitHubComment":
        created_at = parse_date_variables(data["created_at"])
        updated_at = parse_date_variables(data["updated_at"])
        latest_saved_at = parse_date_variables(data["latest_saved_at"])

        # TODO: Update these when data got updated
        return cls(
            author_name=data["author_name"],
            id=data["id"],
            repository_name=data["repository_name"],
            url=data["url"],
            created_at=created_at,
            updated_at=updated_at,
            related_title=data["related_title"],
            related_node=data["related_node"],
            text=data["text"],
            latest_saved_at=latest_saved_at,
            reactions=data["reactions"],
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
