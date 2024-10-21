from pydantic import BaseModel


class TelegramMessagesModel(BaseModel):
    """
    Represents a Telegram message with its associated metadata.
    """

    message_id: int
    message_text: str
    author_username: str
    message_created_at: float
    message_edited_at: float
    mentions: list[str]
    repliers: list[str]
    reactors: list[str]
