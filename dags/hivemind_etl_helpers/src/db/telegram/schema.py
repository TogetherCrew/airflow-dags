from pydantic import BaseModel


class TelegramMessagesModel(BaseModel):
    message_id: int
    message_text: str
    author_username: str
    message_created_at: float
    message_edited_at: float
    mentions: list
    repliers: list
    reactors: list
