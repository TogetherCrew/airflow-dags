from hivemind_etl_helpers.src.db.telegram.utils import (
    TelegramPlatform,
)


class TelegramUtils(TelegramPlatform):
    def __init__(self, chat_id: str, chat_name: str) -> None:
        super().__init__(chat_id, chat_name)
