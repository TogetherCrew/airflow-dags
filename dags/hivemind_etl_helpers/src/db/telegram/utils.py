from hivemind_etl_helpers.src.db.telegram.utils import (
    TelegramCommunity,
    TelegramPlatform,
)


class TelegramUtils(TelegramPlatform, TelegramCommunity):
    def __init__(self, chat_id: str) -> None:
        super().__init__(chat_id)
