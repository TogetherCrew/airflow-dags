from collections import defaultdict
from datetime import date, datetime

from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel

from . import ExtractMessages


class ExtractMessagesDaily:
    def __init__(self, chat_id: str) -> None:
        self.extractor = ExtractMessages(chat_id=chat_id)

    def extract(
        self, from_date: datetime | None = None
    ) -> dict[date, list[TelegramMessagesModel]]:
        """
        extract messages daily

        Parameters
        -----------
        from_date : datetime | None
            extract from a specific date
            if not given, extract all data

        Returns
        --------
        daily_tg_messages : dict[datetime.date, list[TelegramMessagesModel]]
            telegram messages extracted and daily grouped
        """
        messages = self.extractor.extract(from_date=from_date)

        daily_tg_messages: dict[date, list[TelegramMessagesModel]] = defaultdict(list)
        for msg in messages:
            msg_date = datetime.fromtimestamp(msg.message_created_at / 1000).date()
            daily_tg_messages[msg_date].append(msg)

        return daily_tg_messages
