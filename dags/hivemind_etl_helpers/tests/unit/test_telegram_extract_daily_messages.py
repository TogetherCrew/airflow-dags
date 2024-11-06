import unittest
from datetime import datetime
from unittest.mock import patch

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.telegram.extract import ExtractMessagesDaily
from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel


class TestExtractMessagesDaily(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        self.chat_id = 12344321
        self.extractor = ExtractMessagesDaily(chat_id=self.chat_id)

    @patch(
        "hivemind_etl_helpers.src.db.telegram.extract.messages.ExtractMessages.extract"
    )
    def test_empty_message_list(self, mock_extract):
        # Test with an empty message list
        mock_extract.return_value = []

        result = self.extractor.extract(from_date=None)
        self.assertEqual(result, {}, "Expected empty dictionary for empty message list")

    @patch(
        "hivemind_etl_helpers.src.db.telegram.extract.messages.ExtractMessages.extract"
    )
    def test_single_message(self, mock_extract):
        # Test with a single message
        msg = TelegramMessagesModel(
            message_id=1,
            message_text="Hello, World!",
            author_username="user1",
            message_created_at=1633046400.0,  # Equivalent to 2021-10-01T00:00:00 UTC
            message_edited_at=1633046500.0,
            mentions=["user2"],
            repliers=["user3"],
            reactors=["user4"],
        )
        mock_extract.return_value = [msg]

        result = self.extractor.extract(from_date=None)
        expected_date = datetime.fromtimestamp(msg.message_created_at).date()

        self.assertIn(expected_date, result, "Expected date key in result")
        self.assertEqual(
            result[expected_date],
            [msg],
            "Expected message in result for the given date",
        )

    @patch(
        "hivemind_etl_helpers.src.db.telegram.extract.messages.ExtractMessages.extract"
    )
    def test_multiple_messages_same_date(self, mock_extract):
        # Test with multiple messages on the same date
        msg1 = TelegramMessagesModel(
            message_id=1,
            message_text="Message 1",
            author_username="user1",
            message_created_at=1633046400.0,  # Equivalent to 2021-10-01
            message_edited_at=1633046500.0,
            mentions=["user2"],
            repliers=["user3"],
            reactors=["user4"],
        )
        msg2 = TelegramMessagesModel(
            message_id=2,
            message_text="Message 2",
            author_username="user2",
            message_created_at=1633050000.0,  # Same date as msg1
            message_edited_at=1633050500.0,
            mentions=["user1"],
            repliers=["user3"],
            reactors=["user4"],
        )
        mock_extract.return_value = [msg1, msg2]

        result = self.extractor.extract(from_date=None)
        expected_date = datetime.fromtimestamp(msg1.message_created_at).date()

        self.assertIn(expected_date, result, "Expected date key in result")
        self.assertEqual(
            result[expected_date], [msg1, msg2], "Expected messages on the same date"
        )

    @patch(
        "hivemind_etl_helpers.src.db.telegram.extract.messages.ExtractMessages.extract"
    )
    def test_multiple_messages_different_dates(self, mock_extract):
        # Test with messages on different dates
        msg1 = TelegramMessagesModel(
            message_id=1,
            message_text="Message 1",
            author_username="user1",
            message_created_at=1633046400.0,  # 2021-10-01
            message_edited_at=1633046500.0,
            mentions=["user2"],
            repliers=["user3"],
            reactors=["user4"],
        )
        msg2 = TelegramMessagesModel(
            message_id=2,
            message_text="Message 2",
            author_username="user2",
            message_created_at=1633132800.0,  # 2021-10-02
            message_edited_at=1633132900.0,
            mentions=["user1"],
            repliers=["user3"],
            reactors=["user4"],
        )
        mock_extract.return_value = [msg1, msg2]

        result = self.extractor.extract(from_date=None)
        date1 = datetime.fromtimestamp(msg1.message_created_at).date()
        date2 = datetime.fromtimestamp(msg2.message_created_at).date()

        self.assertIn(date1, result, "Expected first date key in result")
        self.assertIn(date2, result, "Expected second date key in result")
        self.assertEqual(
            result[date1], [msg1], "Expected first message on the first date"
        )
        self.assertEqual(
            result[date2], [msg2], "Expected second message on the second date"
        )
