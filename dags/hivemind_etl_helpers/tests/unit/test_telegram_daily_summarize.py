import unittest
from datetime import date

from hivemind_etl_helpers.src.db.telegram.schema import TelegramMessagesModel
from hivemind_etl_helpers.src.db.telegram.transform import SummarizeMessages
from llama_index.core import Document, MockEmbedding, Settings
from llama_index.core.llms import MockLLM


class TestTelegramSummarize(unittest.TestCase):
    def setUp(self):
        Settings.llm = MockLLM()
        Settings.chunk_size = 256
        Settings.embed_model = MockEmbedding(embed_dim=1024)

        self.summarizer = SummarizeMessages(chat_id="temp_id", chat_name="temp_chat")

    def test_summarize_empty_messages(self):
        summaries = self.summarizer.summarize_daily(messages={})
        self.assertEqual(summaries, {})

    def test_summarize_single_day_single_document(self):
        summaries = self.summarizer.summarize_daily(
            messages={
                date(2024, 6, 6): [
                    TelegramMessagesModel(
                        message_id=1,
                        message_text="message 1",
                        author_username="username1",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                ],
            }
        )

        for key, summary in summaries.items():
            self.assertEqual(key, date(2024, 6, 6))
            self.assertIsInstance(summary, str)

    def test_summarize_single_day_multiple_documents(self):
        summaries = self.summarizer.summarize_daily(
            messages={
                date(2024, 6, 6): [
                    TelegramMessagesModel(
                        message_id=1,
                        message_text="message 1",
                        author_username="username1",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                    TelegramMessagesModel(
                        message_id=2,
                        message_text="message 2",
                        author_username="username2",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                ],
            }
        )
        for key, summary in summaries.items():
            self.assertEqual(key, date(2024, 6, 6))
            self.assertIsInstance(summary, str)

    def test_summarize_multiple_days_multiple_document(self):
        summaries = self.summarizer.summarize_daily(
            messages={
                date(2024, 6, 6): [
                    TelegramMessagesModel(
                        message_id=1,
                        message_text="message 1",
                        author_username="username1",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                    TelegramMessagesModel(
                        message_id=2,
                        message_text="message 2",
                        author_username="username2",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                ],
                date(2024, 6, 7): [
                    TelegramMessagesModel(
                        message_id=1,
                        message_text="message 1",
                        author_username="username1",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                    TelegramMessagesModel(
                        message_id=2,
                        message_text="message 2",
                        author_username="username2",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                ],
                date(2024, 6, 8): [
                    TelegramMessagesModel(
                        message_id=1,
                        message_text="message 1",
                        author_username="username1",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                    TelegramMessagesModel(
                        message_id=2,
                        message_text="message 2",
                        author_username="username2",
                        message_created_at=1730801345876,
                        message_edited_at=1730801345877,
                        mentions=[],
                        reactors=[],
                        repliers=[],
                    ),
                ],
            }
        )
        for key, summary in summaries.items():
            self.assertIn(
                key,
                [
                    date(2024, 6, 6),
                    date(2024, 6, 7),
                    date(2024, 6, 8),
                ],
            )
            self.assertIsInstance(summary, str)
