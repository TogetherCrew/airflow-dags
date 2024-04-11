import unittest

from hivemind_etl_helpers.src.db.discord.summary.summary_utils import (
    DiscordSummaryTransformer,
)
from llama_index.core import Document


class TestTransformSummaryToDocument(unittest.TestCase):
    def setUp(self) -> None:
        self.discord_summary_transformer = DiscordSummaryTransformer()

    def test_transform_thread_summary_to_document(self):
        # Test transform_thread_summary_to_document function
        thread_name = "Thread 1"
        thread_summary = "Summary for Thread 1"
        summary_date = "2023-01-01"
        thread_channel = "General"

        expected_document = Document(
            text=thread_summary,
            metadata={
                "date": summary_date,
                "thread": thread_name,
                "channel": thread_channel,
            },
        )

        result_document = (
            self.discord_summary_transformer.transform_thread_summary_to_document(
                thread_name, thread_summary, summary_date, thread_channel
            )
        )

        self.assertEqual(result_document.text, expected_document.text)
        self.assertEqual(
            result_document.metadata["date"], expected_document.metadata["date"]
        )
        self.assertEqual(
            result_document.metadata["thread"], expected_document.metadata["thread"]
        )
        self.assertEqual(
            result_document.metadata["channel"], expected_document.metadata["channel"]
        )

    def test_transform_channel_summary_to_document(self):
        # Test transform_channel_summary_to_document function
        channel_name = "General"
        channel_summary = "Summary for General channel"
        summary_date = "2023-01-01"

        expected_document = Document(
            text=channel_summary,
            metadata={
                "date": summary_date,
                "channel": channel_name,
            },
        )

        result_document = (
            self.discord_summary_transformer.transform_channel_summary_to_document(
                channel_name, channel_summary, summary_date
            )
        )

        self.assertEqual(result_document.text, expected_document.text)
        self.assertEqual(
            result_document.metadata["date"], expected_document.metadata["date"]
        )
        self.assertEqual(
            result_document.metadata["channel"], expected_document.metadata["channel"]
        )
