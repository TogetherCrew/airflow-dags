import unittest

from hivemind_etl_helpers.src.db.discord.summary.prepare_summaries import (
    PrepareSummaries,
)
from llama_index.core import Document, MockEmbedding, Settings
from llama_index.core.llms import MockLLM


class TestPrepareSummaries(unittest.TestCase):
    def setUp(self):
        Settings.llm = MockLLM()
        Settings.chunk_size = 256
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def test_prepare_daily_summaries_empty_data(self):
        self.setUp()

        prepare_summaries = PrepareSummaries()
        summaries, channel_docs = prepare_summaries.prepare_daily_summaries(
            channel_summaries={},
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})
        self.assertEqual(channel_docs, [])

    def test_prepare_daily_summaries_some_data(self):
        self.setUp()

        sample_channel_summary = {
            "2023-11-28": {
                "channel#1": "Summary for channel#1",
                "channel#2": "Summary for channel#2",
                "channel#3": "Summary for channel#3",
            }
        }

        prepare_summaries = PrepareSummaries()
        summaries, channel_docs = prepare_summaries.prepare_daily_summaries(
            channel_summaries=sample_channel_summary,
            summarization_query="Please give a summary of the data you have.",
        )
        # 3 documents for 3 channels
        self.assertEqual(len(channel_docs), 3)
        for doc in channel_docs:
            self.assertIsInstance(doc, Document)
        # date
        self.assertEqual(list(summaries.keys()), ["2023-11-28"])
        # channels
        self.assertIsInstance(summaries["2023-11-28"], str)
