import unittest

from hivemind_etl_helpers.src.db.discord.summary.prepare_summaries import (
    PrepareSummaries,
)
from llama_index import Document, MockEmbedding, ServiceContext
from llama_index.llms import MockLLM


class TestPrepareSummaries(unittest.TestCase):
    def setUp(self):
        self.mock_llm = MockLLM()
        self.service_context = ServiceContext.from_defaults(
            llm=MockLLM(), chunk_size=256, embed_model=MockEmbedding(embed_dim=1024)
        )

    def test_prepare_channel_summaries_empty_data(self):
        self.setUp()

        prepare_summaries = PrepareSummaries(
            service_context=self.service_context, llm=self.mock_llm
        )
        summaries, thread_docs = prepare_summaries.prepare_channel_summaries(
            thread_summaries={},
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})
        self.assertEqual(thread_docs, [])

    def test_prepare_channel_summaries_some_data(self):
        self.setUp()

        sample_thread_summary = {
            "2023-11-28": {
                "channel#1": {
                    "SAMPLE_THREAD": "A sample summary!",
                    "SAMPLE_THREAD2": "Another sample summary",
                },
                "channel#2": {
                    "THREAD#1": "A sample summary!",
                    "THREAD#2": "Another sample summary",
                },
            }
        }

        prepare_summaries = PrepareSummaries(
            service_context=self.service_context, llm=self.mock_llm
        )
        summaries, thread_docs = prepare_summaries.prepare_channel_summaries(
            thread_summaries=sample_thread_summary,
            summarization_query="Please give a summary of the data you have.",
        )
        # 4 documents for 4 threads
        self.assertEqual(len(thread_docs), 4)
        for doc in thread_docs:
            self.assertIsInstance(doc, Document)
        # date
        self.assertEqual(list(summaries.keys()), ["2023-11-28"])
        # channels
        self.assertEqual(
            list(summaries["2023-11-28"].keys()), ["channel#1", "channel#2"]
        )
        # channel summary
        self.assertIsInstance(summaries["2023-11-28"]["channel#1"], str)
        # the summary strings
        self.assertIsInstance(summaries["2023-11-28"]["channel#2"], str)
