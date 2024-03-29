import unittest

from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from llama_index.core import Document, MockEmbedding, Settings
from llama_index.core.llms import MockLLM


class TestDiscoursePrepareDailySummaryDocuments(unittest.TestCase):
    def setUp(self):
        Settings.llm = MockLLM()
        Settings.chunk_size = 256
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def test_prepare_documents_empty_input(self):
        self.setUp()
        forum_id = "12121221212"
        forum_endpoint = "sample_endpoint"

        prepare_summaries = DiscourseSummary(
            forum_id=forum_id,
            forum_endpoint=forum_endpoint,
        )

        docs = prepare_summaries.prepare_daily_summary_documents(daily_summaries={})

        self.assertEqual(docs, [])

    def test_prepare_documents_some_inputs(self):
        self.setUp()
        forum_id = "12121221212"
        forum_endpoint = "sample_endpoint"

        prepare_summaries = DiscourseSummary(
            forum_id=forum_id,
            forum_endpoint=forum_endpoint,
        )

        daily_summaries = {
            "2023-01-01": "This is a summary text",
            "2023-01-02": "This is a summary text",
            "2023-01-03": "This is a summary text",
        }

        docs = prepare_summaries.prepare_daily_summary_documents(
            daily_summaries=daily_summaries
        )
        self.assertEqual(len(docs), 3)
        for doc in docs:
            self.assertIsInstance(doc, Document)
            self.assertEqual(doc.text, "This is a summary text")
            self.assertEqual(doc.metadata["topic"], None)
            self.assertEqual(doc.metadata["category"], None)
            self.assertIn(
                doc.metadata["date"], ["2023-01-01", "2023-01-02", "2023-01-03"]
            )
