import unittest

from llama_index import MockEmbedding, ServiceContext, Document
from llama_index.llms import MockLLM

from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)


class TestDiscoursePrepareDailySummaries(unittest.TestCase):
    def setUp(self):
        self.mock_llm = MockLLM()
        self.service_context = ServiceContext.from_defaults(
            llm=MockLLM(), chunk_size=256, embed_model=MockEmbedding(embed_dim=1024)
        )

    def test_prepare_daily_summaries_empty_data(self):
        self.setUp()
        forum_id = "12121221212"

        prepare_summaries = DiscourseSummary(
            service_context=self.service_context, llm=self.mock_llm, forum_id=forum_id
        )

        summaries, docs = prepare_summaries.prepare_daily_summaries(
            category_summaries={},
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})
        self.assertEqual(docs, [])

    def test_prepare_daily_summaries_some_data(self):
        self.setUp()
        forum_id = "12121221212"

        prepare_summaries = DiscourseSummary(
            service_context=self.service_context, llm=self.mock_llm, forum_id=forum_id
        )

        category_summaries = {
            "2023-01-01": {
                "category#1": "Something discussed here",
                "category#2": "Something discussed here",
            },
            "2023-01-02": {
                "category#3": "Something discussed here",
                "category#4": "Something discussed here",
            },
        }
        (
            daily_summaries,
            topic_summary_documents,
        ) = prepare_summaries.prepare_daily_summaries(
            category_summaries=category_summaries,
            summarization_query="Please give a summary of the data you have.",
        )
        # date
        self.assertEqual(list(daily_summaries.keys()), ["2023-01-01", "2023-01-02"])

        # the summary strings
        self.assertIsInstance(daily_summaries["2023-01-01"], str)
        self.assertIsInstance(daily_summaries["2023-01-01"], str)
        self.assertIsInstance(topic_summary_documents, list)

        for doc in topic_summary_documents:
            self.assertIsInstance(doc, Document)

    def test_prepare_daily_summaries_some_data_check_documents(self):
        self.setUp()
        forum_id = "12121221212"

        prepare_summaries = DiscourseSummary(
            service_context=self.service_context, llm=self.mock_llm, forum_id=forum_id
        )

        category_summaries = {
            "2023-01-01": {
                "category#1": "Something discussed here",
                "category#2": "Something discussed here",
            },
            "2023-01-02": {
                "category#3": "Something discussed here",
                "category#4": "Something discussed here",
            },
        }

        _, category_summary_documenets = prepare_summaries.prepare_daily_summaries(
            category_summaries=category_summaries,
            summarization_query="Please give a summary of the data you have.",
        )

        for doc in category_summary_documenets:
            self.assertIsInstance(doc, Document)
            self.assertIn(doc.metadata["date"], ["2023-01-01", "2023-01-02"])
            self.assertIn(
                doc.metadata["category"],
                ["category#1", "category#2", "category#3", "category#4"],
            )
            self.assertEqual(doc.metadata["topic"], None)
            self.assertIsInstance(doc.text, str)
