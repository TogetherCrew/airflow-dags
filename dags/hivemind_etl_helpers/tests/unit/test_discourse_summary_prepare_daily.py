import unittest

from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from llama_index.core import Document, MockEmbedding, Settings
from llama_index.core.llms import MockLLM


class TestDiscoursePrepareCategorySummaries(unittest.TestCase):
    def setUp(self):
        Settings.llm = MockLLM()
        Settings.chunk_size = 256
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def test_prepare_category_summaries_empty_data(self):
        self.setUp()
        forum_id = "12121221212"
        forum_endpoint = "sample_endpoint"

        prepare_summaries = DiscourseSummary(
            forum_id=forum_id,
            forum_endpoint=forum_endpoint,
        )

        summaries, docs = prepare_summaries.prepare_category_summaries(
            topic_summaries={},
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})
        self.assertEqual(docs, [])

    def test_prepare_category_summaries_some_data(self):
        self.setUp()
        forum_id = "12121221212"
        forum_endpoint = "sample_endpoint"

        prepare_summaries = DiscourseSummary(
            forum_id=forum_id,
            forum_endpoint=forum_endpoint,
        )

        topic_summaries = {
            "2023-01-01": {
                "category#1": {
                    "topic#1": "In this topic#1 a lot of thigs were discussed",
                    "topic#2": "In this topic#2 a lot of thigs were discussed",
                    "topic#3": "In this topic#3 a lot of thigs were discussed",
                },
                "category#2": {
                    "topic#4": "In this topic#4 a lot of thigs were discussed",
                    "topic#5": "In this topic#5 a lot of thigs were discussed",
                    "topic#6": "In this topic#6 a lot of thigs were discussed",
                },
            },
            "2023-01-02": {
                "category#3": {
                    "topic#7": "In this topic#7 a lot of thigs were discussed",
                    "topic#8": "In this topic#8 a lot of thigs were discussed",
                    "topic#9": "In this topic#9 a lot of thigs were discussed",
                },
                "category#4": {
                    "topic#10": "In this topic#10 a lot of thigs were discussed",
                    "topic#11": "In this topic#11 a lot of thigs were discussed",
                    "topic#12": "In this topic#12 a lot of thigs were discussed",
                },
            },
        }
        (
            category_summaries,
            topic_summary_documents,
        ) = prepare_summaries.prepare_category_summaries(
            topic_summaries=topic_summaries,
            summarization_query="Please give a summary of the data you have.",
        )
        # date
        self.assertEqual(list(category_summaries.keys()), ["2023-01-01", "2023-01-02"])
        # categories
        self.assertEqual(
            list(category_summaries["2023-01-01"].keys()), ["category#1", "category#2"]
        )
        self.assertEqual(
            list(category_summaries["2023-01-02"].keys()), ["category#3", "category#4"]
        )

        # the summary strings
        self.assertIsInstance(category_summaries["2023-01-01"]["category#1"], str)
        self.assertIsInstance(category_summaries["2023-01-01"]["category#2"], str)
        self.assertIsInstance(category_summaries["2023-01-02"]["category#3"], str)
        self.assertIsInstance(category_summaries["2023-01-02"]["category#4"], str)
        self.assertIsInstance(topic_summary_documents, list)

        for doc in topic_summary_documents:
            self.assertIsInstance(doc, Document)

    def test_prepare_category_summaries_some_data_check_documents(self):
        self.setUp()
        forum_id = "12121221212"
        forum_endpoint = "sample_endpoint"

        prepare_summaries = DiscourseSummary(
            forum_id=forum_id,
            forum_endpoint=forum_endpoint,
        )

        topic_summaries = {
            "2023-01-01": {
                "category#1": {
                    "topic#1": "In this topic#1 a lot of thigs were discussed",
                    "topic#2": "In this topic#2 a lot of thigs were discussed",
                },
                "category#2": {
                    "topic#4": "In this topic#4 a lot of thigs were discussed",
                    "topic#5": "In this topic#5 a lot of thigs were discussed",
                    "topic#6": "In this topic#6 a lot of thigs were discussed",
                },
            },
            "2023-01-02": {
                "category#3": {
                    "topic#7": "In this topic#7 a lot of thigs were discussed",
                },
            },
        }

        _, topic_summary_documents = prepare_summaries.prepare_category_summaries(
            topic_summaries=topic_summaries,
            summarization_query="Please give a summary of the data you have.",
        )

        for doc in topic_summary_documents:
            self.assertIsInstance(doc, Document)
            self.assertIn(doc.metadata["date"], ["2023-01-01", "2023-01-02"])
            self.assertIn(
                doc.metadata["category"], ["category#1", "category#2", "category#3"]
            )
            self.assertIn(
                doc.metadata["topic"],
                [
                    "topic#1",
                    "topic#2",
                    "topic#4",
                    "topic#5",
                    "topic#6",
                    "topic#6",
                    "topic#7",
                ],
            )
            self.assertIsInstance(doc.text, str)
