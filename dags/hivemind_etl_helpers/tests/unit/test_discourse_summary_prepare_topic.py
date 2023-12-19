import unittest

import neo4j
from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from llama_index import MockEmbedding, ServiceContext
from llama_index.llms import MockLLM


class TestDiscoursePrepareTopicSummaries(unittest.TestCase):
    def setUp(self):
        self.mock_llm = MockLLM()
        self.service_context = ServiceContext.from_defaults(
            llm=MockLLM(), chunk_size=256, embed_model=MockEmbedding(embed_dim=1024)
        )

    def test_prepare_topic_summaries_empty_data(self):
        self.setUp()
        forum_id = "12121221212"

        prepare_summaries = DiscourseSummary(
            service_context=self.service_context, llm=self.mock_llm, forum_id=forum_id
        )

        summaries = prepare_summaries.prepare_topic_summaries(
            raw_data_grouped=[],
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})

    def test_prepare_topic_summaries_some_data(self):
        self.setUp()
        forum_id = "12121221212"

        raw_data_grouped = []
        for i in range(2):
            data = neo4j._data.Record(
                {
                    "date": f"2022-01-0{i+1}",
                    "posts": [
                        {
                            "author_name": f"user{i+1}",
                            "authorTrustLevel": 4 + i,
                            "raw": f"texttexttext of post {i+1}",
                            "postId": 100 + i,
                            "liker_names": [],
                            "replier_names": [f"user{i}"],
                            "replier_usernames": [f"user#{i}"],
                            "createdAt": "2022-01-01T00:00:00.000Z",
                            "forum_endpoint": "sample.com",
                            "topic": f"topic#{i}",
                            "liker_usernames": [],
                            "category": f"SampleCat{i}",
                            "author_username": f"user#{i}",
                            "updatedAt": f"2022-01-01T0{i}:00:00.000Z",
                        }
                    ],
                }
            )
            raw_data_grouped.append(data)

        prepare_summaries = DiscourseSummary(
            service_context=self.service_context, llm=self.mock_llm, forum_id=forum_id
        )
        summaries = prepare_summaries.prepare_topic_summaries(
            raw_data_grouped=raw_data_grouped,
            summarization_query="Please give a summary of the data you have.",
        )
        print(summaries)
        # date
        self.assertEqual(list(summaries.keys()), ["2022-01-01", "2022-01-02"])
        # categories
        self.assertEqual(list(summaries["2022-01-01"].keys()), ["SampleCat0"])
        self.assertEqual(list(summaries["2022-01-02"].keys()), ["SampleCat1"])
        # topics
        self.assertEqual(
            list(summaries["2022-01-01"]["SampleCat0"].keys()), ["topic#0"]
        )
        self.assertEqual(
            list(summaries["2022-01-02"]["SampleCat1"].keys()), ["topic#1"]
        )
        # the summary strings
        self.assertIsInstance(summaries["2022-01-01"]["SampleCat0"]["topic#0"], str)
        self.assertIsInstance(summaries["2022-01-02"]["SampleCat1"]["topic#1"], str)
