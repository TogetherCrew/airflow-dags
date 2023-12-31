from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.discord.discord_summary import DiscordSummary
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from llama_index import Document, MockEmbedding, ServiceContext
from llama_index.llms import MockLLM
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams


class TestDiscordGroupedDataPreparation(TestCase):
    def setUp(self):
        self.mock_llm = MockLLM()
        chunk_size, embedding_dim = load_model_hyperparams()
        self.service_context = ServiceContext.from_defaults(
            llm=MockLLM(),
            chunk_size=chunk_size,
            embed_model=MockEmbedding(embed_dim=embedding_dim),
        )

    def test_empty_data_prepare_without_per_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        self.setUp()

        discord_summary = DiscordSummary(
            service_context=self.service_context, llm=self.mock_llm
        )
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(guild_id, summarization_prefix="")

        self.assertEqual(thread_summary_docs, [])
        self.assertEqual(channel_summary_docs, [])
        self.assertEqual(day_summary_docs, [])

    def test_empty_data_prepare_with_from_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 8, 1)
        self.setUp()

        discord_summary = DiscordSummary(
            service_context=self.service_context, llm=self.mock_llm
        )
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id, from_date=from_date, summarization_prefix=""
        )

        self.assertEqual(thread_summary_docs, [])
        self.assertEqual(channel_summary_docs, [])
        self.assertEqual(day_summary_docs, [])

    def test_some_data_prepare_with_from_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 8, 1)

        raw_data = []
        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "general",
                "threadId": None,
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        self.setUp()

        discord_summary = DiscordSummary(
            service_context=self.service_context, llm=self.mock_llm
        )
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id, from_date=from_date, summarization_prefix=""
        )

        # we had 2 days with 3 channels of each 1 thread
        self.assertEqual(len(thread_summary_docs), 6)
        for doc in thread_summary_docs:
            self.assertIsInstance(doc, Document)

        # we had 3 channels and 2 days
        self.assertEqual(len(channel_summary_docs), 6)
        for doc in channel_summary_docs:
            self.assertIsInstance(doc, Document)

        # we had 2 days
        self.assertEqual(len(day_summary_docs), 2)
        for doc in day_summary_docs:
            self.assertIsInstance(doc, Document)

    def test_some_data_prepare_after_from_date(self):
        """
        should return no data as we're getting them after the specific date
        """
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 11, 1)

        raw_data = []
        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "general",
                "threadId": None,
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": "12454123",
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        self.setUp()

        discord_summary = DiscordSummary(
            service_context=self.service_context, llm=self.mock_llm
        )
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id, from_date=from_date, summarization_prefix=""
        )

        self.assertEqual(thread_summary_docs, [])
        self.assertEqual(channel_summary_docs, [])
        self.assertEqual(day_summary_docs, [])
