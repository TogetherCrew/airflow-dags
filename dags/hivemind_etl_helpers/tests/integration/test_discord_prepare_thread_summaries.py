import unittest
from datetime import datetime

from hivemind_etl_helpers.src.db.discord.summary.prepare_summaries import (
    PrepareSummaries,
)
from llama_index.core import MockEmbedding, Settings
from llama_index.core.llms import MockLLM
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestPrepareSummaries(unittest.TestCase):
    def setUp(self):
        self.mock_llm = MockLLM()
        Settings.llm = MockLLM()
        Settings.chunk_size = 256
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def test_prepare_thread_summaries_empty_data(self):
        self.setUp()
        guild_id = "1234"

        prepare_summaries = PrepareSummaries()
        summaries = prepare_summaries.prepare_thread_summaries(
            guild_id=guild_id,
            raw_data_grouped={},
            summarization_query="Please give a summary of the data you have.",
        )

        self.assertEqual(summaries, {})

    def test_prepare_thread_summaries_some_data(self):
        self.setUp()
        guild_id = "1234"

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("guildmembers")
        client[guild_id]["guildmembers"].insert_many(
            [
                {
                    "discordId": "8989288923",
                    "username": "user1",
                    "roles": [],
                    "joinedAt": datetime.now(),
                    "avatar": "sample_avatar1",
                    "isBot": False,
                    "discriminator": "0",
                    "permissions": "58383",
                    "deletedAt": None,
                    "globalName": "user1_GlobalName",
                    "nickname": None,
                },
                {
                    "discordId": "8888341972390",
                    "username": "user2",
                    "roles": [],
                    "joinedAt": datetime.now(),
                    "avatar": "sample_avatar2",
                    "isBot": False,
                    "discriminator": "0",
                    "permissions": "58383",
                    "deletedAt": None,
                    "globalName": "user2_GlobalName",
                    "nickname": None,
                },
            ]
        )

        sample_grouped_data = {
            "2023-11-28": {
                "channel#1": {
                    "SAMPLE_THREAD": [
                        {
                            "type": 0,
                            "author": "8989288923",
                            "content": "this is a sample text",
                            "user_mentions": [],
                            "role_mentions": [],
                            "reactions": [],
                            "replied_user": None,
                            "createdDate": datetime(2023, 11, 28),
                            "messageId": "111111111",
                            "channelId": "44444444444",
                            "channelName": "channel#1",
                            "threadId": "999999999",
                            "threadName": "SAMPLE_THREAD",
                            "isGeneratedByWebhook": False,
                            "__v": 0,
                        },
                        {
                            "type": 19,
                            "author": "8888341972390",
                            "content": "Another sample text here",
                            "user_mentions": ["8989288923"],
                            "role_mentions": [],
                            "reactions": [],
                            "replied_user": "8989288923",
                            "createdDate": datetime(2023, 11, 28),
                            "messageId": "0000000000",
                            "channelId": "44444444444",
                            "channelName": "channel#1",
                            "threadId": "999999999",
                            "threadName": "SAMPLE_THREAD",
                            "isGeneratedByWebhook": False,
                            "__v": 0,
                        },
                    ]
                }
            }
        }

        prepare_summaries = PrepareSummaries()
        summaries = prepare_summaries.prepare_thread_summaries(
            guild_id=guild_id,
            raw_data_grouped=sample_grouped_data,
            summarization_query="Please give a summary of the data you have.",
        )
        print(summaries)
        # date
        self.assertEqual(list(summaries.keys()), ["2023-11-28"])
        # channels
        self.assertEqual(list(summaries["2023-11-28"].keys()), ["channel#1"])
        # threads
        self.assertEqual(
            list(summaries["2023-11-28"]["channel#1"].keys()), ["SAMPLE_THREAD"]
        )
        # the summary strings
        self.assertIsInstance(
            summaries["2023-11-28"]["channel#1"]["SAMPLE_THREAD"], str
        )
