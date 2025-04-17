from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.discord_summary import DiscordSummary
from llama_index.core import Document, MockEmbedding, Settings
from llama_index.core.llms import MockLLM
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestDiscordGroupedDataPreparation(TestCase):
    def setUp(self):
        Settings.llm = MockLLM()
        Settings.chunk_size = 512
        Settings.embed_model = MockEmbedding(embed_dim=1024)

    def setup_db(
        self,
        channels: list[str],
        create_modules: bool = True,
        create_platform: bool = True,
        guild_id: str = "1234",
    ):
        client = MongoSingleton.get_instance().client

        community_id = ObjectId("9f59dd4f38f3474accdc8f24")
        platform_id = ObjectId("063a2a74282db2c00fbc2428")

        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        if create_modules:
            data = {
                "name": "hivemind",
                "communityId": community_id,
                "options": {
                    "platforms": [
                        {
                            "platformId": platform_id,
                            "fromDate": datetime(2023, 10, 1),
                            "options": {
                                "channels": channels,
                                "roles": ["role_id"],
                                "users": ["user_id"],
                            },
                        }
                    ]
                },
                "activated": True,
            }
            client["Core"]["modules"].insert_one(data)

        if create_platform:
            client["Core"]["platforms"].insert_one(
                {
                    "_id": platform_id,
                    "name": "discord",
                    "metadata": {
                        "action": {
                            "INT_THR": 1,
                            "UW_DEG_THR": 1,
                            "PAUSED_T_THR": 1,
                            "CON_T_THR": 4,
                            "CON_O_THR": 3,
                            "EDGE_STR_THR": 5,
                            "UW_THR_DEG_THR": 5,
                            "VITAL_T_THR": 4,
                            "VITAL_O_THR": 3,
                            "STILL_T_THR": 2,
                            "STILL_O_THR": 2,
                            "DROP_H_THR": 2,
                            "DROP_I_THR": 1,
                        },
                        "window": {"period_size": 7, "step_size": 1},
                        "id": guild_id,
                        "isInProgress": False,
                        "period": datetime.now() - timedelta(days=35),
                        "icon": "some_icon_hash",
                        "selectedChannels": channels,
                        "name": "GuildName",
                    },
                    "community": community_id,
                    "disconnectedAt": None,
                    "connectedAt": datetime.now(),
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                }
            )

    def test_empty_data_prepare(self):
        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 8, 1)

        discord_summary = DiscordSummary()
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id,
            selected_channels=channels,
            from_date=from_date,
            summarization_prefix="",
        )

        self.assertEqual(thread_summary_docs, [])
        self.assertEqual(channel_summary_docs, [])
        self.assertEqual(day_summary_docs, [])

    def test_some_data_prepare_with_from_date(self):
        channels = ["111111", "22222"]
        user_ids = ["user1", "user2"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("guildmembers")

        for user in user_ids:
            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": False,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )
        from_date = datetime(2023, 8, 1)

        raw_data = []
        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "general",
                "threadId": "987123",
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        discord_summary = DiscordSummary()
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id,
            selected_channels=channels,
            from_date=from_date,
            summarization_prefix="",
        )

        # we had 2 days with 3 channels of each 1 thread
        # MockLLM will output exactly the given query to it
        # which is out custom prompt
        # each day the MockLLM output would be multiplied
        # by 7 because we're breaking the lines
        self.assertEqual(len(thread_summary_docs), 6 * 7)
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
        channels = ["111111", "22222"]
        user_ids = ["user1", "user2"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("guildmembers")

        for user in user_ids:
            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": False,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        from_date = datetime(2023, 11, 1)

        raw_data = []
        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "general",
                "threadId": None,
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
            data = {
                "type": 0,
                "author": user_ids[i],
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[i % len(channels)],
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        discord_summary = DiscordSummary()
        (
            thread_summary_docs,
            channel_summary_docs,
            day_summary_docs,
        ) = discord_summary.prepare_summaries(
            guild_id,
            selected_channels=channels,
            from_date=from_date,
            summarization_prefix="",
        )

        self.assertEqual(thread_summary_docs, [])
        self.assertEqual(channel_summary_docs, [])
        self.assertEqual(day_summary_docs, [])
