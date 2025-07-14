from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.summary.prepare_grouped_data import (
    prepare_grouped_data,
)
from tc_hivemind_backend.db.mongo import MongoSingleton
from dateutil.parser import parse

class TestDiscordGroupedDataPreparation(TestCase):
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

    def test_empty_data_prepare_with_from_date(self):
        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("channels")
        client[guild_id].drop_collection("threads")
        from_date = datetime(2023, 8, 1)

        data = prepare_grouped_data(
            guild_id=guild_id, selected_channels=channels, from_date=from_date
        )
        self.assertEqual(data, {})

    def test_some_data_prepare_with_from_date(self):
        channels = ["111111", "22222", "33333"]
        user_ids = ["user1", "user2"]

        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("guildmembers")
        client[guild_id].drop_collection("channels")
        client[guild_id].drop_collection("threads")

        # Create channels collection
        channels_data = [
            {"channelId": "111111", "name": "general"},
            {"channelId": "22222", "name": "writing"},
            {"channelId": "33333", "name": "reading"},
        ]
        client[guild_id]["channels"].insert_many(channels_data)

        # Create threads collection
        threads_data = [
            {"id": "987123", "name": "Something"},
            {"id": "123443211", "name": "Available"},
        ]
        client[guild_id]["threads"].insert_many(threads_data)

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
                "channelId": channels[i % len(channels)],
                "channelName": None,
                "threadId": None,
                "threadName": None,
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
                "channelId": channels[i % len(channels)],
                "channelName": None,
                "threadId": "123443211",
                "threadName": None,
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
                "channelId": "33333",  # Use the reading channel ID
                "channelName": None,
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        data = prepare_grouped_data(
            guild_id=guild_id, selected_channels=channels, from_date=from_date
        )

        self.assertEqual(set(data.keys()), set([parse("2023-10-01").timestamp(), parse("2023-10-02").timestamp()]))
        for date in data.keys():
            if date == parse("2023-10-01").timestamp():
                for channel in data[date].keys():
                    if channel == "reading":
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            [None],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel][None]), 1)
                    elif channel == "writing":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Available"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Available"]), 1)
                    elif channel == "general":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Something"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Something"]), 1)
            elif date == parse("2023-10-02").timestamp():
                for channel in data[date].keys():
                    if channel == "reading":
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            [None],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel][None]), 1)
                    elif channel == "writing":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Available"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Available"]), 1)
                    elif channel == "general":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Something"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Something"]), 1)
            else:
                raise IndexError("Not possible, it shouldn't reach here")

    def test_some_data_prepare_after_from_date(self):
        """
        should return no data as we're getting them after the specific date
        """
        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("channels")
        client[guild_id].drop_collection("threads")
        from_date = datetime(2023, 11, 1)

        # Create channels collection
        channels_data = [
            {"channelId": "111111", "name": "general"},
            {"channelId": "22222", "name": "writing"},
            {"channelId": "33333", "name": "reading"},
        ]
        client[guild_id]["channels"].insert_many(channels_data)

        # Create threads collection
        threads_data = [
            {"id": "987123", "name": "Something"},
            {"id": "123443211", "name": "Available"},
        ]
        client[guild_id]["threads"].insert_many(threads_data)

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
                "channelId": channels[i % len(channels)],
                "channelName": None,
                "threadId": None,
                "threadName": None,
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
                "channelId": channels[i % len(channels)],
                "channelName": None,
                "threadId": "123443211",
                "threadName": None,
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
                "channelId": "33333",  # Use the reading channel ID
                "channelName": None,
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        data = prepare_grouped_data(
            guild_id=guild_id, selected_channels=channels, from_date=from_date
        )

        self.assertEqual(data, {})
