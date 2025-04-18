import unittest
from datetime import datetime, timedelta

import numpy as np
from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_messages
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestFetchRawMessages(unittest.TestCase):
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
        client[guild_id].drop_collection("guildmembers")

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

    def test_fetch_raw_messages_fetch_all(self):
        client = MongoSingleton.get_instance().client
        channels = ["111111", "22222"]
        users_id = ["user1", "user2", "user3"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        # droping any previous data
        client[guild_id].drop_collection("rawinfos")

        message_count = 3

        for user in users_id:
            is_bot = False
            if user == "user3":
                is_bot = True

            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": is_bot,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        raw_data = []
        for i in range(message_count):
            data = {
                "type": 0,
                "author": users_id[i],
                "content": f"{np.random.randint(0, 10)} Apples are falling from trees!",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime.now(),
                "messageId": str(np.random.randint(1000000, 9999999)),
                "channelId": channels[i % len(channels)],
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        messages = fetch_raw_messages(
            guild_id,
            selected_channels=channels,
            from_date=datetime.now() - timedelta(hours=1),
        )

        # one message was for a bot
        self.assertEqual(len(messages), 2)

        for idx, msg in enumerate(messages):
            for field in msg.keys():
                if field != "createdDate":
                    self.assertEqual(msg[field], raw_data[idx][field])
                # date in milisecond in python and mongodb are slightly different
                # so we're considering out assertion for it
                else:
                    self.assertEqual(
                        msg[field].strftime("%Y-%m-%d %H:%M:%S"),
                        raw_data[idx][field].strftime("%Y-%m-%d %H:%M:%S"),
                    )

    def test_fetch_raw_messages_fetch_all_no_data_available(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"
        users_id = ["user1", "user2", "user3"]

        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )
        # droping any previous data
        client[guild_id].drop_collection("rawinfos")

        for user in users_id:
            is_bot = False
            if user == "user3":
                is_bot = True

            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": is_bot,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        messages = fetch_raw_messages(
            guild_id,
            selected_channels=channels,
            from_date=datetime.now() - timedelta(hours=1),
        )

        self.assertEqual(len(messages), 0)
        self.assertEqual(messages, [])

    def test_fetch_raw_messages_fetch_from_date(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"
        channels = ["111111", "22222"]
        users_id = ["user1", "user2", "user3", "user4", "user5"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        for user in users_id:
            is_bot = False
            if user == "user3":
                is_bot = True

            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": is_bot,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        # Dropping any previous data
        client[guild_id].drop_collection("rawinfos")

        # Insert messages with different dates
        raw_data = []
        for i in range(5):
            data = {
                "type": 0,
                "author": users_id[i],
                "content": f"Apples falling from trees {np.random.randint(0, 10)}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": str(np.random.randint(1000000, 9999999)),
                "channelId": channels[i % len(channels)],
                "channelName": f"general {channels[i % len(channels)]}",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        # Fetch messages from a specific date (October 3, 2023)
        from_date = datetime(2023, 10, 3)
        messages = fetch_raw_messages(
            guild_id, selected_channels=channels, from_date=from_date
        )

        # Check if the fetched messages have the correct date
        for message in messages:
            self.assertTrue(message["createdDate"] >= from_date)

        # Check if the number of fetched messages is correct
        expected_messages = [
            message
            for message in raw_data
            if message["createdDate"] >= from_date and message["author"] != "user3"
        ]
        self.assertEqual(len(messages), len(expected_messages))

        # Check if the fetched messages are equal to the expected messages
        self.assertCountEqual(messages, expected_messages)

    def test_fetch_raw_messages_fetch_limited_characters(self):
        """
        fetch raw messages and do filtering
        """
        client = MongoSingleton.get_instance().client

        guild_id = "1234"
        channels = ["111111", "22222"]
        users_id = ["user1", "user2", "user3", "user4", "user5"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        for user in users_id:
            is_bot = False
            if user == "user3":
                is_bot = True

            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": is_bot,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        # Dropping any previous data
        client[guild_id].drop_collection("rawinfos")

        # Insert messages with different dates
        raw_data = []

        data = {
            "type": 0,
            "author": users_id[0],
            "content": "AA",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 10, 1),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": channels[0],
            "channelName": f"general {channels[0]}",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        raw_data.append(data)

        data = {
            "type": 0,
            "author": users_id[1],
            "content": "A sample text with more than 15 characters!",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 10, 1),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": channels[1],
            "channelName": f"general {channels[1]}",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        messages = fetch_raw_messages(
            guild_id,
            selected_channels=channels,
            from_date=datetime(2023, 9, 20),
        )
        # Check if the fetched messages are equal to the expected messages
        self.assertEqual(len(messages), 1)

    def test_fetch_raw_messages_fetch_limited_characters_specified(self):
        """
        fetch raw messages and do filtering with a specified value
        """
        client = MongoSingleton.get_instance().client

        guild_id = "1234"
        channels = ["111111", "22222"]
        users_id = ["user1", "user2", "user3", "user4", "user5"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        for user in users_id:
            is_bot = False
            if user == "user3":
                is_bot = True

            client[guild_id]["guildmembers"].insert_one(
                {
                    "discordId": user,
                    "username": f"username_{user}",
                    "roles": None,
                    "joinedAt": datetime(2023, 1, 1),
                    "avatar": None,
                    "isBot": is_bot,
                    "discriminator": "0",
                    "permissions": None,
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                }
            )

        # Dropping any previous data
        client[guild_id].drop_collection("rawinfos")

        # Insert messages with different dates
        raw_data = []

        data = {
            "type": 0,
            "author": users_id[0],
            "content": "AA",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 10, 1),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": channels[0],
            "channelName": f"general {channels[0]}",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        raw_data.append(data)

        data = {
            "type": 0,
            "author": users_id[1],
            "content": "A sample text with more than 8 characters!",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 10, 1),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": channels[1],
            "channelName": f"general {channels[1]}",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        # Fetch messages from a specific date (October 3, 2023)
        messages = fetch_raw_messages(
            guild_id,
            selected_channels=channels,
            from_date=datetime(2023, 9, 20),
            min_word_limit=1,
        )
        # Check if the fetched messages are equal to the expected messages
        self.assertEqual(len(messages), 2)
