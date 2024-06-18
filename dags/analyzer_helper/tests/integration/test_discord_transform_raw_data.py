import unittest
from datetime import datetime

from analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData
from bson import ObjectId
from analyzer_helper.discord.utils.is_user_bot import UserBotChecker
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordTransformRawData(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client["discord_platform"]
        self.platform_id = "discord"
        self.transformer = DiscordTransformRawData()
        self.bot_checker = UserBotChecker(self.platform_id)
        self.platform_id = "discord_platform1"
        self.guildmembers_collection = self.db["guildmembers"]
        self.period = datetime(2023, 1, 1)
        self.guildmembers_collection.delete_many({})

        self.guildmembers_collection.insert_many(
            [
                {
                    "_id": ObjectId(),
                    "discordId": "user123",
                    "username": "user1",
                    "roles": ["1088165451651092635"],
                    "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "avatar": "b50adff099924dd5e6b72d13f77eb9d7",
                    "isBot": False,
                    "discriminator": "1234",
                    "permissions": "559642693856991",
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                },
                {
                    "_id": ObjectId(),
                    "discordId": "user789",
                    "username": "user2",
                    "roles": [],
                    "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "avatar": "9876543210abcdef",
                    "isBot": True,
                    "discriminator": "5678",
                    "permissions": "123456789",
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                },
                {
                    "_id": ObjectId(),
                    "discordId": "user456",
                    "username": "user3",
                    "roles": [],
                    "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "avatar": "9876543210abcdef",
                    "isBot": False,
                    "discriminator": "5678",
                    "permissions": "123456789",
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                },
                {
                    "_id": ObjectId(),
                    "discordId": "user1",
                    "username": "user4",
                    "roles": [],
                    "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "avatar": "9876543210abcdef",
                    "isBot": True,
                    "discriminator": "5678",
                    "permissions": "123456789",
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                },
                {
                    "_id": ObjectId(),
                    "discordId": "user2",
                    "username": "user5",
                    "roles": [],
                    "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "avatar": "9876543210abcdef",
                    "isBot": False,
                    "discriminator": "5678",
                    "permissions": "123456789",
                    "deletedAt": None,
                    "globalName": None,
                    "nickname": None,
                },
            ]
        )

    def test_transform_data_with_replied_user(self):
        raw_data = [
            {
                "author": "user123",
                "replied_user": "user789",
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "threadId": "thread123",
                "createdDate": self.period,
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": ["user789"],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": "user789",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": True,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": ["user123"],
                        "type": "receiver",
                    }
                ],
            },
        ]

        result = self.transformer.transform(
            raw_data=raw_data, platform_id=self.platform_id, period=self.period
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_with_user_mentions(self):
        raw_data = [
            {
                "author": "user123",
                "user_mentions": ["user456"],
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "threadId": "thread123",
                "createdDate": self.period,
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": ["user456"],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": "user456",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": ["user123"],
                        "type": "receiver",
                    }
                ],
            },
        ]

        result = self.transformer.transform(
            raw_data=raw_data, platform_id=self.platform_id, period=self.period
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_with_reactions(self):
        raw_data = [
            {
                "author": "user123",
                "reactions": ["user1, user2, :laugh:"],
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "threadId": "thread123",
                "createdDate": self.period,
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reaction",
                        "users_engaged_id": ["user1", "user2"],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": "user1",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": True,
                },
                "actions": [{"name": "reaction", "type": "emitter"}],
                "interactions": [],
            },
            {
                "author_id": "user2",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [{"name": "reaction", "type": "emitter"}],
                "interactions": [],
            },
        ]

        result = self.transformer.transform(
            raw_data=raw_data, platform=self.platform_id, period=self.period
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_empty(self):
        raw_data = []

        expected_result = []

        result = self.transformer.transform(
            raw_data=raw_data, platform_id=self.platform_id, period=self.period
        )
        self.assertEqual(result, expected_result)
