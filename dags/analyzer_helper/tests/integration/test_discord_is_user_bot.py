import unittest
from datetime import datetime

from analyzer_helper.discord.utils.is_user_bot import UserBotChecker
from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestUserBotChecker(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.platform_id = "discord"
        self.guild_id = "discord_guild1"
        self.db = self.client[self.guild_id]
        self.rawinfo_collection = self.db["rawinfos"]
        self.guildmembers_collection = self.db["guildmembers"]
        self.rawinfo_collection.delete_many({})
        self.guildmembers_collection.delete_many({})

        self.rawinfo_collection.insert_many(
            [
                {
                    "_id": ObjectId(),
                    "type": 0,
                    "author": "100000000000000001",
                    "content": "sample message",
                    "user_mentions": ["100000000000000002", "100000000000000003"],
                    "role_mentions": ["100000000000000004", "100000000000000005"],
                    "reactions": [
                        "100000000000000006,thelounge",
                        "100000000000000007,100000000000000008,👌"
                    ],
                    "replied_user": None,
                    "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "messageId": "100000000000000009",
                    "channelId": "100000000000000010",
                    "channelName": "💬・general-chat",
                    "threadId": None,
                    "threadName": None,
                    "isGeneratedByWebhook": False,
                },
                {
                    "_id": ObjectId(),
                    "type": 0,
                    "author": "200000000000000011",
                    "content": "another sample message",
                    "user_mentions": ["200000000000000012"],
                    "role_mentions": [],
                    "reactions": [],
                    "replied_user": "100000000000000001",
                    "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "messageId": "200000000000000013",
                    "channelId": "200000000000000014",
                    "channelName": "📣・announcements",
                    "threadId": None,
                    "threadName": None,
                    "isGeneratedByWebhook": False,
                },
            ]
        )
        self.guildmembers_collection.insert_many(
            [
                {
                    "_id": ObjectId(),
                    "discordId": "100000000000000001",
                    "username": "user1",
                    "roles": ["300000000000000015"],
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
                    "discordId": "200000000000000011",
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
            ]
        )

    def tearDown(self):
        self.rawinfo_collection.delete_many({})
        self.guildmembers_collection.delete_many({})

    def test_is_user_bot(self):
        checker = UserBotChecker("discord")

        self.assertFalse(checker.is_user_bot("100000000000000001"))

        self.assertTrue(checker.is_user_bot("200000000000000011"))

        self.assertFalse(checker.is_user_bot("300000000000000012"))
