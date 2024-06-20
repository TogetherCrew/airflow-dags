import unittest
from datetime import datetime

from analyzer_helper.discord.utils.is_user_bot import UserBotChecker
from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestUserBotChecker(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.platform_id = "discord"
        self.db = self.client[self.platform_id]
        self.rawinfo_collection = self.db["rawinfos"]
        self.guildmembers_collection = self.db["guildmembers"]
        self.rawinfo_collection.delete_many({})
        self.guildmembers_collection.delete_many({})

        self.rawinfo_collection.insert_many(
            [
                {
                    "_id": ObjectId(),
                    "type": 0,
                    "author": "DUMMY_DISCORD_ID_1",
                    "content": "sample message",
                    "user_mentions": ["DUMMY_DISCORD_ID_2", "DUMMY_DISCORD_ID_3"],
                    "role_mentions": ["DUMMY_ROLE_ID_1", "DUMMY_ROLE_ID_2"],
                    "reactions": [
                        "DUMMY_REACTION_ID_1,thelounge",
                        "DUMMY_REACTION_ID_2,DUMMY_USER_ID_1,👌",
                    ],
                    "replied_user": None,
                    "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "messageId": "DUMMY_MESSAGE_ID_1",
                    "channelId": "DUMMY_CHANNEL_ID_1",
                    "channelName": "💬・general-chat",
                    "threadId": None,
                    "threadName": None,
                    "isGeneratedByWebhook": False,
                },
                {
                    "_id": ObjectId(),
                    "type": 0,
                    "author": "DUMMY_DISCORD_ID_4",
                    "content": "another sample message",
                    "user_mentions": ["DUMMY_DISCORD_ID_5"],
                    "role_mentions": [],
                    "reactions": [],
                    "replied_user": "DUMMY_DISCORD_ID_1",
                    "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                    "messageId": "DUMMY_MESSAGE_ID_2",
                    "channelId": "DUMMY_CHANNEL_ID_2",
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
                    "discordId": "DUMMY_DISCORD_ID_1",
                    "username": "user1",
                    "roles": ["DUMMY_ROLE_ID_3"],
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
                    "discordId": "DUMMY_DISCORD_ID_4",
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
        checker = UserBotChecker(self.platform_id)

        self.assertFalse(checker.is_user_bot("DUMMY_DISCORD_ID_1"))
        self.assertTrue(checker.is_user_bot("DUMMY_DISCORD_ID_4"))
        self.assertFalse(checker.is_user_bot("DUMMY_DISCORD_ID_6"))
