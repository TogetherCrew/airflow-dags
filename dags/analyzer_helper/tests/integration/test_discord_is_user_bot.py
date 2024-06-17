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

        self.rawinfo_collection.insert_many([
            {
                "_id": ObjectId(),
                "type": 0,
                "author": "111111111111111111",
                "content": "sample message",
                "user_mentions": ["63632723832823823", "83279873210490238"],
                "role_mentions": ["873892901809120912", "897234876127365121"],
                "reactions": ["44444444444444444,thelounge",
                              "91919191919191919,6373687382748239,👌"],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "888888888888888888",
                "channelId": "999999999999999999",
                "channelName": "💬・general-chat",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False
            },
            {
                "_id": ObjectId(),
                "type": 0,
                "author": "222222222222222222",
                "content": "another sample message",
                "user_mentions": ["99999999999999999"],
                "role_mentions": [],
                "reactions": [],
                "replied_user": "111111111111111111",
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "777777777777777777",
                "channelId": "888888888888888888",
                "channelName": "📣・announcements",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False
            }
        ])
        self.guildmembers_collection.insert_many([
            {
                "_id": ObjectId(),
                "discordId": "111111111111111111",
                "username": "user1",
                "roles": ["1088165451651092635"],
                "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "avatar": "b50adff099924dd5e6b72d13f77eb9d7",
                "isBot": False,
                "discriminator": "1234",
                "permissions": "559642693856991",
                "deletedAt": None,
                "globalName": None,
                "nickname": None
            },
            {
                "_id": ObjectId(),
                "discordId": "222222222222222222",
                "username": "user2",
                "roles": [],
                "joinedAt": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "avatar": "9876543210abcdef",
                "isBot": True,
                "discriminator": "5678",
                "permissions": "123456789",
                "deletedAt": None,
                "globalName": None,
                "nickname": None
            }
        ])

    def tearDown(self):
        self.rawinfo_collection.delete_many({})
        self.guildmembers_collection.delete_many({})

    def test_is_user_bot(self):
        checker = UserBotChecker("discord")

        self.assertFalse(checker.is_user_bot("111111111111111111"))

        self.assertTrue(checker.is_user_bot("222222222222222222"))

        self.assertFalse(checker.is_user_bot("333333333333333333"))
