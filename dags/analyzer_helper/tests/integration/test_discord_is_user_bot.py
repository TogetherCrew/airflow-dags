import unittest
from datetime import datetime

from analyzer_helper.discord.utils.is_user_bot import UserBotChecker
from bson import ObjectId
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestUserBotChecker(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.guild_id = "discord"
        self.db = self.client[self.guild_id]
        self.guildmembers_collection = self.db["guildmembers"]
        self.guildmembers_collection.delete_many({})

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
        self.guildmembers_collection.delete_many({})

    def test_is_user_bot(self):
        checker = UserBotChecker(self.guild_id)

        self.assertFalse(checker.is_user_bot("DUMMY_DISCORD_ID_1"))
        self.assertTrue(checker.is_user_bot("DUMMY_DISCORD_ID_4"))
        self.assertFalse(checker.is_user_bot("DUMMY_DISCORD_ID_6"))
