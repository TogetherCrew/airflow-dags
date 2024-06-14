import unittest

from analyzer_helper.discord.utils.is_user_bot import UserBotChecker
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
            [{"author": "user1"}, {"author": "user2"}, {"author": "user3"}]
        )
        self.guildmembers_collection.insert_many(
            [
                {"discordId": "user1", "isBot": False},
                {"discordId": "user2", "isBot": True},
            ]
        )

    def tearDown(self):
        self.rawinfo_collection.delete_many({})
        self.guildmembers_collection.delete_many({})

    def test_is_user_bot(self):
        checker = UserBotChecker("discord")

        self.assertFalse(checker.is_user_bot("user1"))

        self.assertTrue(checker.is_user_bot("user2"))

        self.assertFalse(checker.is_user_bot("user3"))
