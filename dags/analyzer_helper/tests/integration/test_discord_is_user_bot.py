import unittest
from dags.analyzer_helper.discord.utils.is_user_bot import UserBotChecker
from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestUserBotChecker(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.client = MongoSingleton.get_instance().client
        self.platform_id = 'discord'
        self.guild_id = 'discord_guild'
        self.db = self.client[self.guild_id]
        self.rawinfo_collection = self.db["rawinfos"]
        self.guildmembers_collection = self.db["guildmembers"]

        self.rawinfo_collection.insert_many([
            {"author": "user1"},
            {"author": "user2"},
            {"author": "user3"}
        ])
        self.guildmembers_collection.insert_many([
            {"discordId": "user1", "isBot": False},
            {"discordId": "user2", "isBot": True}
        ])

    def setUp(self):

        self.db = self.client[self.guild_id]
        self.collection = self.db['guildmembers']
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_is_user_bot(self):
        checker = UserBotChecker("discord")

        self.assertFalse(checker.is_user_bot("user1"))

        self.assertTrue(checker.is_user_bot("user2"))

        self.assertFalse(checker.is_user_bot("user3"))
