import unittest
from datetime import datetime

from dags.analyzer_helper.discord.discord_extract_raw_members import DiscordExtractRawMembers
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawMembers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoSingleton.get_instance().client
        cls.platform_id = 'discord_platform'
        cls.guild_id = 'discord_guild'
        cls.db = cls.client[cls.guild_id]
        cls.collection = cls.db['guildmembers']

    @classmethod
    def tearDownClass(cls):
        cls.collection.delete_many({})
        cls.client.close()

    def setUp(self):
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_extract_recompute_true(self):
        sample_data = [
            {'_id': '1', 'joinedAt': datetime(2023, 1, 1)},
            {'_id': '2', 'joinedAt': datetime(2023, 1, 2)},
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.platform_id, self.guild_id)
        result = extractor.extract(recompute=True)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        sample_data = [
            {'_id': '1', 'joinedAt': datetime(2023, 1, 1)},
            {'_id': '2', 'joinedAt': datetime(2023, 1, 2)},
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.platform_id, self.guild_id)
        result = extractor.extract(recompute=False)

        expected_result = []

        self.assertEqual(result, expected_result)

        self.collection.insert_one({'_id': '3', 'joinedAt': datetime(2023, 1, 3)})

        result = extractor.extract(recompute=False)
        expected_result = [{'_id': '3', 'joinedAt': datetime(2023, 1, 3)}]

        self.assertEqual(result, expected_result)
