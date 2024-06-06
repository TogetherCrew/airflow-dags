import unittest
from datetime import datetime
from analyzer_helper.discord.discord_extract_raw_infos import DiscordExtractRawInfos
from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawInfos(unittest.TestCase):

    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client['discord_platform']
        self.collection = self.db['rawmemberactivities']
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})
        self.client.close()

    def test_extract_recompute_true(self):
        sample_data = [
            {'_id': '1', 'date': datetime(2023, 1, 1), 'data': 'test_data_1'},
            {'_id': '2', 'date': datetime(2023, 1, 2), 'data': 'test_data_2'}
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos('discord_platform')
        result = extractor.extract(datetime(2023, 1, 1), recompute=True)

        expected_result = [
            {'_id': '1', 'date': datetime(2023, 1, 1), 'data': 'test_data_1'},
            {'_id': '2', 'date': datetime(2023, 1, 2), 'data': 'test_data_2'}
        ]

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        sample_data = [
            {'_id': '1', 'date': datetime(2023, 1, 1), 'data': 'test_data_1'},
            {'_id': '2', 'date': datetime(2023, 1, 2), 'data': 'test_data_2'}
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos('discord_platform')
        result = extractor.extract(datetime(2023, 1, 2), recompute=False)

        expected_result = [
            {'_id': '2', 'date': datetime(2023, 1, 2), 'data': 'test_data_2'}
        ]

        self.assertEqual(result, expected_result)

    def test_extract_empty_data(self):
        extractor = DiscordExtractRawInfos('discord_platform')
        result = extractor.extract(datetime(2023, 1, 1), recompute=False)

        expected_result = []

        self.assertEqual(result, expected_result)
