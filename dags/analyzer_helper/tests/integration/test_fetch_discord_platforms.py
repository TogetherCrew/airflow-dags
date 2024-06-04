import unittest
from dags.analyzer_helper.fetch_discord_platforms import FetchDiscordPlatforms
from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchDiscordPlatforms(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoSingleton.get_instance().client
        cls.db = cls.client['Core']
        cls.collection = cls.db['platforms']

    @classmethod
    def tearDownClass(cls):
        cls.collection.delete_many({})
        cls.client.close()

    def setUp(self):
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_fetch_all(self):
        sample_data = [
            {
                '_id': '1',
                'platform': 'discord',
                'metadata': {
                    'action': 'action1',
                    'window': 'window1',
                    'period': 'period1',
                    'selectedChannels': ['channel1', 'channel2'],
                    'id': 'guild1'
                }
            },
            {
                '_id': '2',
                'platform': 'discord',
                'metadata': {
                    'action': 'action2',
                    'window': 'window2',
                    'period': 'period2',
                    'selectedChannels': ['channel3'],
                    'id': 'guild2'
                }
            }
        ]

        self.collection.insert_many(sample_data)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = [
            {
                'platform_id': '1',
                'metadata': {
                    'action': 'action1',
                    'window': 'window1',
                    'period': 'period1',
                    'selectedChannels': ['channel1', 'channel2'],
                    'id': 'guild1'
                },
                'recompute': False
            },
            {
                'platform_id': '2',
                'metadata': {
                    'action': 'action2',
                    'window': 'window2',
                    'period': 'period2',
                    'selectedChannels': ['channel3'],
                    'id': 'guild2'
                },
                'recompute': False
            }
        ]

        self.assertEqual(result, expected_result)

    def test_get_empty_data(self):
        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = []

        self.assertEqual(result, expected_result)

    def test_get_single_data(self):
        sample_data = {
            '_id': '1',
            'platform': 'discord',
            'metadata': {
                'action': 'action1',
                'window': 'window1',
                'period': 'period1',
                'selectedChannels': ['channel1', 'channel2'],
                'id': 'guild1'
            }
        }

        self.collection.insert_one(sample_data)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = [
            {
                'platform_id': '1',
                'metadata': {
                    'action': 'action1',
                    'window': 'window1',
                    'period': 'period1',
                    'selectedChannels': ['channel1', 'channel2'],
                    'id': 'guild1'
                },
                'recompute': False
            }
        ]
        self.assertEqual(result, expected_result)
