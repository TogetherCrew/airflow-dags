import unittest
from unittest.mock import patch

from dags.analyzer_helper.discord.fetch_discord_platforms import FetchDiscordPlatforms


class TestFetchDiscordPlatformsUnit(unittest.TestCase):
    @patch('fetch_discord_platforms.MongoClient')
    def test_fetch_all(self, MockMongoClient):
        mock_client = MockMongoClient.return_value
        mock_db = mock_client['Core']
        mock_collection = mock_db['platforms']

        sample_data = [
            {
                '_id': '1',
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
                'metadata': {
                    'action': 'action2',
                    'window': 'window2',
                    'period': 'period2',
                    'selectedChannels': ['channel3'],
                    'id': 'guild2'
                }
            }
        ]

        mock_collection.find.return_value = sample_data

        fetcher = FetchDiscordPlatforms('mock_uri')

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

    @patch('fetch_discord_platforms.MongoClient')
    def test_fetch_all_empty(self, MockMongoClient):

        mock_client = MockMongoClient.return_value
        mock_db = mock_client['Core']
        mock_collection = mock_db['platforms']

        mock_collection.find.return_value = []

        fetcher = FetchDiscordPlatforms('mock_uri')

        result = fetcher.fetch_all()

        expected_result = []

        self.assertEqual(result, expected_result)
