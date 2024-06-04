from datetime import datetime
import unittest

from dags.analyzer_helper.discord.discord_load_transformed_data import DiscordLoadTransformedData
from dags.hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordLoadTransformedData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoSingleton.get_instance().client
        cls.db = cls.client['discord_platform']
        cls.collection = cls.db['rawmemberactivities']

    @classmethod
    def tearDownClass(cls):
        cls.collection.delete_many({})
        cls.client.close()

    def setUp(self):
        self.collection.delete_many({})
        self.loader = DiscordLoadTransformedData('discord_platform')

    def tearDown(self):
        self.collection.delete_many({})

    def test_load_data(self):
        processed_data = [
            {
                'author_id': 'user123',
                'date': datetime(2023, 1, 1),
                'source_id': 'discord_platform',
                'metadata': {
                    'channel_id': 'channel456',
                    'channel_name': 'general',
                    'thread_id': 'thread123',
                    'thread_name': 'discussion',
                    'message_id': 'msg123',
                    'is_generated_by_webhook': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reply',
                        'users_engaged_id': ['user789'],
                        'type': 'receiver'
                    }
                ]
            }
        ]

        self.loader.load(processed_data, recompute=False)

        result = list(self.collection.find({}))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['author_id'], 'user123')

    def test_load_data_with_recompute(self):
        processed_data = [
            {
                'author_id': 'user123',
                'date': datetime(2023, 1, 1),
                'source_id': 'discord_platform',
                'metadata': {
                    'channel_id': 'channel456',
                    'channel_name': 'general',
                    'thread_id': 'thread123',
                    'thread_name': 'discussion',
                    'message_id': 'msg123',
                    'is_generated_by_webhook': False
                },
                'actions': [
                    {
                        'name': 'message',
                        'type': 'emitter'
                    }
                ],
                'interactions': [
                    {
                        'name': 'reply',
                        'users_engaged_id': ['user789'],
                        'type': 'receiver'
                    }
                ]
            }
        ]

        self.collection.insert_many([
            {'author_id': 'initial_user', 'date': datetime(2023, 1, 1)}
        ])

        self.loader.load(processed_data, recompute=True)

        result = list(self.collection.find({}))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['author_id'], 'user123')
