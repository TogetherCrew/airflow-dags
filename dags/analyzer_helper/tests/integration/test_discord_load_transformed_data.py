from datetime import datetime
import unittest

from analyzer_helper.discord.discord_load_transformed_data import DiscordLoadTransformedData
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordLoadTransformedData(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client['discord_platform']
        self.collection = self.db['rawmemberactivities']
        self.collection.delete_many({})
        self.loader = DiscordLoadTransformedData('discord_platform')

    def tearDown(self):
        self.collection.delete_many({})
        self.client.close()

    def test_load_data(self):
        processed_data = [
            {
                "author_id": "159985870458322944",
                "date": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "source_id": "msg123",
                "metadata": {
                    "thread_id": "thread123",
                    "channel_id": "1088165451651092635",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter"
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": ["159985870458322945"],
                        "type": "emitter"
                    }
                ]
            },
            {
                "author_id": "159985870458322945",
                "date": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "source_id": "msg123",
                "metadata": {
                    "thread_id": "thread123",
                    "channel_id": "1088165451651092635",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "reply",
                        "type": "receiver"
                    }
                ],
                "interactions": []
            }
        ]

        self.loader.load(processed_data, recompute=False)

        result = list(self.collection.find({}))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['author_id'], '159985870458322944')
        self.assertEqual(result[1]['author_id'], '159985870458322945')

    def test_load_data_with_recompute(self):
        processed_data = [
            {
                "author_id": "159985870458322944",
                "date": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "source_id": "msg123",
                "metadata": {
                    "thread_id": "thread123",
                    "channel_id": "1088165451651092635",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter"
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": ["159985870458322945"],
                        "type": "emitter"
                    }
                ]
            },
            {
                "author_id": "159985870458322945",
                "date": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "source_id": "msg123",
                "metadata": {
                    "thread_id": "thread123",
                    "channel_id": "1088165451651092635",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "reply",
                        "type": "receiver"
                    }
                ],
                "interactions": []
            }
        ]

        self.collection.insert_many([
            {
                "author_id": "initial_user",
                "date": datetime(2023, 1, 1),
                "source_id": "initial_msg",
                "metadata": {
                    "thread_id": "initial_thread",
                    "channel_id": "initial_channel",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "initial_message",
                        "type": "emitter"
                    }
                ],
                "interactions": [
                    {
                        "name": "initial_reply",
                        "users_engaged_id": ["initial_user_engaged"],
                        "type": "receiver"
                    }
                ]
            }
        ])

        self.loader.load(processed_data, recompute=True)

        result = list(self.collection.find({}))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['author_id'], '159985870458322944')
        self.assertEqual(result[1]['author_id'], '159985870458322945')