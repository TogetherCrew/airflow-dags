import unittest
from datetime import datetime

from analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData


class TestDiscordTransformRawData(unittest.TestCase):

    def setUp(self):
        self.transformer = DiscordTransformRawData()
        self.platform_id = 'discord_platform1'
        self.period = datetime(2023, 1, 1)

    def test_transform_data_with_replied_user(self):
        raw_data = [
            {
                "author": "user123",
                "replied_user": "user789",
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "botActivity": False,
                "threadId": "thread123"
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
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
                        "users_engaged_id": ["user789"],
                        "type": "emitter"
                    }
                ]
            },
            {
                "author_id": "user789",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": ["user123"],
                        "type": "receiver"
                    }
                ]
            }
        ]

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)

    def test_transform_data_with_user_mentions(self):
        raw_data = [
            {
                "author": "user123",
                "user_mentions": ["user456"],
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "botActivity": False,
                "threadId": "thread123"
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
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
                        "name": "mention",
                        "users_engaged_id": ["user456"],
                        "type": "emitter"
                    }
                ]
            }
        ]

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)

    def test_transform_data_with_reactions(self):
        raw_data = [
            {
                "author": "user123",
                "reactions": ["user1, user2, :laugh:"],
                "messageId": "msg123",
                "channelId": "channel456",
                "isGeneratedByWebhook": False,
                "botActivity": False,
                "threadId": "thread123"
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
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
                        "name": "reaction",
                        "users_engaged_id": ["user1", "user2"],
                        "type": "receiver"
                    }
                ]
            },
            {
                "author_id": "user1",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "reaction",
                        "type": "emitter"
                    }
                ],
                "interactions": []
            },
            {
                "author_id": "user2",
                "date": self.period,
                "source_id": "msg123",
                "metadata": {
                    "channel_id": "channel456",
                    "thread_id": "thread123",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "reaction",
                        "type": "emitter"
                    }
                ],
                "interactions": []
            }
        ]

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)

    def test_transform_data_empty(self):
        raw_data = []

        expected_result = []

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)
