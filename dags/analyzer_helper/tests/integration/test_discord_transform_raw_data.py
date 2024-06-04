import unittest
from datetime import datetime

from dags.analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData


class TestDiscordTransformRawData(unittest.TestCase):

    def setUp(self):
        self.transformer = DiscordTransformRawData()
        self.platform_id = 'discord_platform'
        self.period = datetime(2023, 1, 1)

    def test_transform_data_with_replied(self):
        raw_data = [
            {
                "author": "user123",
                "replied": "user789",
                "messageId": "msg123",
                "channelId": "channel456",
                "channelName": "general",
                "threadId": "thread123",
                "threadName": "discussion",
                "isGeneratedByWebhook": False
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": self.platform_id,
                "metadata": {
                    "channel_id": "channel456",
                    "channel_name": "general",
                    "thread_id": "thread123",
                    "thread_name": "discussion",
                    "message_id": "msg123",
                    "is_generated_by_webhook": False
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
                "channelName": "general",
                "threadId": "thread123",
                "threadName": "discussion",
                "isGeneratedByWebhook": False
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": self.platform_id,
                "metadata": {
                    "channel_id": "channel456",
                    "channel_name": "general",
                    "thread_id": "thread123",
                    "thread_name": "discussion",
                    "message_id": "msg123",
                    "is_generated_by_webhook": False
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
                "channelName": "general",
                "threadId": "thread123",
                "threadName": "discussion",
                "isGeneratedByWebhook": False
            }
        ]

        expected_result = [
            {
                "author_id": "user123",
                "date": self.period,
                "source_id": self.platform_id,
                "metadata": {
                    "channel_id": "channel456",
                    "channel_name": "general",
                    "thread_id": "thread123",
                    "thread_name": "discussion",
                    "message_id": "msg123",
                    "is_generated_by_webhook": False
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
                        "type": "emitter"
                    }
                ]
            }
        ]

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)

    def test_transform_data_empty(self):
        raw_data = []

        expected_result = []

        result = self.transformer.transform(raw_data, self.platform_id, self.period)
        self.assertEqual(result, expected_result)
