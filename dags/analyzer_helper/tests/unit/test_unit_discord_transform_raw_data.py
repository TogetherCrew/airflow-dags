from datetime import datetime
import unittest

from analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData


class DiscordTransformRawDataUnitTest(unittest.TestCase):

    def test_create_interaction_valid_data(self):
        transformer = DiscordTransformRawData()
        interaction = transformer.create_interaction("reply", ["user1234"], "emitter")
        self.assertEqual(interaction, {
            "name": "reply",
            "users_engaged_id": ["user1234"],
            "type": "emitter",
        })

    def test_create_interaction_missing_arguments(self):
        transformer = DiscordTransformRawData()
        with self.assertRaises(TypeError):
            transformer.create_interaction("reply")

    def test_create_receiver_interaction_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "createdDate": {"$date": "2024-06-11T00:00:00Z"},
            "messageId": "12345",
            "threadId": "abc",
            "channelId": "xyz",
            "isGeneratedByWebhook": False,
            "botActivity": False,
        }
        interaction = transformer.create_receiver_interaction(data, "reply", "user456", "user789")
        self.assertEqual(interaction, {
            "author_id": "user456",
            "date": "2024-06-11T00:00:00Z",
            "source_id": "12345",
            "metadata": {
                "thread_id": "abc",
                "channel_id": "xyz",
                "bot_activity": False,
            },
            "actions": [],
            "interactions": [
                {
                    "name": "reply",
                    "users_engaged_id": ["user789"],
                    "type": "receiver",
                }
            ]
        })

    def test_create_emitter_interaction_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "messageId": "56789",
            "threadId": "def",
            "channelId": "ghi",
            "isGeneratedByWebhook": True,
            "botActivity": True,
        }
        interaction = transformer.create_emitter_interaction("user123", datetime(2024, 6, 10), data, "reaction", "user456")
        self.assertEqual(interaction, {
            "author_id": "user123",
            "date": datetime(2024, 6, 10),
            "source_id": "56789",
            "metadata": {
                "thread_id": "def",
                "channel_id": "ghi",
                "bot_activity": True,
            },
            "actions": [],
            "interactions": [
                {
                    "name": "reaction",
                    "users_engaged_id": ["user456"],
                    "type": "emitter",
                }
            ]
        })

    def test_create_transformed_item_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "author": "user789",
            "messageId": "90123",
            "threadId": "jkl",
            "channelId": "mno",
            "isGeneratedByWebhook": False,
            "botActivity": False,
        }
        interactions = [
            {
                "name": "mention",
                "users_engaged_id": ["user123"],
                "type": "emitter",
            }
        ]
        transformed_item = transformer.create_transformed_item(data, datetime(2024, 6, 11), interactions)
        self.assertEqual(transformed_item, {
            "author_id": "user789",
            "date": datetime(2024, 6, 11),
            "source_id": "90123",
            "metadata": {
                "thread_id": "jkl",
                "channel_id": "mno",
                "bot_activity": False,
            },
            "actions": [],
            "interactions": interactions
        })
