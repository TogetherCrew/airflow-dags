import unittest
from datetime import datetime

from analyzer_helper.discord.discord_transform_raw_data import DiscordTransformRawData
from bson import ObjectId


class DiscordTransformRawDataUnitTest(unittest.TestCase):
    def test_create_interaction_base_valid_data(self):
        transformer = DiscordTransformRawData()
        interaction = transformer.create_interaction_base(
            name="reply",
            users_engaged_id=["user1234"],
            type="emitter"
        )
        self.assertEqual(
            interaction,
            {
                "name": "reply",
                "users_engaged_id": ["user1234"],
                "type": "emitter",
            },
        )

    def test_create_interaction_missing_arguments(self):
        transformer = DiscordTransformRawData()
        with self.assertRaises(TypeError):
            transformer.create_interaction_base(name="reply")

    def test_create_interaction_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "_id": ObjectId("649fc4dfb65f6981303e32ef"),
            "type": 0,
            "author": "user456",
            "content": "sample message",
            "user_mentions": ["user789"],
            "role_mentions": ["role1", "role2"],
            "reactions": ["user1, user2, :laugh:"],
            "replied_user": None,
            "createdDate": datetime(2024, 6, 11),
            "messageId": "12345",
            "channelId": "xyz",
            "channelName": "💬・general-chat",
            "threadId": "abc",
            "threadName": "thread-abc",
            "isGeneratedByWebhook": False,
        }
        interaction = transformer.create_interaction(
            data=data,
            name="reply",
            author="user456",
            engaged_users=["user789"],
            type="receiver",
        )
        self.assertEqual(
            interaction,
            {
                "author_id": "user456",
                "date": datetime(2024, 6, 11),
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
                ],
            },
        )

    def test_create_emitter_interaction_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "_id": ObjectId("649fc4dfb65f6981303e32ef"),
            "type": 0,
            "author": "user123",
            "content": "sample message",
            "user_mentions": ["user456"],
            "role_mentions": ["role1", "role2"],
            "reactions": ["user1, user2, :laugh:"],
            "replied_user": None,
            "createdDate": datetime(2024, 6, 10),
            "messageId": "56789",
            "channelId": "ghi",
            "channelName": "💬・general-chat",
            "threadId": "def",
            "threadName": "thread-def",
            "isGeneratedByWebhook": True,
        }
        interaction = transformer.create_interaction(
            data=data,
            name="reaction",
            author="user123",
            engaged_users=["user456"],
            type="emitter",
        )
        self.assertEqual(
            interaction,
            {
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
                ],
            },
        )

    def test_create_transformed_item_valid_data(self):
        transformer = DiscordTransformRawData()
        data = {
            "_id": ObjectId("649fc4dfb65f6981303e32ef"),
            "type": 0,
            "author": "user789",
            "content": "sample message",
            "user_mentions": ["user123"],
            "role_mentions": ["role1", "role2"],
            "reactions": ["user1, user2, :laugh:"],
            "replied_user": None,
            "createdDate": datetime(2024, 6, 11),
            "messageId": "90123",
            "channelId": "mno",
            "channelName": "💬・general-chat",
            "threadId": "jkl",
            "threadName": "thread-jkl",
            "isGeneratedByWebhook": False,
        }
        interactions = [
            {
                "name": "mention",
                "users_engaged_id": ["user123"],
                "type": "emitter",
            }
        ]
        transformed_item = transformer.create_transformed_item(
            data=data, period=datetime(2024, 6, 11), interactions=interactions
        )
        self.assertEqual(
            transformed_item,
            {
                "author_id": "user789",
                "date": datetime(2024, 6, 11),
                "source_id": "90123",
                "metadata": {
                    "thread_id": "jkl",
                    "channel_id": "mno",
                    "bot_activity": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": interactions,
            },
        )
