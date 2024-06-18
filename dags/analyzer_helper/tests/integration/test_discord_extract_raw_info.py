import unittest
from datetime import datetime

from analyzer_helper.discord.discord_extract_raw_infos import DiscordExtractRawInfos
from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawInfos(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.guild_id = "discord_guild_id"
        self.platform_id = "platform_db"
        self.guild_db = self.client[self.guild_id]
        self.platform_db = self.client[self.platform_id]
        self.rawinfos_collection = self.guild_db["rawinfo"]
        self.rawinfos_collection.delete_many({})

    def test_extract_recompute_true(self):
        sample_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ef"),
                "type": 0,
                "author": "111111111111111111",
                "content": "sample message",
                "user_mentions": ["63632723832823823", "83279873210490238"],
                "role_mentions": ["873892901809120912", "897234876127365121"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "888888888888888888",
                "channelId": "999999999999999999",
                "channelName": "💬・general-chat",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        ]
        self.rawinfos_collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos(self.guild_id, self.platform_id)
        result = extractor.extract(datetime(2023, 6, 30), recompute=True)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        rawmember_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ef"),
                "type": 0,
                "author": "159985870458322944",
                "content": "sample member activity",
                "user_mentions": ["63632723832823823", "83279873210490238"],
                "role_mentions": ["873892901809120912", "897234876127365121"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 29, 20, 28, 3, 494000),
                "messageId": "rawmemberactivities_source_id",
                "channelId": "some_channel_id",
                "channelName": "💬・general-chat",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        ]
        self.rawinfos_collection.insert_many(rawmember_data)

        sample_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ef"),
                "type": 0,
                "author": "111111111111111111",
                "content": "sample message",
                "user_mentions": ["63632723832823823", "83279873210490238"],
                "role_mentions": ["873892901809120912", "897234876127365121"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "888888888888888888",
                "channelId": "999999999999999999",
                "channelName": "💬・general-chat",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        ]
        self.rawinfos_collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos(self.guild_id, self.platform_id)
        result = extractor.extract(datetime(2023, 6, 30), recompute=False)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_empty_data(self):
        extractor = DiscordExtractRawInfos(self.guild_id, self.platform_id)
        result = extractor.extract(datetime(2023, 1, 1), recompute=False)

        expected_result = []

        self.assertEqual(result, expected_result)
