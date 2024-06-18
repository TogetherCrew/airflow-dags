import unittest
from datetime import datetime
from bson import ObjectId
from analyzer_helper.discord.discord_extract_raw_infos import DiscordExtractRawInfos
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton

class TestDiscordExtractRawInfos(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.guild_id = "discord_guild_id"
        self.platform_id = "platform_db"
        self.guild_db = self.client[self.guild_id]
        self.platform_db = self.client[self.platform_id]
        self.rawinfos_collection = self.guild_db["rawinfos"]
        self.rawinfos_collection.delete_many({})

    def test_extract_recompute_true(self):
        sample_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ef"),
                "type": 0,
                "author": "DUMMY_DISCORD_ID_1",
                "content": "sample message",
                "user_mentions": ["DUMMY_DISCORD_ID_2", "DUMMY_DISCORD_ID_3"],
                "role_mentions": ["DUMMY_ROLE_ID_1", "DUMMY_ROLE_ID_2"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "DUMMY_MESSAGE_ID_1",
                "channelId": "DUMMY_CHANNEL_ID_1",
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

        print("Result: ", result)
        print("Expected: ", expected_result)
        # print("Difference: ", set([str(doc['_id']) for doc in result]) - set([str(doc['_id']) for doc in expected_result]))

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        rawmember_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ef"),
                "type": 0,
                "author": "DUMMY_DISCORD_ID_4",
                "content": "sample member activity",
                "user_mentions": ["DUMMY_DISCORD_ID_5"],
                "role_mentions": ["DUMMY_ROLE_ID_1", "DUMMY_ROLE_ID_2"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 29, 20, 28, 3, 494000),
                "messageId": "DUMMY_MESSAGE_ID_2",
                "channelId": "DUMMY_CHANNEL_ID_2",
                "channelName": "💬・general-chat",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        ]
        self.rawinfos_collection.insert_many(rawmember_data)

        sample_data = [
            {
                "_id": ObjectId("649fc4dfb65f6981303e32ex"),
                "type": 0,
                "author": "DUMMY_DISCORD_ID_1",
                "content": "sample message",
                "user_mentions": ["DUMMY_DISCORD_ID_2", "DUMMY_DISCORD_ID_3"],
                "role_mentions": ["DUMMY_ROLE_ID_1", "DUMMY_ROLE_ID_2"],
                "reactions": [
                    "44444444444444444,thelounge",
                    "91919191919191919,6373687382748239,👌",
                ],
                "replied_user": None,
                "createdDate": datetime(2023, 6, 30, 20, 28, 3, 494000),
                "messageId": "DUMMY_MESSAGE_ID_1",
                "channelId": "DUMMY_CHANNEL_ID_1",
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
