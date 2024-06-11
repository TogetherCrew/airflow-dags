import unittest
from datetime import datetime
from bson import ObjectId
from analyzer_helper.discord.discord_extract_raw_infos import DiscordExtractRawInfos
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawInfos(unittest.TestCase):

    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client['discord']
        self.collection = self.db['rawinfos']
        self.collection.delete_many({})

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_extract_recompute_true(self):
        sample_data = [
            {
                '_id': ObjectId("649fc4dfb65f6981303e32ef"),
                'type': 0,
                'author': '111111111111111111',
                'content': 'sample message',
                'user_mentions': ['63632723832823823', '83279873210490238'],
                'role_mentions': ['873892901809120912', '897234876127365121'],
                'reactions': ['44444444444444444,thelounge', '91919191919191919,6373687382748239,👌'],
                'replied_user': None,
                'createdDate': datetime(2023, 6, 30, 20, 28, 3, 494000),
                'messageId': '888888888888888888',
                'channelId': '999999999999999999',
                'channelName': '💬・general-chat',
                'threadId': None,
                'threadName': None,
                'isGeneratedByWebhook': False
            }
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos('discord')
        result = extractor.extract(datetime(2023, 6, 30), recompute=True)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        sample_data = [
            {
                '_id': ObjectId("649fc4dfb65f6981303e32ef"),
                'type': 0,
                'author': '111111111111111111',
                'content': 'sample message',
                'user_mentions': ['63632723832823823', '83279873210490238'],
                'role_mentions': ['873892901809120912', '897234876127365121'],
                'reactions': ['44444444444444444,thelounge', '91919191919191919,6373687382748239,👌'],
                'replied_user': None,
                'createdDate': datetime(2023, 6, 30, 20, 28, 3, 494000),
                'messageId': '888888888888888888',
                'channelId': '999999999999999999',
                'channelName': '💬・general-chat',
                'threadId': None,
                'threadName': None,
                'isGeneratedByWebhook': False
            }
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawInfos('discord')
        result = extractor.extract(datetime(2023, 6, 30), recompute=False)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_empty_data(self):
        extractor = DiscordExtractRawInfos('discord')
        result = extractor.extract(datetime(2023, 1, 1), recompute=False)

        expected_result = []

        self.assertEqual(result, expected_result)
