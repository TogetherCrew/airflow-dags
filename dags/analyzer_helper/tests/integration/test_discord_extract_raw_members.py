import unittest
from datetime import datetime

from analyzer_helper.discord.discord_extract_raw_members import DiscordExtractRawMembers
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawMembers(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.platform_id = 'discord_platform'
        self.guild_id = 'discord_guild'
        self.db = self.client[self.guild_id]
        self.collection = self.db['guildmembers']
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})
        self.client.close()

    def test_extract_recompute_true(self):
        sample_data = [
            {
                'discordId': '159985870458322944',
                'username': 'MEE6',
                'roles': ['1088165451651092635', '1046009276432400435'],
                'joinedAt': datetime(2023, 3, 22, 18, 21, 0, 870000),
                'avatar': 'b50adff099924dd5e6b72d13f77eb9d7',
                'isBot': True,
                'discriminator': '4876',
                'permissions': '559642693856991',
                'deletedAt': None,
                'globalName': None,
                'nickname': None,
            },
            {
                'discordId': '159985870458322945',
                'username': 'TestUser',
                'roles': ['1088165451651092636', '1046009276432400436'],
                'joinedAt': datetime(2023, 3, 23, 18, 21, 0, 870000),
                'avatar': 'a50adff099924dd5e6b72d13f77eb9d8',
                'isBot': False,
                'discriminator': '4877',
                'permissions': '559642693856992',
                'deletedAt': None,
                'globalName': 'GlobalTestUser',
                'nickname': 'TestNick',
            },
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.platform_id, self.guild_id)
        result = extractor.extract(recompute=True)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        sample_data = [
            {
                'discordId': '159985870458322944',
                'username': 'MEE6',
                'roles': ['1088165451651092635', '1046009276432400435'],
                'joinedAt': datetime(2023, 3, 22, 18, 21, 0, 870000),
                'avatar': 'b50adff099924dd5e6b72d13f77eb9d7',
                'isBot': True,
                'discriminator': '4876',
                'permissions': '559642693856991',
                'deletedAt': None,
                'globalName': None,
                'nickname': None,
            },
            {
                'discordId': '159985870458322945',
                'username': 'TestUser',
                'roles': ['1088165451651092636', '1046009276432400436'],
                'joinedAt': datetime(2023, 3, 23, 18, 21, 0, 870000),
                'avatar': 'a50adff099924dd5e6b72d13f77eb9d8',
                'isBot': False,
                'discriminator': '4877',
                'permissions': '559642693856992',
                'deletedAt': None,
                'globalName': 'GlobalTestUser',
                'nickname': 'TestNick',
            },
        ]
        self.collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.platform_id, self.guild_id)
        result = extractor.extract(recompute=False)

        expected_result = []

        self.assertEqual(result, expected_result)

        new_data = {
            'discordId': '159985870458322946',
            'username': 'NewUser',
            'roles': ['1088165451651092637', '1046009276432400437'],
            'joinedAt': datetime(2023, 3, 24, 18, 21, 0, 870000),
            'avatar': 'c50adff099924dd5e6b72d13f77eb9d9',
            'isBot': False,
            'discriminator': '4878',
            'permissions': '559642693856993',
            'deletedAt': None,
            'globalName': 'GlobalNewUser',
            'nickname': 'NewNick',
        }
        self.collection.insert_one(new_data)

        result = extractor.extract(recompute=False)
        expected_result = [new_data]

        self.assertEqual(result, expected_result)