import unittest
from datetime import datetime

from analyzer_helper.discord.discord_extract_raw_members import DiscordExtractRawMembers
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordExtractRawMembers(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.guild_id = "discord_guild"
        self.platform_id = "platform_db"
        self.guild_db = self.client[self.guild_id]
        self.platform_db = self.client[self.platform_id]
        self.guild_collection = self.guild_db["guildmembers"]
        self.rawmembers_collection = self.platform_db["rawmembers"]
        self.guild_collection.delete_many({})
        self.rawmembers_collection.delete_many({})

    def tearDown(self):
        self.guild_collection.delete_many({})
        self.rawmembers_collection.delete_many({})

    def test_extract_recompute_true(self):
        sample_data = [
            {
                "discordId": "159985870458322944",
                "username": "MEE6",
                "roles": ["1088165451651092635", "1046009276432400435"],
                "joinedAt": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "avatar": "b50adff099924dd5e6b72d13f77eb9d7",
                "isBot": True,
                "discriminator": "4876",
                "permissions": "559642693856991",
                "deletedAt": None,
                "globalName": None,
                "nickname": None,
            },
            {
                "discordId": "159985870458322945",
                "username": "TestUser",
                "roles": ["1088165451651092636", "1046009276432400436"],
                "joinedAt": datetime(2023, 3, 23, 18, 21, 0, 870000),
                "avatar": "a50adff099924dd5e6b72d13f77eb9d8",
                "isBot": False,
                "discriminator": "4877",
                "permissions": "559642693856992",
                "deletedAt": None,
                "globalName": "GlobalTestUser",
                "nickname": "TestNick",
            },
        ]
        self.guild_collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.guild_id, self.platform_id)
        result = extractor.extract(recompute=True)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

    def test_extract_recompute_false(self):
        rawmember_data = [
            {
                "id": "159985870458322944",
                # "username": "MEE6",
                # "roles": ["1088165451651092635", "1046009276432400435"],
                "joined_at": datetime(2023, 3, 22, 18, 21, 0, 870000),
                # "avatar": "b50adff099924dd5e6b72d13f77eb9d7",
                "is_bot": True,
                # "discriminator": "4876",
                # "permissions": "559642693856991",
                "left_at": None,
                # "globalName": None,
                # "nickname": None,
            }
        ]
        self.rawmembers_collection.insert_many(rawmember_data)

        sample_data = [
            {
                "discordId": "159985870458322945",
                "username": "TestUser",
                "roles": ["1088165451651092636", "1046009276432400436"],
                "joinedAt": datetime(2023, 3, 23, 18, 21, 0, 870000),
                "avatar": "a50adff099924dd5e6b72d13f77eb9d8",
                "isBot": False,
                "discriminator": "4877",
                "permissions": "559642693856992",
                "deletedAt": None,
                "globalName": "GlobalTestUser",
                "nickname": "TestNick",
            },
        ]
        self.guild_collection.insert_many(sample_data)

        extractor = DiscordExtractRawMembers(self.guild_id, self.platform_id)
        result = extractor.extract(recompute=False)

        expected_result = sample_data

        self.assertEqual(result, expected_result)

        new_data = {
            "discordId": "159985870458322946",
            "username": "NewUser",
            "roles": ["1088165451651092637", "1046009276432400437"],
            "joinedAt": datetime(2023, 3, 24, 18, 21, 0, 870000),
            "avatar": "c50adff099924dd5e6b72d13f77eb9d9",
            "isBot": False,
            "discriminator": "4878",
            "permissions": "559642693856993",
            "deletedAt": None,
            "globalName": "GlobalNewUser",
            "nickname": "NewNick",
        }
        self.guild_collection.insert_one(new_data)

        result = extractor.extract(recompute=False)
        expected_result = [new_data]

        self.assertEqual(result, expected_result)
