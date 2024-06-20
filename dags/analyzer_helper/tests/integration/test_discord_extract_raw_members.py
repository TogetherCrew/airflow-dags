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
                "discordId": "DUMMY_DISCORD_ID_1",
                "username": "MEE6",
                "roles": ["DUMMY_ROLE_ID_1", "DUMMY_ROLE_ID_2"],
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
                "discordId": "DUMMY_DISCORD_ID_2",
                "username": "TestUser",
                "roles": ["DUMMY_ROLE_ID_3", "DUMMY_ROLE_ID_4"],
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
                "id": "DUMMY_DISCORD_ID_1",
                "joined_at": datetime(2023, 3, 22, 18, 21, 0, 870000),
                "is_bot": True,
                "left_at": None,
            }
        ]
        self.rawmembers_collection.insert_many(rawmember_data)

        sample_data = [
            {
                "discordId": "DUMMY_DISCORD_ID_2",
                "username": "TestUser",
                "roles": ["DUMMY_ROLE_ID_3", "DUMMY_ROLE_ID_4"],
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

        expected_result = [sample_data[0]]

        self.assertEqual(result, expected_result)

        new_data = {
            "discordId": "DUMMY_DISCORD_ID_3",
            "username": "NewUser",
            "roles": ["DUMMY_ROLE_ID_5", "DUMMY_ROLE_ID_6"],
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

        self.assertEqual(result, expected_result)

        result = extractor.extract(recompute=False)
        expected_result = [sample_data[0], new_data]

        self.assertEqual(result, expected_result)
