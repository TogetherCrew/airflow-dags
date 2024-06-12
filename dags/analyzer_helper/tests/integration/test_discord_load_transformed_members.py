import unittest
from datetime import datetime

from analyzer_helper.discord.discord_load_transformed_members import (
    DiscordLoadTransformedMembers,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordLoadTransformedMembers(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client["discord_platform"]
        self.platform_id = "discord"
        self.collection = self.db["rawmembers"]
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_load_recompute_true(self):
        """
        Tests that load replaces all existing data when recompute is True
        """
        initial_data = [
            {
                "id": 1,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 1, 1),
                "options": {},
            },
            {
                "id": 2,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 1, 2),
                "options": {},
            },
        ]
        self.collection.insert_many(initial_data)

        processed_data = [
            {
                "id": 3,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 1, 3),
                "options": {},
            },
            {
                "id": 4,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 1, 4),
                "options": {},
            },
        ]
        loader = DiscordLoadTransformedMembers(self.platform_id)

        loader.load(processed_data, recompute=True)

        # Verify that the collection is replaced with the processed data
        result = list(
            self.collection.find({}, {"_id": False})
        )  # Exclude '_id' field for comparison
        self.assertEqual(result, processed_data)

    def test_load_recompute_false(self):
        """
        Tests that load inserts new data when recompute is False
        """
        initial_data = [
            {
                "id": 1,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 1, 1),
                "options": {},
            },
            {
                "id": 2,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 1, 2),
                "options": {},
            },
        ]
        self.collection.insert_many(initial_data)

        processed_data = [
            {
                "id": 3,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 1, 3),
                "options": {},
            },
            {
                "id": 4,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 1, 4),
                "options": {},
            },
        ]
        loader = DiscordLoadTransformedMembers(self.platform_id)

        loader.load(processed_data, recompute=False)

        # Verify that the new data is inserted along with the existing data
        result = list(
            self.collection.find({}, {"_id": False})
        )  # Exclude '_id' field for comparison
        expected_result = initial_data + processed_data
        self.assertEqual(result, expected_result)

