import unittest

from hivemind_etl_helpers.src.db.discord.utils.sort_based_id import sort_based_on_id


class TestSortBasedOnId(unittest.TestCase):
    def test_sort_based_on_id(self):
        ids = ["1", "3", "2", "5", "4"]

        data = [
            {"discordId": "1", "name": "John Doe"},
            {"discordId": "2", "name": "Jane Doe"},
            {"discordId": "3", "name": "Alice"},
            {"discordId": "4", "name": "Bob"},
            {"discordId": "5", "name": "Charlie"},
        ]

        sorted_data = sort_based_on_id(ids, data, "discordId")

        expected_sorted_data = [
            {"discordId": "1", "name": "John Doe"},
            {"discordId": "3", "name": "Alice"},
            {"discordId": "2", "name": "Jane Doe"},
            {"discordId": "5", "name": "Charlie"},
            {"discordId": "4", "name": "Bob"},
        ]

        self.assertEqual(sorted_data, expected_sorted_data)

    def test_sort_based_on_id_empty_data(self):
        ids = ["1", "3", "2", "5", "4"]
        data = []

        sorted_data = sort_based_on_id(ids, data, "discordId")

        self.assertEqual(sorted_data, [])

    def test_sort_based_on_id_missing_id(self):
        ids = ["1", "3", "2", "5", "4"]

        data = [
            {"discordId": "1", "name": "John Doe"},
            {"discordId": "3", "name": "Alice"},
            {"discordId": "5", "name": "Charlie"},
        ]

        sorted_data = sort_based_on_id(ids, data, "discordId")

        expected_sorted_data = [
            {"discordId": "1", "name": "John Doe"},
            {"discordId": "3", "name": "Alice"},
            {"discordId": "5", "name": "Charlie"},
        ]

        self.assertEqual(sorted_data, expected_sorted_data)
