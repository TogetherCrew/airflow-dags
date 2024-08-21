import unittest
import datetime

from analyzer_helper.telegram.transform_raw_members import TransformRawMembers


class TestTelegramTransformRawMembers(unittest.TestCase):
    def setUp(self):
        self.transformer = TransformRawMembers()

    def test_transform_empty_list(self):
        """
        Tests that transform returns an empty list for an empty members list
        """
        result = self.transformer.transform([])
        self.assertEqual(result, [])

    def test_transform_single_member(self):
        """
        Tests that transform correctly transforms a single member
        """
        raw_member = {
            "id": 203678862.0,
            "is_bot": False,
            "joined_at": 1713038774.0,
            "left_at": None,
        }

        expected_result = [
            {
                "id": "203678862.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 13, 20, 6, 14, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            }
        ]

        result = self.transformer.transform([raw_member])
        self.assertEqual(result, expected_result)

    def test_transform_multiple_members(self):
        """
        Tests that transform correctly transforms multiple members
        """
        raw_members = [
            {
                "id": 203678862.0,
                "is_bot": False,
                "joined_at": 1713038774.0,
                "left_at": None,
            },
            {
                "id": 265278326.0,
                "is_bot": False,
                "joined_at": 1713161415.0,
                "left_at": None,
            },
            {
                "id": 501641383.0,
                "is_bot": False,
                "joined_at": 1713038805.0,
                "left_at": None,
            },
            {
                "id": 551595722.0,
                "is_bot": False,
                "joined_at": 1713047141.0,
                "left_at": None,
            },
            {
                "id": 926245054.0,
                "is_bot": False,
                "joined_at": 1713178099.0,
                "left_at": None,
            },
            {"id": 927814807.0, "is_bot": False, "joined_at": 0.0, "left_at": None},
            {"id": 6504405389.0, "is_bot": False, "joined_at": 0.0, "left_at": None},
        ]

        expected_result = [
            {
                "id": "203678862.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 13, 20, 6, 14, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            },
            {
                "id": "265278326.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 15, 6, 10, 15, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            },
            {
                "id": "501641383.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 13, 20, 6, 45, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            },
            {
                "id": "551595722.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 13, 22, 25, 41, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            },
            {
                "id": "926245054.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(
                    2024, 4, 15, 10, 48, 19, tzinfo=datetime.timezone.utc
                ),
                "options": {},
            },
            {
                "id": "927814807.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": None,
                "options": {},
            },
            {
                "id": "6504405389.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": None,
                "options": {},
            },
        ]

        result = self.transformer.transform(raw_members)
        print("result: \n", result)
        print("expceted result: \n", expected_result)
        self.assertEqual(result, expected_result)
