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
            'id': 203678862.0,
            'is_bot': False,
            'joined_at': 1713038774.0,
            'left_at': None
        }

        expected_result = [
            {
                "id": "203678862.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 14, 6, 12, 54),  # Use the direct datetime format
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
            {'id': 203678862.0, 'is_bot': False, 'joined_at': 1713038774.0, 'left_at': None},
            {'id': 265278326.0, 'is_bot': False, 'joined_at': 1713161415.0, 'left_at': None},
            {'id': 501641383.0, 'is_bot': False, 'joined_at': 1713038805.0, 'left_at': None},
            {'id': 551595722.0, 'is_bot': False, 'joined_at': 1713047141.0, 'left_at': None},
            {'id': 926245054.0, 'is_bot': False, 'joined_at': 1713178099.0, 'left_at': None},
            {'id': 927814807.0, 'is_bot': False, 'joined_at': 0.0, 'left_at': None},
            {'id': 6504405389.0, 'is_bot': False, 'joined_at': 0.0, 'left_at': None}
        ]

        expected_result = [
            {
                "id": "203678862.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 14, 6, 12, 54),
                "options": {},
            },
            {
                "id": "265278326.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 15, 12, 16, 55),
                "options": {},
            },
            {
                "id": "501641383.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 14, 6, 13, 25),
                "options": {},
            },
            {
                "id": "551595722.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 14, 8, 5, 41),
                "options": {},
            },
            {
                "id": "926245054.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(2024, 4, 15, 17, 8, 19),
                "options": {},
            },
            {
                "id": "927814807.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(1970, 1, 1, 0, 0),
                "options": {},
            },
            {
                "id": "6504405389.0",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime.datetime(1970, 1, 1, 0, 0),
                "options": {},
            }
        ]

        result = self.transformer.transform(raw_members)
        self.assertEqual(result, expected_result)
