from datetime import datetime
import unittest

from analyzer_helper.discourse.transform_raw_members import (
    TransformRawMembers,
)

class TestDiscordTransformRawMembers(unittest.TestCase):

    def setUp(self):
        self.transformer = TransformRawMembers()
     
    def test_transform_empty_list(self):
        """
        Tests that transform returns an empty list for an empty members list
        """
        transformer = TransformRawMembers()
        result = transformer.transform([])
        self.assertEqual(result, [])

    def test_transform_single_member(self):
        """
        Tests that transform correctly transforms a single member
        """
        transformer = TransformRawMembers()
        raw_member = {
            "id": 85149,
            "joined_at": "2023-07-02",
        }

        expected_result = [
            {
                "id": "85149",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 7, 2, 0, 0, 0, 0),
                "options": {},
            }
        ]

        result = transformer.transform([raw_member])
        self.assertEqual(result, expected_result)

    def test_transform_multiple_members(self):
        """
        Tests that transform correctly transforms multiple members
        """
        transformer = TransformRawMembers()
        raw_member1 = {
            "id": "85159",
            "avatar": "avatar1",
            "joined_at": "2023-07-01",
            "badgeIds": ["badge1"],
        }
        
        raw_member2 = {
            "id": "85161",
            "avatar": "avatar2",
            "joined_at": "2023-07-02",
            "badgeIds": ["badge2"],
        }

        expected_result1 = {
            "id": "85159",
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 7, 1, 0, 0, 0, 0),
            "options": {},
        }

        expected_result2 = {
            "id": "85161",
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 7, 2, 0, 0, 0, 0),
            "options": {},
        }

        result = transformer.transform([raw_member1, raw_member2])
        self.assertEqual(result, [expected_result1, expected_result2])