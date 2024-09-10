import unittest
from datetime import datetime, timezone

from analyzer_helper.discourse.transform_raw_members import TransformRawMembers


class TestDiscordTransformRawMembers(unittest.TestCase):
    def setUp(self):
        self.endpoint = "sample.endpoint.gov"
        self.transformer = TransformRawMembers(endpoint=self.endpoint)

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
            "id": 85149,
            "joined_at": "2023-07-02",
            "isBot": False,
            "name": "member1",
            "username": "memberUser1",
            "avatar": "/path1/endpoint",
        }

        expected_result = [
            {
                "id": "85149",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 7, 2, 0, 0, 0, 0, tzinfo=timezone.utc),
                "options": {
                    "name": "member1",
                    "username": "memberUser1",
                    "avatar": "https://" + self.endpoint + "/path1/endpoint",
                },
            }
        ]

        result = self.transformer.transform([raw_member])
        self.assertEqual(result, expected_result)

    def test_transform_multiple_members(self):
        """
        Tests that transform correctly transforms multiple members
        """
        raw_member1 = {
            "id": "85159",
            "joined_at": "2023-07-01",
            "isBot": True,
            "name": "member1",
            "username": "memberUser1",
            "avatar": "https://cdn.com/path1/endpoint",
        }
        raw_member2 = {
            "id": "85161",
            "joined_at": "2023-07-02",
            "name": "member2",
            "username": "memberUser2",
            "avatar": "/path1/endpoint2",
        }
        expected_result1 = {
            "id": "85159",
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
            "options": {
                "name": "member1",
                "username": "memberUser1",
                "avatar": "https://cdn.com/path1/endpoint",
            },
        }
        expected_result2 = {
            "id": "85161",
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 7, 2, 0, 0, 0, 0, tzinfo=timezone.utc),
            "options": {
                "name": "member2",
                "username": "memberUser2",
                "avatar": "https://" + self.endpoint + "/path1/endpoint2",
            },
        }

        result = self.transformer.transform([raw_member1, raw_member2])
        self.assertEqual(result, [expected_result1, expected_result2])
