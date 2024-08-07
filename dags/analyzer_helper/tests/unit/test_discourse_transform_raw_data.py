import datetime
import unittest

from analyzer_helper.discourse.transform_raw_data import TransformRawInfo


class TestTransformRawInfo(unittest.TestCase):
    def setUp(self):
        """Initialize the TransformRawInfo instance before each test."""
        self.platform_id = "test_platform"
        self.forum_endpoint = "sample.endpoint.gov"
        self.transformer = TransformRawInfo(forum_endpoint=self.forum_endpoint)

    def test_create_data_entry_no_interaction(self):
        """Test data entry creation with no specific interaction type."""
        raw_data = {
            "post_id": 6262,
            "author_id": 6168,
            "created_at": "2023-09-11T21:41:43.553Z",
            "text": "some content",
            "post_number": 1.0,
            "category_id": 500,
            "topic_id": 6134,
            "reactions": [],
            "replied_post_id": None,
        }
        result = self.transformer.create_data_entry(raw_data)
        self.assertEqual(result["author_id"], str(raw_data["author_id"]))
        self.assertIsInstance(result["date"], datetime.datetime)
        self.assertFalse(result["metadata"]["bot_activity"])
        self.assertEqual(
            result["metadata"]["link"],
            f"https://{self.forum_endpoint}/t/6134/1",
        )
        self.assertEqual(result["text"], "some content")
        self.assertEqual(len(result["interactions"]), 0)
        self.assertEqual(result["source_id"], str(raw_data["post_id"]))
        self.assertEqual(result["metadata"]["category_id"], raw_data["category_id"])
        self.assertEqual(result["metadata"]["topic_id"], raw_data["topic_id"])
        self.assertEqual(len(result["actions"]), 1)
        self.assertEqual(result["actions"][0]["name"], "message")
        self.assertEqual(result["actions"][0]["type"], "emitter")

    def test_create_data_entry_with_reaction(self):
        """Test data entry creation for a reaction interaction."""
        raw_data = {
            "post_id": 6261,
            "author_id": 6168,
            "created_at": "2023-09-11T21:42:43.553Z",
            "category_id": 500,
            "topic_id": 6134,
            "reactions": [6263],
            "replied_post_id": None,
            "text": "some content #2",
            "post_number": 1.0,
        }
        result = self.transformer.create_data_entry(
            raw_data, interaction_type="reaction", interaction_user=6263
        )
        expected_result = {
            "actions": [],
            "author_id": "6263",
            "date": datetime.datetime(
                2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
            ),
            "text": "some content #2",
            "interactions": [
                {
                    "name": "reaction",
                    "type": "emitter",
                    "users_engaged_id": ["6168"],
                }
            ],
            "metadata": {
                "category_id": 500,
                "topic_id": 6134,
                "bot_activity": False,
                "link": f"https://{self.forum_endpoint}/t/6134/1",
            },
            "source_id": "6261",
        }
        self.assertEqual(result, expected_result)

    def test_transform_data_with_replied_user(self):
        raw_data = [
            {
                "post_id": 6262,
                "author_id": 6168,
                "created_at": "2023-09-11T21:41:43.553Z",
                "author_name": "Test Author Name1",
                "reactions": [],
                "replied_post_id": 6512,
                "replied_post_user_id": 4444,
                "topic_id": 6134,
                "text": "some content #2",
                "post_number": 1.0,
            }
        ]

        expected_result = [
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 41, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #2",
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["4444"],
                    }
                ],
            },
            {
                "author_id": "4444",
                "text": "some content #2",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 41, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["6168"],
                    }
                ],
            },
        ]

        result = self.transformer.transform(
            raw_data=raw_data,
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_with_reactions(self):
        raw_data = [
            {
                "post_id": 6261,
                "author_id": 6168,
                "created_at": "2023-09-11T21:42:43.553Z",
                "author_name": "Test Author Name2",
                "reactions": [1, 2],
                "replied_post_id": None,
                "replied_post_user_id": None,
                "topic_id": 6134,
                "text": "some content #2",
                "post_number": 1.0,
            }
        ]
        expected_result = [
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #2",
                "source_id": "6261",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "receiver",
                        "users_engaged_id": ["1", "2"],
                    }
                ],
            },
            {
                "actions": [],
                "author_id": "1",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #2",
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "emitter",
                        "users_engaged_id": ["6168"],
                    }
                ],
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "source_id": "6261",
            },
            {
                "actions": [],
                "author_id": "2",
                "text": "some content #2",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "emitter",
                        "users_engaged_id": ["6168"],
                    }
                ],
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "source_id": "6261",
            },
        ]

        result = self.transformer.transform(
            raw_data=raw_data,
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_replied_and_reactions(self):
        raw_data = [
            {
                "post_id": 6262,
                "author_id": 6168,
                "created_at": "2023-09-11T21:41:43.553Z",
                "author_name": "Test Author Name1",
                "reactions": [],
                "replied_post_id": 6512,
                "replied_post_user_id": 4444,
                "topic_id": 6134,
                "text": "some content #1",
                "post_number": 1.0,
            },
            {
                "post_id": 6261,
                "author_id": 6168,
                "created_at": "2023-09-11T21:42:43.553Z",
                "author_name": "Test Author Name2",
                "reactions": [1, 2],
                "replied_post_id": None,
                "replied_post_user_id": None,
                "topic_id": 6134,
                "text": "some content #2",
                "post_number": 2.0,
            },
        ]

        expected_result = [
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 41, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #1",
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["4444"],
                    }
                ],
            },
            {
                "author_id": "4444",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 41, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #1",
                "source_id": "6262",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/1",
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["6168"],
                    }
                ],
            },
            {
                "author_id": "6168",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "text": "some content #2",
                "source_id": "6261",
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/2",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "receiver",
                        "users_engaged_id": ["1", "2"],
                    }
                ],
            },
            {
                "actions": [],
                "author_id": "1",
                "text": "some content #2",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "emitter",
                        "users_engaged_id": ["6168"],
                    }
                ],
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/2",
                },
                "source_id": "6261",
            },
            {
                "actions": [],
                "author_id": "2",
                "text": "some content #2",
                "date": datetime.datetime(
                    2023, 9, 11, 21, 42, 43, 553000, tzinfo=datetime.timezone.utc
                ),
                "interactions": [
                    {
                        "name": "reaction",
                        "type": "emitter",
                        "users_engaged_id": ["6168"],
                    }
                ],
                "metadata": {
                    "category_id": None,
                    "topic_id": 6134,
                    "bot_activity": False,
                    "link": f"https://{self.forum_endpoint}/t/6134/2",
                },
                "source_id": "6261",
            },
        ]
        result = self.transformer.transform(
            raw_data=raw_data,
        )
        self.assertEqual(result, expected_result)

    def test_transform_data_empty(self):
        raw_data = []

        expected_result = []

        result = self.transformer.transform(
            raw_data=raw_data,
        )
        self.assertEqual(result, expected_result)
