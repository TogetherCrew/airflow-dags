from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from violation_detection_helpers import ExtractPlatformRawData


class TestExtractRawData(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.platform_id = "51515151515151"

        self.client.drop_database(self.platform_id)

    def tearDown(self) -> None:
        self.client.drop_database(self.platform_id)

    def test_extract_all_resources(self):
        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "test_test",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
            {
                "author_id": "2",
                "date": datetime(2022, 1, 1),
                "source_id": "8880",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34569",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
        ]
        self.client[self.platform_id]["rawmemberactivities"].insert_many(sample_data)
        extract_data = ExtractPlatformRawData(self.platform_id, "channel_id")

        cursor, override_recompute = extract_data.extract(
            from_date=datetime(2020, 1, 1),
            to_date=None,
            resources=["8888", "8880"],
            recompute=False,
        )
        results = list(cursor)

        self.assertEqual(len(results), 2)
        self.assertEqual(results, sample_data)
        self.assertTrue(override_recompute)

    def test_extract_some_resources(self):
        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "test_test",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
            {
                "author_id": "2",
                "date": datetime(2022, 1, 1),
                "source_id": "8880",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34569",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
        ]
        self.client[self.platform_id]["rawmemberactivities"].insert_many(sample_data)
        extract_data = ExtractPlatformRawData(self.platform_id, "channel_id")

        cursor, override_recompute = extract_data.extract(
            from_date=datetime(2020, 1, 1),
            to_date=None,
            resources=["8888"],
            recompute=False,
        )
        results = list(cursor)

        self.assertEqual(len(results), 1)
        self.assertEqual(results, [sample_data[0]])
        self.assertTrue(override_recompute)

    def test_extract_no_data_date_filtered(self):
        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "test_test",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
            {
                "author_id": "2",
                "date": datetime(2022, 1, 1),
                "source_id": "8880",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34569",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
        ]
        self.client[self.platform_id]["rawmemberactivities"].insert_many(sample_data)
        extract_data = ExtractPlatformRawData(self.platform_id, "channel_id")

        cursor, override_recompute = extract_data.extract(
            from_date=datetime(2023, 1, 1),
            to_date=None,
            resources=["8888"],
            recompute=False,
        )
        results = list(cursor)

        self.assertEqual(len(results), 0)
        self.assertTrue(override_recompute)

    def test_extract_some_with_to_date(self):
        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2023, 1, 1),
                "source_id": "8888",
                "text": "test_test",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
            {
                "author_id": "2",
                "date": datetime(2023, 3, 1),
                "source_id": "8880",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34569",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
        ]
        self.client[self.platform_id]["rawmemberactivities"].insert_many(sample_data)
        extract_data = ExtractPlatformRawData(self.platform_id, "channel_id")

        cursor, override_recompute = extract_data.extract(
            from_date=datetime(2022, 1, 1),
            to_date=datetime(2023, 2, 1),
            resources=["8888"],
            recompute=False,
        )
        results = list(cursor)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["date"], sample_data[0]["date"])
        self.assertEqual(results[0]["author_id"], sample_data[0]["author_id"])
        self.assertEqual(results[0]["source_id"], sample_data[0]["source_id"])
        self.assertEqual(results[0]["text"], sample_data[0]["text"])
        self.assertEqual(results[0]["metadata"], sample_data[0]["metadata"])
        self.assertEqual(results[0]["actions"], sample_data[0]["actions"])
        self.assertEqual(results[0]["interactions"], sample_data[0]["interactions"])
        self.assertTrue(override_recompute)

    def test_extract_no_override(self):
        sample_data = [
            {
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "test_test",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": False,
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
            {
                "author_id": "2",
                "date": datetime(2022, 1, 2),
                "source_id": "8880",
                "text": "test_test2",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34569",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
                "interactions": [],
            },
        ]
        self.client[self.platform_id]["rawmemberactivities"].insert_many(sample_data)
        extract_data = ExtractPlatformRawData(self.platform_id, "channel_id")

        cursor, override_recompute = extract_data.extract(
            from_date=datetime(2020, 1, 1),
            to_date=None,
            resources=["8888", "8880"],
            recompute=False,
        )
        results = list(cursor)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["date"], sample_data[1]["date"])
        self.assertEqual(results[0]["author_id"], sample_data[1]["author_id"])
        self.assertEqual(results[0]["source_id"], sample_data[1]["source_id"])
        self.assertEqual(results[0]["text"], sample_data[1]["text"])
        self.assertEqual(results[0]["metadata"], sample_data[1]["metadata"])
        self.assertEqual(results[0]["actions"], sample_data[1]["actions"])
        self.assertEqual(results[0]["interactions"], sample_data[1]["interactions"])
        self.assertFalse(override_recompute)
