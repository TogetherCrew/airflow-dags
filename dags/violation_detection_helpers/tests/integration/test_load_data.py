from unittest import TestCase
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from violation_detection_helpers import LoadPlatformLabeledData


class TestLoadPlatformLabeledData(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.platform_id = "51515151515151"
        self.client.drop_database(self.platform_id)

    def tearDown(self) -> None:
        self.client.drop_database(self.platform_id)

    def test_load_single_data(self):
        # saving the data without label
        self.client[self.platform_id]["rawmemberactivities"].insert_one(
            {
                "_id": ObjectId("64c6288b1e02c3e4b8f705a3"),
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
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
            }
        )

        loader = LoadPlatformLabeledData()
        labeled_data = [
            {
                "_id": ObjectId("64c6288b1e02c3e4b8f705a3"),
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": "identifying, sexualized",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]

        loader.load(self.platform_id, labeled_data)

        cursor = self.client[self.platform_id]["rawmemberactivities"].find({})
        results = list(cursor)

        self.assertEqual(len(results), 1)

        # asserting just the labels we wanted data to have
        for result in cursor:
            self.assertEqual(result["metadata"]["vdLabel"], "identifying, sexualized")

    def test_load_multiple_data(self):
        # saving the data without label
        self.client[self.platform_id]["rawmemberactivities"].insert_many(
            [
                {
                    "_id": ObjectId("64c6288b1e02c3e4b8f705a3"),
                    "author_id": "1",
                    "date": datetime(2022, 1, 1),
                    "source_id": "8888",
                    "text": "some text message",
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
                },
                {
                    "_id": ObjectId("64c6288b1e02c3e4b8f705a4"),
                    "author_id": "2",
                    "date": datetime(2022, 1, 2),
                    "source_id": "8889",
                    "text": "some text message 2",
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
                },
                {
                    "_id": ObjectId("64c6288b1e02c3e4b8f705a5"),
                    "author_id": "3",
                    "date": datetime(2022, 1, 3),
                    "source_id": "8880",
                    "text": "some text message 3",
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
                },
            ]
        )

        loader = LoadPlatformLabeledData()
        labeled_data = [
            {
                "_id": ObjectId("64c6288b1e02c3e4b8f705a3"),
                "author_id": "1",
                "date": datetime(2022, 1, 1),
                "source_id": "8888",
                "text": "some text message",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": "identifying, sexualized",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "_id": ObjectId("64c6288b1e02c3e4b8f705a4"),
                "author_id": "2",
                "date": datetime(2022, 1, 2),
                "source_id": "8889",
                "text": "some text message 2",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": "identifying, sexualized",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
            {
                "_id": ObjectId("64c6288b1e02c3e4b8f705a5"),
                "author_id": "3",
                "date": datetime(2022, 1, 3),
                "source_id": "8880",
                "text": "some text message 3",
                "metadata": {
                    "topic_id": None,
                    "category_id": "34567",
                    "vdLabel": "identifying, sexualized",
                },
                "actions": [
                    {
                        "name": "message",
                        "type": "emitter",
                    }
                ],
            },
        ]

        loader.load(self.platform_id, labeled_data)

        cursor = self.client[self.platform_id]["rawmemberactivities"].find({})
        results = list(cursor)

        self.assertEqual(len(results), 3)

        # asserting just the labels we wanted data to have
        for result in cursor:
            self.assertEqual(result["metadata"]["vdLabel"], "identifying, sexualized")
