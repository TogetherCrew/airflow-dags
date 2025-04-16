from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesDiscourse
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestGetDiscourseCommunityData(TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        self.modules_discourse = ModulesDiscourse()
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
        self.client = client

    def test_get_empty_data(self):
        result = self.modules_discourse.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_single_data(self):
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "discourse",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 1, 1),
                                    "endpoint": "forum.endpoint.com",
                                }
                            },
                        }
                    ]
                },
                "activated": True,
            }
        )

        result = self.modules_discourse.get_learning_platforms()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        print(result[0])

        self.assertEqual(
            result[0],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "endpoint": "forum.endpoint.com",
                "from_date": datetime(2024, 1, 1),
            },
        )

    def test_get_discourse_communities_data_multiple_platforms(self):
        """
        two discourse platform for one community
        """
        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc6"),
                                "name": "discourse",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 1, 1),
                                        "endpoint": "example.endpoint.com",
                                    }
                                },
                            }
                        ]
                    },
                    "activated": True,
                },
                {
                    "name": "hivemind",
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc7"),
                                "name": "discourse",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 2, 2),
                                        "endpoint": "example2.endpoint.com",
                                    }
                                },
                            }
                        ]
                    },
                    "activated": True,
                },
            ]
        )

        result = self.modules_discourse.get_learning_platforms()
        print(result)

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "endpoint": "example.endpoint.com",
                "from_date": datetime(2024, 1, 1),
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "endpoint": "example2.endpoint.com",
                "from_date": datetime(2024, 2, 2),
            },
        )
