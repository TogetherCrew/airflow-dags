import unittest
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules.modules_base import ModulesBase
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
        self.modules_base = ModulesBase()
        self.client = client

    def test_query_modules_db_empty_data(self):
        result = self.modules_base.query(platform="github")
        self.assertEqual(result, [])

    def test_query_modules_db_single_modules(self):
        """
        single github platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "github",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 1, 1),
                                }
                            },
                        },
                    ]
                },
            }
        )

        result = self.modules_base.query(platform="github")

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0]["name"], "hivemind")
        self.assertEqual(result[0]["community"], ObjectId("6579c364f1120850414e0dc5"))
        self.assertEqual(
            result[0]["options"],
            {
                "platforms": [
                    {
                        "platform": ObjectId("6579c364f1120850414e0dc6"),
                        "name": "github",
                        "metadata": {
                            "learning": {
                                "fromDate": datetime(2024, 1, 1),
                            }
                        },
                    },
                ]
            },
        )

    def test_query_modules_db_multiple_platforms(self):
        """
        two github platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "github",
                            "metadata": {
                                "learning": {
                                    "organizationId": 123,
                                    "fromDate": datetime(2024, 1, 1),
                                }
                            },
                        },
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc7"),
                            "name": "github",
                            "metadata": {
                                "learning": {
                                    "organizationId": 321,
                                    "fromDate": datetime(2024, 2, 2),
                                }
                            },
                        },
                    ]
                },
            }
        )

        result = self.modules_base.query(platform="github")

        self.assertIsInstance(result, list)

        # 1 community holding two platforms
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["community"], ObjectId("6579c364f1120850414e0dc5"))

        # two platform we had
        self.assertEqual(len(result[0]["options"]["platforms"]), 2)

        # github PLATFORM 1
        self.assertEqual(
            result[0]["options"]["platforms"][0]["metadata"]["learning"][
                "organizationId"
            ],
            123,
        )

        self.assertEqual(
            result[0]["options"]["platforms"][0]["platform"],
            ObjectId("6579c364f1120850414e0dc6"),
        )

        # github PLATFORM 2
        self.assertEqual(
            result[0]["options"]["platforms"][1]["metadata"]["learning"][
                "organizationId"
            ],
            321,
        )

        self.assertEqual(
            result[0]["options"]["platforms"][1]["platform"],
            ObjectId("6579c364f1120850414e0dc7"),
        )

    def test_query_modules_db_multiple_platforms_multiple_communities(self):
        """
        two github platform for two separate communities
        """
        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "community": ObjectId("6579c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc6"),
                                "name": "github",
                                "metadata": {
                                    "learning": {
                                        "organizationId": 123,
                                        "fromDate": datetime(2024, 1, 1),
                                    }
                                },
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "community": ObjectId("6579c364f1120850414e0dc8"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc9"),
                                "name": "discord",
                                "metadata": {
                                    "learning": {
                                        "organizationId": 123,
                                        "fromDate": datetime(2024, 1, 1),
                                    }
                                },
                            },
                        ]
                    },
                },
            ]
        )
        results = self.modules_base.query(platform="github")

        self.assertIsInstance(results, list)
        # two communities we have
        self.assertEqual(len(results), 1)

        self.assertEqual(results[0]["name"], "hivemind")
        self.assertEqual(results[0]["community"], ObjectId("6579c364f1120850414e0dc5"))
        self.assertEqual(
            results[0]["options"],
            {
                "platforms": [
                    {
                        "platform": ObjectId("6579c364f1120850414e0dc6"),
                        "name": "github",
                        "metadata": {
                            "learning": {
                                "organizationId": 123,
                                "fromDate": datetime(2024, 1, 1),
                            }
                        },
                    },
                ]
            },
        )
