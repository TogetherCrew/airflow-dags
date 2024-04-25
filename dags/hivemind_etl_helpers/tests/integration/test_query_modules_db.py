import unittest
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.get_communities_data import query_modules_db
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        self.client = client

    def test_query_modules_db_empty_data(self):
        result = query_modules_db(platform="github")
        self.assertEqual(result, [])

    def test_query_modules_db_single_modules(self):
        """
        single github platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "communityId": ObjectId("6579c364f1120850414e0dc5"),
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
                        }
                    ]
                },
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId("6579c364f1120850414e0dc6"),
                "name": "github",
                "metadata": {"name": "TEST", "organizationId": "1234"},
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 12, 1),
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
            }
        )

        result = query_modules_db(platform="github")

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["communityId"], ObjectId("6579c364f1120850414e0dc5"))

        # one platform we had
        self.assertEqual(len(result[0]["options"][0]), 1)
        self.assertEqual(list(result[0]["options"][0].keys()), ["platforms"])
        self.assertEqual(
            result[0]["options"][0]["platforms"]["metadata"]["organizationId"], "1234"
        )

        self.assertEqual(
            result[0]["options"][0]["platforms"]["platformId"],
            ObjectId("6579c364f1120850414e0dc6"),
        )

        self.assertEqual(
            result[0]["options"][0]["platforms"]["fromDate"], datetime(2024, 1, 1)
        )

    def test_query_modules_db_multiple_platforms(self):
        """
        two github platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "communityId": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platformId": ObjectId("6579c364f1120850414e0dc6"),
                            "fromDate": datetime(2024, 1, 1),
                            "options": {"roles": ["role_id"], "users": ["user_id"]},
                        },
                        {
                            "platformId": ObjectId("6579c364f1120850414e0dc7"),
                            "fromDate": datetime(2024, 2, 2),
                            "options": {"roles": ["role_id"], "users": ["user_id"]},
                        },
                    ]
                },
            }
        )
        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId("6579c364f1120850414e0dc6"),
                    "name": "github",
                    "metadata": {"name": "TEST", "organizationId": "1234"},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dc7"),
                    "name": "github",
                    "metadata": {"name": "TEST2", "organizationId": "4321"},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                # discord shouldn't be returned in this test case
                {
                    "_id": ObjectId("6579c364f1120850414e0dc8"),
                    "name": "discord",
                    "metadata": {"name": "TEST3", "channels": ["9000", "9001"]},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
            ]
        )

        result = query_modules_db(platform="github")

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["communityId"], ObjectId("1009c364f1120850414e0dc5"))

        # two platform we had
        self.assertEqual(len(result[0]["options"]), 2)

        # github PLATFORM 1
        self.assertEqual(
            result[0]["options"][0]["platforms"]["metadata"]["organizationId"], "1234"
        )

        self.assertEqual(
            result[0]["options"][0]["platforms"]["platformId"],
            ObjectId("6579c364f1120850414e0dc6"),
        )

        self.assertEqual(
            result[0]["options"][0]["platforms"]["fromDate"], datetime(2024, 1, 1)
        )

        # github platform 2
        self.assertEqual(
            result[0]["options"][1]["platforms"]["metadata"]["organizationId"], "4321"
        )

        self.assertEqual(
            result[0]["options"][1]["platforms"]["platformId"],
            ObjectId("6579c364f1120850414e0dc7"),
        )

        self.assertEqual(
            result[0]["options"][1]["platforms"]["fromDate"], datetime(2024, 2, 2)
        )

    def test_query_modules_db_multiple_platforms_multiple_communities(self):
        """
        two github platform for two separate communities
        """
        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "communityId": ObjectId("1009c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platformId": ObjectId("6579c364f1120850414e0dc6"),
                                "fromDate": datetime(2024, 1, 1),
                                "options": {"roles": ["role_id"], "users": ["user_id"]},
                            },
                            {
                                "platformId": ObjectId("6579c364f1120850414e0dc7"),
                                "fromDate": datetime(2024, 2, 2),
                                "options": {"roles": ["role_id"], "users": ["user_id"]},
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "communityId": ObjectId("1009c364f1120850414e0de5"),
                    "options": {
                        "platforms": [
                            {
                                "platformId": ObjectId("6579c364f1120850414e0dd6"),
                                "fromDate": datetime(2024, 1, 1),
                                "options": {"roles": ["role_id"], "users": ["user_id"]},
                            },
                            {
                                "platformId": ObjectId("6579c364f1120850414e0dd7"),
                                "fromDate": datetime(2024, 2, 2),
                                "options": {"roles": ["role_id"], "users": ["user_id"]},
                            },
                        ]
                    },
                },
            ]
        )
        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId("6579c364f1120850414e0dc6"),
                    "name": "github",
                    "metadata": {"name": "TEST", "organizationId": "1234"},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dc7"),
                    "name": "github",
                    "metadata": {"name": "TEST2", "organizationId": "4321"},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dd6"),
                    "name": "github",
                    "metadata": {"name": "TEST3", "organizationId": "111111"},
                    "community": ObjectId("1009c364f1120850414e0de5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dd7"),
                    "name": "github",
                    "metadata": {"name": "TEST4", "organizationId": "444444"},
                    "community": ObjectId("1009c364f1120850414e0de5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
            ]
        )
        results = query_modules_db(platform="github")

        self.assertIsInstance(results, list)
        # two communities we have
        self.assertEqual(len(results), 2)

        for community_result in results:
            if community_result["communityId"] == ObjectId("1009c364f1120850414e0de5"):
                # Community 2: github PLATFORM 1
                self.assertEqual(
                    community_result["options"][0]["platforms"]["metadata"][
                        "organizationId"
                    ],
                    "111111",
                )

                self.assertEqual(
                    community_result["options"][0]["platforms"]["platformId"],
                    ObjectId("6579c364f1120850414e0dd6"),
                )

                self.assertEqual(
                    community_result["options"][0]["platforms"]["fromDate"],
                    datetime(2024, 1, 1),
                )

                # Community 2: github PLATFORM 2
                self.assertEqual(
                    community_result["options"][1]["platforms"]["metadata"][
                        "organizationId"
                    ],
                    "444444",
                )

                self.assertEqual(
                    community_result["options"][1]["platforms"]["platformId"],
                    ObjectId("6579c364f1120850414e0dd7"),
                )

                self.assertEqual(
                    community_result["options"][1]["platforms"]["fromDate"],
                    datetime(2024, 2, 2),
                )
            elif community_result["communityId"] == ObjectId(
                "1009c364f1120850414e0dc5"
            ):
                # two platform we had
                self.assertEqual(len(community_result["options"]), 2)

                # Community 1: github PLATFORM 1
                self.assertEqual(
                    community_result["options"][0]["platforms"]["metadata"][
                        "organizationId"
                    ],
                    "1234",
                )

                self.assertEqual(
                    community_result["options"][0]["platforms"]["platformId"],
                    ObjectId("6579c364f1120850414e0dc6"),
                )

                self.assertEqual(
                    community_result["options"][0]["platforms"]["fromDate"],
                    datetime(2024, 1, 1),
                )

                # Community 1: github PLATFORM 2
                self.assertEqual(
                    community_result["options"][1]["platforms"]["metadata"][
                        "organizationId"
                    ],
                    "4321",
                )

                self.assertEqual(
                    community_result["options"][1]["platforms"]["platformId"],
                    ObjectId("6579c364f1120850414e0dc7"),
                )

                self.assertEqual(
                    community_result["options"][1]["platforms"]["fromDate"],
                    datetime(2024, 2, 2),
                )
            else:
                # we should never reach this point
                ValueError("No more communities we had!")
