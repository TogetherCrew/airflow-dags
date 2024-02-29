import unittest
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.get_github_communities_orgs import (
    get_github_communities_and_orgs,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryGitHubModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        self.client = client

    def test_get_github_communities_and_orgs_empty_data(self):
        result = get_github_communities_and_orgs()
        self.assertEqual(result, [])

    def test_get_github_communities_and_orgs_single_modules(self):
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
                            "platformId": ObjectId("6579c364f1120850414e0dc6"),
                            "fromDate": datetime(2024, 1, 1),
                            "options": {"roles": ["role_id"], "users": ["user_id"]},
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

        result = get_github_communities_and_orgs()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(
            result[0],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "organization_id": "1234",
                "from_date": datetime(2024, 1, 1),
            },
        )

    def test_get_github_communities_and_orgs_multiple_platforms(self):
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

        result = get_github_communities_and_orgs()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "organization_id": "1234",
                "from_date": datetime(2024, 1, 1),
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "organization_id": "4321",
                "from_date": datetime(2024, 2, 2),
            },
        )

    def test_get_github_communities_and_orgs_multiple_platforms_multiple_communities(
        self,
    ):
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
                                "fromDate": datetime(2024, 3, 1),
                                "options": {"roles": ["role_id"], "users": ["user_id"]},
                            },
                            {
                                "platformId": ObjectId("6579c364f1120850414e0dd7"),
                                "fromDate": datetime(2024, 3, 2),
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
        results = get_github_communities_and_orgs()

        self.assertIsInstance(results, list)
        # two communities we have
        self.assertEqual(len(results), 4)

        for res in results:
            if res["organization_id"] == "1234":
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0dc5",
                        "organization_id": "1234",
                        "from_date": datetime(2024, 1, 1),
                    },
                )
            elif res["organization_id"] == "4321":
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0dc5",
                        "organization_id": "4321",
                        "from_date": datetime(2024, 2, 2),
                    },
                )
            elif res["organization_id"] == "111111":
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0de5",
                        "organization_id": "111111",
                        "from_date": datetime(2024, 3, 1),
                    },
                )
            elif res["organization_id"] == "444444":
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0de5",
                        "organization_id": "444444",
                        "fromDate": datetime(2024, 3, 2),
                    },
                )
            else:
                # should never reach here
                raise ValueError("No more organizations we had!")
