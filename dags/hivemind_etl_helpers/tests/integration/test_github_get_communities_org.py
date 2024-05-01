import unittest
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesGitHub
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryGitHubModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        self.modules_github = ModulesGitHub()

        self.client = client

    def test_get_github_communities_data_empty_data(self):
        result = self.modules_github.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_github_communities_data_single_modules(self):
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
                                    "organizationId": ["1234"],
                                    "repoIds": ["111", "234"],
                                }
                            },
                        }
                    ]
                },
            }
        )

        result = self.modules_github.get_learning_platforms()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(
            result[0],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "organization_ids": ["1234"],
                "repo_ids": ["111", "234"],
                "from_date": datetime(2024, 1, 1),
            },
        )

    def test_get_github_communities_data_multiple_platforms(self):
        """
        two github platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "github",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 1, 1),
                                    "organizationId": ["1234"],
                                    "repoIds": ["111", "234"],
                                }
                            },
                        },
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc7"),
                            "name": "github",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 2, 2),
                                    "organizationId": ["4321"],
                                    "repoIds": ["2132", "8888"],
                                }
                            },
                        },
                    ]
                },
            }
        )

        result = self.modules_github.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "organization_ids": ["1234"],
                "repo_ids": ["111", "234"],
                "from_date": datetime(2024, 1, 1),
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "organization_ids": ["4321"],
                "repo_ids": ["2132", "8888"],
                "from_date": datetime(2024, 2, 2),
            },
        )

    def test_get_github_communities_data_multiple_platforms_multiple_communities(
        self,
    ):
        """
        two github platform for two separate communities
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
                                "name": "github",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 1, 1),
                                        "organizationId": ["1234"],
                                    }
                                },
                            },
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc7"),
                                "name": "github",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 2, 2),
                                        "repoIds": ["1111"],
                                    }
                                },
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "community": ObjectId("2009c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0db5"),
                                "name": "github",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 3, 1),
                                        "organizationId": ["111111"],
                                    }
                                },
                            },
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc7"),
                                "name": "discord",
                                "metadata": {
                                    "learning": {
                                        "fromDate": datetime(2024, 3, 1),
                                        "selectedChannels": ["666", "777"],
                                    }
                                },
                            },
                        ]
                    },
                },
            ]
        )
        results = self.modules_github.get_learning_platforms()

        self.assertIsInstance(results, list)
        # two communities we have
        self.assertEqual(len(results), 3)

        for res in results:
            if res["organization_ids"] == ["1234"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0dc5",
                        "organization_ids": ["1234"],
                        "repo_ids": [],
                        "from_date": datetime(2024, 1, 1),
                    },
                )
            elif res["organization_ids"] == []:
                self.assertEqual(
                    res,
                    {
                        "community_id": "1009c364f1120850414e0dc5",
                        "organization_ids": [],
                        "repo_ids": ["1111"],
                        "from_date": datetime(2024, 2, 2),
                    },
                )
            elif res["organization_ids"] == ["111111"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": "2009c364f1120850414e0dc5",
                        "organization_ids": ["111111"],
                        "repo_ids": [],
                        "from_date": datetime(2024, 3, 1),
                    },
                )
            else:
                # should never reach here
                raise ValueError("No more organizations we had!")
