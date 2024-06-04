import unittest
from datetime import datetime

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesGitHub
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryGitHubModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
        self.modules_github = ModulesGitHub()

        self.client = client

    def test_get_github_communities_data_empty_data(self):
        result = self.modules_github.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_github_communities_data_single_modules(self):
        """
        single github platform for one community
        """
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        community_id = ObjectId("6579c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "github",
                "metadata": {
                    "installationId": "12112",
                    "account": {
                        "login": "org1",
                        "id": "345678",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )

        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": community_id,
                "options": {
                    "platforms": [
                        {
                            "platform": platform_id,
                            "name": "github",
                            "metadata": {
                                # "fromDate": datetime(2024, 1, 1),
                                # "organizationId": ["1234"],
                                # "repoIds": ["111", "234"],
                                "activated": True,
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
                "organization_ids": ["345678"],
                # "repo_ids": ["111", "234"],
                # "from_date": datetime(2024, 1, 1),
                "from_date": None,
            },
        )

    def test_get_github_communities_data_multiple_platforms(self):
        """
        two github platform for one community
        """
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        platform_id2 = ObjectId("6579c364f1120850414e0dc7")
        community_id = ObjectId("6579c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "github",
                "metadata": {
                    "installationId": "8888",
                    "account": {
                        "login": "org1",
                        "id": "11111",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id2,
                "name": "github",
                "metadata": {
                    "installationId": "45678",
                    "account": {
                        "login": "org2",
                        "id": "222222",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": community_id,
                "options": {
                    "platforms": [
                        {
                            "platform": platform_id,
                            "name": "github",
                            "metadata": {
                                # "repoIds": ["111", "234"],
                                "activated": True,
                            },
                        },
                        {
                            "platform": platform_id2,
                            "name": "github",
                            "metadata": {
                                # "fromDate": datetime(2024, 2, 2),
                                # "repoIds": ["2132", "8888"],
                                "activated": True,
                            },
                        },
                    ]
                },
            }
        )

        result = self.modules_github.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        print(result[0])

        self.assertEqual(
            result[0],
            {
                "community_id": str(community_id),
                "organization_ids": ["11111"],
                # "repo_ids": [],
                # "from_date": datetime(2024, 1, 1),
                "from_date": None,
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": str(community_id),
                "organization_ids": ["222222"],
                # "repo_ids": [],
                # "from_date": datetime(2024, 2, 2),
                "from_date": None,
            },
        )

    def test_get_github_communities_data_multiple_platforms_multiple_communities(
        self,
    ):
        """
        two github platform for two separate communities
        """
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        platform_id2 = ObjectId("6579c364f1120850414e0dc7")
        platform_id3 = ObjectId("6579c364f1120850414e0dc8")
        platform_id4 = ObjectId("6579c364f1120850414e0dc9")
        community_id = ObjectId("6579c364f1120850414e0dc5")
        community_id2 = ObjectId("2009c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "github",
                "metadata": {
                    "installationId": "33241",
                    "account": {
                        "login": "org1",
                        "id": "11111",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id2,
                "name": "github",
                "metadata": {
                    "installationId": "9827138",
                    "account": {
                        "login": "org2",
                        "id": "222222",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id3,
                "name": "github",
                "metadata": {
                    "installationId": "901298",
                    "account": {
                        "login": "org3",
                        "id": "333333",
                    },
                },
                "community": community_id2,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id4,
                "name": "discord",
                "metadata": {
                    "learning": {},
                    "answering": {},
                },
                "community": community_id2,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )

        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "community": community_id,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id,
                                "name": "github",
                                "metadata": {
                                    # "fromDate": datetime(2024, 1, 1),
                                    "activated": True,
                                },
                            },
                            {
                                "platform": platform_id2,
                                "name": "github",
                                "metadata": {
                                    # "fromDate": datetime(2024, 2, 2),
                                    "activated": True,
                                    # "repoIds": ["AAAAA"],
                                },
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "community": community_id2,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id3,
                                "name": "github",
                                "metadata": {
                                    "activated": True,
                                    # "fromDate": datetime(2024, 3, 1),
                                    # "organizationId": ["111111"],
                                },
                            },
                            {
                                "platform": platform_id4,
                                "name": "discord",
                                "metadata": {
                                    # "fromDate": datetime(2024, 3, 1),
                                    # "selectedChannels": ["666", "777"],
                                    "activated": True,
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
            if res["organization_ids"] == ["11111"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": str(community_id),
                        "organization_ids": ["11111"],
                        # "repo_ids": [],
                        # "from_date": datetime(2024, 1, 1),
                        "from_date": None,
                    },
                )
            elif res["organization_ids"] == ["222222"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": str(community_id),
                        "organization_ids": ["222222"],
                        # "repo_ids": ["AAAAA"],
                        # "from_date": datetime(2024, 2, 2),
                        "from_date": None,
                    },
                )
            elif res["organization_ids"] == ["333333"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": str(community_id2),
                        "organization_ids": ["333333"],
                        # "repo_ids": [],
                        # "from_date": datetime(2024, 3, 1),
                        "from_date": None,
                    },
                )
            else:
                # should never reach here
                raise ValueError("No more organizations we had!")

    def test_get_github_communities_data_multiple_platforms_multiple_communities_one_disabled(
        self,
    ):
        """
        two github platform for two separate communities
        """
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        platform_id2 = ObjectId("6579c364f1120850414e0dc7")
        platform_id3 = ObjectId("6579c364f1120850414e0dc8")
        platform_id4 = ObjectId("6579c364f1120850414e0dc9")
        community_id = ObjectId("6579c364f1120850414e0dc5")
        community_id2 = ObjectId("2009c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "github",
                "metadata": {
                    "installationId": "901298",
                    "account": {
                        "login": "org1",
                        "id": "11111",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id2,
                "name": "github",
                "metadata": {
                    "installationId": "7218",
                    "account": {
                        "login": "org2",
                        "id": "222222",
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id3,
                "name": "github",
                "metadata": {
                    "installationId": "81279",
                    "account": {
                        "login": "org3",
                        "id": "333333",
                    },
                },
                "community": community_id2,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id4,
                "name": "discord",
                "metadata": {
                    "learning": {},
                    "answering": {},
                },
                "community": community_id2,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )

        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "community": community_id,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id,
                                "name": "github",
                                "metadata": {
                                    "activated": True,
                                    # "fromDate": datetime(2024, 1, 1),
                                },
                            },
                            {
                                "platform": platform_id2,
                                "name": "github",
                                "metadata": {
                                    # "fromDate": datetime(2024, 2, 2),
                                    "activated": True,
                                    # "repoIds": ["AAAAA"],
                                },
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "community": community_id2,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id3,
                                "name": "github",
                                "metadata": {
                                    "activated": False,
                                    # "fromDate": datetime(2024, 3, 1),
                                    # "organizationId": ["111111"],
                                },
                            },
                            {
                                "platform": platform_id4,
                                "name": "discord",
                                "metadata": {
                                    # "fromDate": datetime(2024, 3, 1),
                                    # "selectedChannels": ["666", "777"],
                                    "activated": True,
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
        self.assertEqual(len(results), 2)

        for res in results:
            if res["organization_ids"] == ["11111"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": str(community_id),
                        "organization_ids": ["11111"],
                        # "repo_ids": [],
                        # "from_date": datetime(2024, 1, 1),
                        "from_date": None,
                    },
                )
            elif res["organization_ids"] == ["222222"]:
                self.assertEqual(
                    res,
                    {
                        "community_id": str(community_id),
                        "organization_ids": ["222222"],
                        # "repo_ids": ["AAAAA"],
                        # "from_date": datetime(2024, 2, 2),
                        "from_date": None,
                    },
                )
            else:
                # should never reach here
                raise ValueError("No more organizations we had!")
