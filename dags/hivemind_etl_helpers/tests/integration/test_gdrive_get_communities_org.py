import unittest
from datetime import datetime, timedelta

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesGDrive
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestQueryGDriveModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("tokens")
        self.client = client
        self.modules_gdrive = ModulesGDrive()

    def test_get_github_communities_data_empty_data(self):
        result = self.modules_gdrive.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_gdrive_communities_data_single_modules(self):
        """
        single gdrive platform for one community
        """
        sample_user = ObjectId("5d7baf326c8a2e2400000000")
        sample_access_token = "tokenid12345"
        sample_refresh_token = "tokenid8899812"
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "google",
                            "metadata": {
                                "driveIds": ["1234"],
                                "folderIds": ["111", "234"],
                                "fileIds": ["124", "782"],
                                "userId": sample_user,
                            },
                        }
                    ]
                },
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token,
                "user": sample_user,
                "type": "google_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_refresh_token,
                "user": sample_user,
                "type": "google_refresh",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        result = self.modules_gdrive.get_learning_platforms()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(
            result[0],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "access_token": sample_access_token,
                "refresh_token": sample_refresh_token,
            },
        )

    def test_get_gdrive_single_community_data_multiple_platforms(self):
        """
        two gdrive platform for one community
        """
        sample_user1 = ObjectId("5d7baf326c8a2e2400000000")
        sample_access_token1 = "tokenid12345"
        sample_refresh_token1 = "tokenid8899812"

        sample_user2 = ObjectId("5d7baf326c8a2e2400000001")
        sample_access_token2 = "tokenid9999"
        sample_refresh_token2 = "tokeni00000"

        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "google",
                            "metadata": {
                                "driveIds": ["1234"],
                                "folderIds": ["111", "234"],
                                "fileIds": ["124", "782"],
                                "userId": sample_user1,
                            },
                        },
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc8"),
                            "name": "google",
                            "metadata": {
                                "driveIds": ["8438348"],
                                "folderIds": ["9090"],
                                "userId": sample_user2,
                            },
                        },
                    ]
                },
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token1,
                "user": sample_user1,
                "type": "google_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_refresh_token1,
                "user": sample_user1,
                "type": "google_refresh",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token2,
                "user": sample_user2,
                "type": "google_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_refresh_token2,
                "user": sample_user2,
                "type": "google_refresh",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        result = self.modules_gdrive.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "access_token": sample_access_token1,
                "refresh_token": sample_refresh_token1,
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["8438348"],
                "folder_ids": ["9090"],
                "file_ids": [],
                "access_token": sample_access_token2,
                "refresh_token": sample_refresh_token2,
            },
        )

    def test_get_gdrive_multiple_community_data_single_platform(self):
        """
        two gdrive platform for one community
        """
        sample_user1 = ObjectId("5d7baf326c8a2e2400000000")
        sample_access_token1 = "tokenid12345"
        sample_refresh_token1 = "tokenid8899812"

        sample_user2 = ObjectId("5d7baf326c8a2e2400000001")
        sample_access_token2 = "tokenid9999"
        sample_refresh_token2 = "tokeni00000"

        self.client["Core"]["modules"].insert_many(
            [
                {
                    "name": "hivemind",
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc6"),
                                "name": "google",
                                "metadata": {
                                    "driveIds": ["1234"],
                                    "folderIds": ["111", "234"],
                                    "fileIds": ["124", "782"],
                                    "userId": sample_user1,
                                },
                            },
                        ]
                    },
                },
                {
                    "name": "hivemind",
                    "community": ObjectId("6579c364f1120850414e0dc5"),
                    "options": {
                        "platforms": [
                            {
                                "platform": ObjectId("6579c364f1120850414e0dc8"),
                                "name": "google",
                                "metadata": {
                                    "driveIds": ["8438348"],
                                    "folderIds": ["9090"],
                                    "userId": sample_user2,
                                },
                            }
                        ]
                    },
                },
            ]
        )

        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token1,
                "user": sample_user1,
                "type": "google_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_refresh_token1,
                "user": sample_user1,
                "type": "google_refresh",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token2,
                "user": sample_user2,
                "type": "google_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_refresh_token2,
                "user": sample_user2,
                "type": "google_refresh",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        result = self.modules_gdrive.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "access_token": sample_access_token1,
                "refresh_token": sample_refresh_token1,
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "drive_ids": ["8438348"],
                "folder_ids": ["9090"],
                "file_ids": [],
                "access_token": sample_access_token2,
                "refresh_token": sample_refresh_token2,
            },
        )
