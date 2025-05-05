import unittest
from datetime import datetime, timedelta

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesGDrive
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestQueryGDriveModulesDB(unittest.TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
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
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        community_id = ObjectId("6579c364f1120850414e0dc5")
        sample_refresh_token = "tokenid8899812"
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "google",
                "metadata": {
                    "id": "113445975232201081511",
                    "userId": str(sample_user),
                    "name": "John Doe",
                    "picture": "random-image",
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
                            "name": "google",
                            "metadata": {
                                "driveIds": ["1234"],
                                "folderIds": ["111", "234"],
                                "fileIds": ["124", "782"],
                            },
                        }
                    ]
                },
                "activated": True,
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
                "platform_id": str(platform_id),
                "community_id": "6579c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "refresh_token": sample_refresh_token,
            },
        )

    def test_get_gdrive_single_community_data_multiple_platforms(self):
        """
        two gdrive platform for one community
        """
        sample_user1 = ObjectId("5d7baf326c8a2e2400000000")
        platform_id1 = ObjectId("6579c364f1120850414e0dc6")
        sample_refresh_token1 = "tokenid8899812"

        sample_user2 = ObjectId("5d7baf326c8a2e2400000001")
        platform_id2 = ObjectId("6579c364f1120850414e0dc8")
        sample_refresh_token2 = "tokeni00000"

        community_id = ObjectId("1009c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id1,
                "name": "google",
                "metadata": {
                    "id": "113445975232201081511",
                    "userId": str(sample_user1),
                    "name": "John Doe",
                    "picture": "random-image",
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
                "name": "google",
                "metadata": {
                    "id": "113445975232201081511",
                    "userId": str(sample_user2),
                    "name": "Jane Doe",
                    "picture": "random-image",
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
                            "platform": platform_id1,
                            "name": "google",
                            "metadata": {
                                "driveIds": ["1234"],
                                "folderIds": ["111", "234"],
                                "fileIds": ["124", "782"],
                            },
                        },
                        {
                            "platform": platform_id2,
                            "name": "google",
                            "metadata": {
                                "driveIds": ["8438348"],
                                "folderIds": ["9090"],
                            },
                        },
                    ]
                },
                "activated": True,
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "platform_id": str(platform_id1),
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
                "platform_id": str(platform_id2),
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
                "platform_id": str(platform_id1),
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "refresh_token": sample_refresh_token1,
            },
        )
        self.assertEqual(
            result[1],
            {
                "platform_id": str(platform_id2),
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["8438348"],
                "folder_ids": ["9090"],
                "file_ids": [],
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
        platform_id1 = ObjectId("6579c364f1120850414e0dc6")

        sample_user2 = ObjectId("5d7baf326c8a2e2400000001")
        sample_access_token2 = "tokenid9999"
        sample_refresh_token2 = "tokeni00000"
        platform_id2 = ObjectId("6579c364f1120850414e0dc8")

        community_id1 = ObjectId("1009c364f1120850414e0dc5")
        community_id2 = ObjectId("6579c364f1120850414e0dc5")

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id1,
                "name": "google",
                "metadata": {
                    "id": "113445975232201081511",
                    "userId": str(sample_user1),
                    "name": "John Doe",
                    "picture": "random-image",
                },
                "community": community_id1,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id2,
                "name": "google",
                "metadata": {
                    "id": "113445975232201081511",
                    "userId": str(sample_user2),
                    "name": "Jane Doe",
                    "picture": "random-image",
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
                    "community": community_id1,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id1,
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
                    "activated": True,
                },
                {
                    "name": "hivemind",
                    "community": community_id2,
                    "options": {
                        "platforms": [
                            {
                                "platform": platform_id2,
                                "name": "google",
                                "metadata": {
                                    "driveIds": ["8438348"],
                                    "folderIds": ["9090"],
                                    "userId": sample_user2,
                                },
                            }
                        ]
                    },
                    "activated": True,
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
                "platform_id": str(platform_id1),
                "community_id": "1009c364f1120850414e0dc5",
                "drive_ids": ["1234"],
                "folder_ids": ["111", "234"],
                "file_ids": ["124", "782"],
                "refresh_token": sample_refresh_token1,
            },
        )
        self.assertEqual(
            result[1],
            {
                "platform_id": str(platform_id2),
                "community_id": "6579c364f1120850414e0dc5",
                "drive_ids": ["8438348"],
                "folder_ids": ["9090"],
                "file_ids": [],
                "refresh_token": sample_refresh_token2,
            },
        )
