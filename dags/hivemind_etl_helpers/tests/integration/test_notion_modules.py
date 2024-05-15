from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesNotion
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestGetNotionModules(TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("tokens")
        client["Core"].drop_collection("platforms")
        self.client = client
        self.modules_notion = ModulesNotion()

    def test_get_empty_data(self):
        result = self.modules_notion.get_learning_platforms()
        self.assertEqual(result, [])

    def test_get_single_data(self):
        sample_user = ObjectId("5d7baf326c8a2e2400000000")
        platform_id = ObjectId("6579c364f1120850414e0dc6")
        community_id = ObjectId("6579c364f1120850414e0dc5")
        sample_access_token = "tokenid8899812"

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id,
                "name": "notion",
                "metadata": {
                    "userId": str(sample_user),
                    "workspace_id": "6146c182-d4f5-481c-a4ce-45cedb42f389",
                    "workspace_name": "John Doe's Notion",
                    "workspace_icon": "https://xxxxxxxx",
                    "bot_id": "***************",
                    "request_id": "3b***************07f",
                    "owner": {
                        "type": "user",
                        "user": {
                            "type": "person",
                            "object": "user",
                            "id": "**sdklj****AAAAA",
                            "name": "Jon Doe",
                            "avatar_url": "https://avatar",
                        },
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
                            "name": "notion",
                            "metadata": {
                                "database_ids": [
                                    "dadd27f1dc1e4fa6b5b9dea76858dabe",
                                    "eadd27f1dc1e4fa6b5b9dea76858dabe",
                                    "fadd27f1dc1e4fa6b5b9dea76858dabe",
                                ],
                                "page_ids": [
                                    "6a3c20b6861145b29030292120aa03e6",
                                    "7a3c20b6861145b29030292120aa03e6",
                                    "8a3c20b6861145b29030292120aa03e6",
                                ],
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
                "type": "notion_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        result = self.modules_notion.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0]["community_id"], "6579c364f1120850414e0dc5")
        self.assertEqual(
            result[0]["database_ids"],
            [
                "dadd27f1dc1e4fa6b5b9dea76858dabe",
                "eadd27f1dc1e4fa6b5b9dea76858dabe",
                "fadd27f1dc1e4fa6b5b9dea76858dabe",
            ],
        )
        self.assertEqual(
            result[0]["page_ids"],
            [
                "6a3c20b6861145b29030292120aa03e6",
                "7a3c20b6861145b29030292120aa03e6",
                "8a3c20b6861145b29030292120aa03e6",
            ],
        )
        self.assertEqual(result[0]["access_token"], sample_access_token)

    def test_get_notion_communities_data_multiple_platforms(self):
        """
        two notion platform for one community
        """
        sample_user1 = ObjectId("5d7baf326c8a2e2400000000")
        sample_user2 = ObjectId("5d7baf326c8a2e2400000001")
        platform_id1 = ObjectId("6579c364f1120850414e0dc6")
        platform_id2 = ObjectId("6579c364f1120850414e0dc7")
        community_id = ObjectId("1009c364f1120850414e0dc5")
        sample_access_token1 = "tokenid8899812"
        sample_access_token2 = "tokenid8899832"

        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": community_id,
                "options": {
                    "platforms": [
                        {
                            "platform": platform_id1,
                            "name": "notion",
                            "metadata": {
                                "database_ids": [
                                    "dadd27f1dc1e4fa6b5b9dea76858dabe",
                                    "384d0d271c8d4668a79db40aca9e15de",
                                ],
                                "page_ids": [
                                    "6a3c20b6861145b29030292120aa03e6",
                                    "e479ee3eef9a4eefb3a393848af9ed9d",
                                ],
                            },
                        },
                        {
                            "platform": platform_id2,
                            "name": "notion",
                            "metadata": {
                                "database_ids": [
                                    "eadd27f1dc1e4fa6b5b9dea76858dabe",
                                    "484d0d271c8d4668a79db40aca9e15de",
                                ],
                                "page_ids": [
                                    "7a3c20b6861145b29030292120aa03e6",
                                    "f479ee3eef9a4eefb3a393848af9ed9d",
                                ],
                            },
                        },
                    ]
                },
            }
        )

        self.client["Core"]["platforms"].insert_one(
            {
                "_id": platform_id1,
                "name": "notion",
                "metadata": {
                    "userId": str(sample_user1),
                    "workspace_id": "6146c182-d4f5-481c-a4ce-45cedb42f389",
                    "workspace_name": "John Doe's Notion",
                    "workspace_icon": "https://xxxxxxxx",
                    "bot_id": "***************",
                    "request_id": "3b***************07f",
                    "owner": {
                        "type": "user",
                        "user": {
                            "type": "person",
                            "object": "user",
                            "id": "**sdklj****AAAAA",
                            "name": "Jon Doe",
                            "avatar_url": "https://avatar",
                        },
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
                "name": "notion",
                "metadata": {
                    "userId": str(sample_user2),
                    "workspace_id": "6146c182-d4f5-481c-a4ce-45cedb42f389",
                    "workspace_name": "John Doe's Notion",
                    "workspace_icon": "https://xxxxxxxx",
                    "bot_id": "***************",
                    "request_id": "3b***************07f",
                    "owner": {
                        "type": "user",
                        "user": {
                            "type": "person",
                            "object": "user",
                            "id": "**sdklj****AAAAA",
                            "name": "Jon Doe",
                            "avatar_url": "https://avatar",
                        },
                    },
                },
                "community": community_id,
                "disconnectedAt": None,
                "connectedAt": datetime.now(),
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        self.client["Core"]["tokens"].insert_one(
            {
                "token": sample_access_token1,
                "user": sample_user1,
                "type": "notion_access",
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
                "type": "notion_access",
                "expires": datetime.now() + timedelta(days=1),
                "blacklisted": False,
                "createdAt": datetime.now() - timedelta(days=1),
                "updatedAt": datetime.now() - timedelta(days=1),
            }
        )

        result = self.modules_notion.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            {
                "community_id": str(community_id),
                "database_ids": [
                    "dadd27f1dc1e4fa6b5b9dea76858dabe",
                    "384d0d271c8d4668a79db40aca9e15de",
                ],
                "page_ids": [
                    "6a3c20b6861145b29030292120aa03e6",
                    "e479ee3eef9a4eefb3a393848af9ed9d",
                ],
                "access_token": sample_access_token1,
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": str(community_id),
                "database_ids": [
                    "eadd27f1dc1e4fa6b5b9dea76858dabe",
                    "484d0d271c8d4668a79db40aca9e15de",
                ],
                "page_ids": [
                    "7a3c20b6861145b29030292120aa03e6",
                    "f479ee3eef9a4eefb3a393848af9ed9d",
                ],
                "access_token": sample_access_token2,
            },
        )
