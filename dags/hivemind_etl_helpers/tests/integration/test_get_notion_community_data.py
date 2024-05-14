from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.modules import ModulesNotion
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestGetNotionCommunityData(TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")
        self.client = client
        self.modules_notion = ModulesNotion()

    def test_get_empty_data(self):
        result = self.modules_notion.get_learning_platforms()
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
                                "client_config": {},
                            },
                            "type": "source",
                            "from_date": datetime(2024, 1, 1),
                            "options": {},
                        }
                    ]
                },
            }
        )

        result = self.modules_notion.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0]["community_id"], "6579c364f1120850414e0dc5")
        self.assertIn(result[0]["from_date"], [datetime(2024, 1, 1), None], "from_date should be datetime(2024, 1, 1) or None")
        self.assertEqual(
            result[0]["database_ids"],
            [
                "dadd27f1dc1e4fa6b5b9dea76858dabe",
                "eadd27f1dc1e4fa6b5b9dea76858dabe",
                "fadd27f1dc1e4fa6b5b9dea76858dabe",
            ]
        )
        self.assertEqual(
            result[0]["page_ids"],
            [
                "6a3c20b6861145b29030292120aa03e6",
                "7a3c20b6861145b29030292120aa03e6",
                "8a3c20b6861145b29030292120aa03e6",
            ]
        )
        self.assertEqual(result[0]["client_config"], {})

    def test_get_string_input(self):
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "notion",
                            "metadata": {
                                "database_ids": [
                                    "dadd27f1dc1e4fa6b5b9dea76858dabe",
                                    "47d677c96cfc434dbe49cb90f0d8fdfb",
                                ],
                                "page_ids": [
                                    "6a3c20b6861145b29030292120aa03e6",
                                    "e479ee3eef9a4eefb3a393848af9ed9d",
                                ],
                                "client_config": {},
                            },
                            "type": "source",
                            "from_date": datetime(2024, 1, 1),
                        }
                    ]
                },
            }
        )

        result = self.modules_notion.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        print(result[0])
        self.assertEqual(result[0]["community_id"], "6579c364f1120850414e0dc5", "Check community ID")
        self.assertIn(result[0]["from_date"], [datetime(2024, 1, 1), None], "from_date should be datetime(2024, 1, 1) or None")
        self.assertEqual(
            result[0]["database_ids"],
            [
                "dadd27f1dc1e4fa6b5b9dea76858dabe",
                "47d677c96cfc434dbe49cb90f0d8fdfb",
            ],
            "Check database IDs match expected"
        )
        self.assertEqual(
            result[0]["page_ids"],
            [
                "6a3c20b6861145b29030292120aa03e6",
                "e479ee3eef9a4eefb3a393848af9ed9d",
            ],
            "Check page IDs match expected"
        )
        self.assertEqual(result[0]["client_config"], {}, "Check client_config is empty")

    def test_get_notion_communities_data_multiple_platforms(self):
        """
        two notion platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
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
                                "client_config": {},
                            },
                            "from_date": datetime(2024, 1, 1),
                        },
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc7"),
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
                                "client_config": {},
                            },
                            "from_date": datetime(2024, 2, 2),
                        },
                    ]
                },
            }
        )

        result = self.modules_notion.get_learning_platforms()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["community_id"], "1009c364f1120850414e0dc5", "Check community ID for first result")
        self.assertIn(result[0]["from_date"], [datetime(2024, 1, 1), None], "from_date should be datetime(2024, 1, 1) or None")
        self.assertEqual(
            result[0]["database_ids"],
            [
                "dadd27f1dc1e4fa6b5b9dea76858dabe",
                "384d0d271c8d4668a79db40aca9e15de",
            ],
            "Check database IDs match expected for first result"
        )
        self.assertEqual(
            result[0]["page_ids"],
            [
                "6a3c20b6861145b29030292120aa03e6",
                "e479ee3eef9a4eefb3a393848af9ed9d",
            ],
            "Check page IDs match expected for first result"
        )
        self.assertEqual(result[0]["client_config"], {}, "Check client_config is empty for first result")

        # Assertions for the second element in the result
        self.assertEqual(result[1]["community_id"], "1009c364f1120850414e0dc5", "Check community ID for second result")
        self.assertIn(result[1]["from_date"], [datetime(2024, 2, 2), None], "from_date should be datetime(2024, 2, 2) or None")
        self.assertEqual(
            result[1]["database_ids"],
            [
                "eadd27f1dc1e4fa6b5b9dea76858dabe",
                "484d0d271c8d4668a79db40aca9e15de",
            ],
            "Check database IDs match expected for second result"
        )
        self.assertEqual(
            result[1]["page_ids"],
            [
                "7a3c20b6861145b29030292120aa03e6",
                "f479ee3eef9a4eefb3a393848af9ed9d",
            ],
            "Check page IDs match expected for second result"
        )
        self.assertEqual(result[1]["client_config"], {}, "Check client_config is empty for second result")
