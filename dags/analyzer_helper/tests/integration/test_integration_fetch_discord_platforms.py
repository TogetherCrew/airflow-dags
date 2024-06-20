import unittest
from datetime import datetime
from bson import ObjectId
from analyzer_helper.discord.fetch_discord_platforms import FetchDiscordPlatforms
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchDiscordPlatforms(unittest.TestCase):
    def setUp(self):
        self.client = MongoSingleton.get_instance().client
        self.db = self.client["Core"]
        self.collection = self.db["platforms"]
        self.collection.delete_many({})

    def tearDown(self):
        self.collection.delete_many({})

    def test_fetch_all(self):
        sample_data = [
            {
                "_id": ObjectId("000000000000000000000001"),
                "platform": "discord",
                "metadata": {
                    "action": {
                        "INT_THR": 1,
                        "UW_DEG_THR": 1,
                        "PAUSED_T_THR": 1,
                        "CON_T_THR": 4,
                        "CON_O_THR": 3,
                        "EDGE_STR_THR": 5,
                        "UW_THR_DEG_THR": 5,
                        "VITAL_T_THR": 4,
                        "VITAL_O_THR": 3,
                        "STILL_T_THR": 2,
                        "STILL_O_THR": 2,
                        "DROP_H_THR": 2,
                        "DROP_I_THR": 1,
                    },
                    "window": {"period_size": 7, "step_size": 1},
                    "id": "777777777777777",
                    "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": [
                        "1067517728543477920",
                        "1067512760163897514",
                        "1177090385307254844",
                        "1177728302123851846",
                        "1194381466663141519",
                        "1194381535734935602",
                    ],
                    "name": "PlatformName",
                    "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "community": "6579c364f1120850414e0dc5",
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 7, 7, 8, 47, 49, 96000),
                "createdAt": datetime(2023, 12, 22, 8, 49, 48, 677000),
                "updatedAt": datetime(2024, 6, 5, 0, 0, 1, 984000),
            },
            {
                "_id": ObjectId("000000000000000000000002"),
                "platform": "discord",
                "metadata": {
                    "action": {
                        "INT_THR": 1,
                        "UW_DEG_THR": 1,
                        "PAUSED_T_THR": 1,
                        "CON_T_THR": 4,
                        "CON_O_THR": 3,
                        "EDGE_STR_THR": 5,
                        "UW_THR_DEG_THR": 5,
                        "VITAL_T_THR": 4,
                        "VITAL_O_THR": 3,
                        "STILL_T_THR": 2,
                        "STILL_O_THR": 2,
                        "DROP_H_THR": 2,
                        "DROP_I_THR": 1,
                    },
                    "window": {"period_size": 7, "step_size": 1},
                    "id": "888888888888888",
                    "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": [
                        "1067517728543477920",
                        "1067512760163897514",
                        "1177090385307254844",
                        "1177728302123851846",
                        "1194381466663141519",
                        "1194381535734935602",
                    ],
                    "name": "PlatformName2",
                    "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "community": "6579c364f1120850414e0dc6",
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 7, 7, 8, 47, 49, 96000),
                "createdAt": datetime(2023, 12, 22, 8, 49, 48, 677000),
                "updatedAt": datetime(2024, 6, 5, 0, 0, 1, 984000),
            },
            {
                "_id": ObjectId("000000000000000000000003"),
                "platform": "telegram",
                "metadata": {
                    "id": "999999999999999",
                    "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": ["1067517728543477920"],
                    "name": "TelegramPlatform",
                    "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "community": "6579c364f1120850414e0dc7",
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 7, 7, 8, 47, 49, 96000),
                "createdAt": datetime(2023, 12, 22, 8, 49, 48, 677000),
                "updatedAt": datetime(2024, 6, 5, 0, 0, 1, 984000),
            },
            {
                "_id": ObjectId("000000000000000000000004"),
                "platform": "discourse",
                "metadata": {
                    "id": "101010101010101",
                    "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": ["1067517728543477920"],
                    "name": "DiscoursePlatform",
                    "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "community": "6579c364f1120850414e0dc8",
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 7, 7, 8, 47, 49, 96000),
                "createdAt": datetime(2023, 12, 22, 8, 49, 48, 677000),
                "updatedAt": datetime(2024, 6, 5, 0, 0, 1, 984000),
            },
        ]

        self.collection.insert_many(sample_data)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = [
            {
                "platform_id": str(sample_data[0]["_id"]),
                "metadata": {
                    "action": {
                        "INT_THR": 1,
                        "UW_DEG_THR": 1,
                        "PAUSED_T_THR": 1,
                        "CON_T_THR": 4,
                        "CON_O_THR": 3,
                        "EDGE_STR_THR": 5,
                        "UW_THR_DEG_THR": 5,
                        "VITAL_T_THR": 4,
                        "VITAL_O_THR": 3,
                        "STILL_T_THR": 2,
                        "STILL_O_THR": 2,
                        "DROP_H_THR": 2,
                        "DROP_I_THR": 1,
                    },
                    "window": {"period_size": 7, "step_size": 1},
                    "id": "777777777777777",
                    # "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    # "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": [
                        "1067517728543477920",
                        "1067512760163897514",
                        "1177090385307254844",
                        "1177728302123851846",
                        "1194381466663141519",
                        "1194381535734935602",
                    ],
                    # "name": "PlatformName",
                    # "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "recompute": False,
            },
            {
                "platform_id": str(sample_data[1]["_id"]),
                "metadata": {
                    "action": {
                        "INT_THR": 1,
                        "UW_DEG_THR": 1,
                        "PAUSED_T_THR": 1,
                        "CON_T_THR": 4,
                        "CON_O_THR": 3,
                        "EDGE_STR_THR": 5,
                        "UW_THR_DEG_THR": 5,
                        "VITAL_T_THR": 4,
                        "VITAL_O_THR": 3,
                        "STILL_T_THR": 2,
                        "STILL_O_THR": 2,
                        "DROP_H_THR": 2,
                        "DROP_I_THR": 1,
                    },
                    "window": {"period_size": 7, "step_size": 1},
                    "id": "888888888888888",
                    # "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    # "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": [
                        "1067517728543477920",
                        "1067512760163897514",
                        "1177090385307254844",
                        "1177728302123851846",
                        "1194381466663141519",
                        "1194381535734935602",
                    ],
                    # "name": "PlatformName2",
                    # "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "recompute": False,
            },
        ]
        self.assertEqual(result, expected_result)

    def test_get_empty_data(self):
        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = []

        self.assertEqual(result, expected_result)

    def test_get_single_data(self):
        sample_data = {
            "_id": ObjectId("000000000000000000000001"),
            "platform": "discord",
            "metadata": {
                "action": {
                    "INT_THR": 1,
                    "UW_DEG_THR": 1,
                    "PAUSED_T_THR": 1,
                    "CON_T_THR": 4,
                    "CON_O_THR": 3,
                    "EDGE_STR_THR": 5,
                    "UW_THR_DEG_THR": 5,
                    "VITAL_T_THR": 4,
                    "VITAL_O_THR": 3,
                    "STILL_T_THR": 2,
                    "STILL_O_THR": 2,
                    "DROP_H_THR": 2,
                    "DROP_I_THR": 1,
                },
                "window": {"period_size": 7, "step_size": 1},
                "id": "777777777777777",
                "isInProgress": False,
                "period": datetime(2023, 10, 20),
                "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                "selectedChannels": [
                    "1067517728543477920",
                    "1067512760163897514",
                    "1177090385307254844",
                    "1177728302123851846",
                    "1194381466663141519",
                    "1194381535734935602",
                ],
                "name": "PlatformName",
                "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
            },
            "community": "6579c364f1120850414e0dc5",
            "disconnectedAt": None,
            "connectedAt": datetime(2023, 7, 7, 8, 47, 49, 96000),
            "createdAt": datetime(2023, 12, 22, 8, 49, 48, 677000),
            "updatedAt": datetime(2024, 6, 5, 0, 0, 1, 984000),
        }

        self.collection.insert_one(sample_data)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = [
            {
                "platform_id": str(sample_data["_id"]),
                "metadata": {
                    "action": {
                        "INT_THR": 1,
                        "UW_DEG_THR": 1,
                        "PAUSED_T_THR": 1,
                        "CON_T_THR": 4,
                        "CON_O_THR": 3,
                        "EDGE_STR_THR": 5,
                        "UW_THR_DEG_THR": 5,
                        "VITAL_T_THR": 4,
                        "VITAL_O_THR": 3,
                        "STILL_T_THR": 2,
                        "STILL_O_THR": 2,
                        "DROP_H_THR": 2,
                        "DROP_I_THR": 1,
                    },
                    "window": {"period_size": 7, "step_size": 1},
                    "id": "777777777777777",
                    # "isInProgress": False,
                    "period": datetime(2023, 10, 20),
                    # "icon": "e160861192ed8c2a6fa65a8ab6ac337e",
                    "selectedChannels": [
                        "1067517728543477920",
                        "1067512760163897514",
                        "1177090385307254844",
                        "1177728302123851846",
                        "1194381466663141519",
                        "1194381535734935602",
                    ],
                    # "name": "PlatformName",
                    # "analyzerStartedAt": datetime(2024, 4, 17, 13, 29, 16, 157000),
                },
                "recompute": False,
            }
        ]

        print("Result `test_get_single_data`:")
        pprint(result)
        print("Expected Result:")
        pprint(expected_result)
        print("Difference:")
        pprint(DeepDiff(result, expected_result, ignore_order=False))
        self.assertEqual(result, expected_result)
