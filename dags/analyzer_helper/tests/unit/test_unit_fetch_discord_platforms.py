import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from analyzer_helper.discord.fetch_discord_platforms import FetchDiscordPlatforms
from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchDiscordPlatformsUnit(unittest.TestCase):
    @patch.object(MongoSingleton, "get_instance")
    def test_fetch_all(self, mock_get_instance):
        mock_client = MagicMock()
        mock_db = mock_client["Core"]
        mock_collection = mock_db["platforms"]

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
            },
        ]

        mock_collection.find.return_value = sample_data
        mock_get_instance.return_value = MagicMock(client=mock_client)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = [
            {
                "platform_id": str(sample_data[0]["_id"]),
                "guild_id": "777777777777777",
                "period": datetime(2023, 10, 20),
                "recompute": False,
            },
            {
                "platform_id": str(sample_data[1]["_id"]),
                "guild_id": "888888888888888",
                "period": datetime(2023, 10, 20),
                "recompute": False,
            },
        ]

        self.assertEqual(result, expected_result)

    @patch.object(MongoSingleton, "get_instance")
    def test_fetch_analyzer_parameters(self, mock_get_instance):
        mock_client = MagicMock()
        mock_db = mock_client["Core"]
        mock_collection = mock_db["platforms"]

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
        ]

        mock_collection.find.return_value = sample_data
        mock_get_instance.return_value = MagicMock(client=mock_client)

        fetcher = FetchDiscordPlatforms()

        platform_id_first = ObjectId("000000000000000000000001")

        result = fetcher.fetch_analyzer_parameters(platform_id_first)

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
        ]
        print("Result: ", result)
        print("Expected result: ", expected_result)
        self.assertEqual(result, expected_result)

    @patch.object(MongoSingleton, "get_instance")
    def test_fetch_all_empty(self, mock_get_instance):
        mock_client = MagicMock()
        mock_db = mock_client["Core"]
        mock_collection = mock_db["platforms"]

        mock_collection.find.return_value = []
        mock_get_instance.return_value = MagicMock(client=mock_client)

        fetcher = FetchDiscordPlatforms()

        result = fetcher.fetch_all()

        expected_result = []

        self.assertEqual(result, expected_result)

    @patch.object(MongoSingleton, "get_instance")
    def test_fetch__analyzer_parameters_empty(self, mock_get_instance):
        mock_client = MagicMock()
        mock_db = mock_client["Core"]
        mock_collection = mock_db["platforms"]

        mock_collection.find.return_value = []
        mock_get_instance.return_value = MagicMock(client=mock_client)

        fetcher = FetchDiscordPlatforms()

        platform_id = ObjectId("000000000000000000000001")

        result = fetcher.fetch_analyzer_parameters(platform_id)

        expected_result = []

        self.assertEqual(result, expected_result)
