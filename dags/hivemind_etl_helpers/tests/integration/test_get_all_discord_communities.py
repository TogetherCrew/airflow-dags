from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from hivemind_etl_helpers.src.utils.modules import ModulesDiscord


class TestGetAllDiscordCommunitites(TestCase):
    def setUp(self) -> None:
        client = MongoSingleton.get_instance().client
        self.modules_discord = ModulesDiscord()
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        self.client = client

    def test_get_empty_data(self):
        result = self.modules_discord.get_learning_platforms()
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
                            "name": "discord",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 1, 1),
                                    "selectedChannels": ["1234", "4321"],
                                }
                            },
                        }
                    ]
                },
            }
        )

        result = self.modules_discord.get_learning_platforms()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        print(result)

        self.assertEqual(
            result[0], 
            {
                "community_id": "6579c364f1120850414e0dc5",
                "platform_id": "6579c364f1120850414e0dc6",
                "selected_channels": ["1234", "4321"],
                "from_date": datetime(2024, 1, 1),
            }
        )

    def test_get_discord_communities_data_multiple_platforms(self):
        """
        two discord platform for one community
        """
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "community": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc6"),
                            "name": "discord",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 1, 1),
                                    "selectedChannels": ["1234", "4321"],
                                }
                            },
                        },
                        {
                            "platform": ObjectId("6579c364f1120850414e0dc7"),
                            "name": "discord",
                            "metadata": {
                                "learning": {
                                    "fromDate": datetime(2024, 2, 2),
                                    "selectedChannels": ["333", "222"],
                                }
                            },
                        },
                    ]
                },
            }
        )

        results = self.modules_discord.get_learning_platforms()
        self.assertEqual(len(results), 2)

        self.assertEqual(
            results, 
            [{
                "community_id": "1009c364f1120850414e0dc5",
                "platform_id": "6579c364f1120850414e0dc6",
                "selected_channels": ["1234", "4321"],
                "from_date": datetime(2024, 1, 1),
            },
            {
                "community_id": "1009c364f1120850414e0dc5",
                "platform_id": "6579c364f1120850414e0dc7",
                "selected_channels": ["333", "222"],
                "from_date": datetime(2024, 2, 2),
            }]
        )
