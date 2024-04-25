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
        result = self.modules_discord.get_discord_communities()
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

        result = self.modules_discord.get_discord_communities()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        self.assertEqual(result, ["6579c364f1120850414e0dc5"])

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
                                    "fromDate": datetime(2024, 1, 1),
                                    "selectedChannels": ["333", "222"],
                                }
                            },
                        },
                    ]
                },
            }
        )

        results = self.modules_discord.get_discord_communities()

        self.assertEqual(results, ["1009c364f1120850414e0dc5"])
