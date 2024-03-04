from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from hivemind_etl_helpers.src.utils.mongo_discord_communities import (
    get_all_discord_communities,
)


class TestGetAllDiscordCommunitites(TestCase):
    def setUp(self) -> None:
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        self.client = client

    def test_get_empty_data(self):
        result = get_all_discord_communities()
        self.assertEqual(result, [])

    def test_get_single_data(self):
        self.client["Core"]["modules"].insert_one(
            {
                "name": "hivemind",
                "communityId": ObjectId("6579c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platformId": ObjectId("6579c364f1120850414e0dc6"),
                            "fromDate": datetime(2024, 1, 1),
                            "options": {},
                        }
                    ]
                },
            }
        )
        self.client["Core"]["platforms"].insert_one(
            {
                "_id": ObjectId("6579c364f1120850414e0dc6"),
                "name": "discord",
                "metadata": {
                    "name": "TEST",
                    "channels": ["1234", "4321"],
                    "roles": ["111", "222"],
                },
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 12, 1),
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
            }
        )

        result = get_all_discord_communities()

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
                "communityId": ObjectId("1009c364f1120850414e0dc5"),
                "options": {
                    "platforms": [
                        {
                            "platformId": ObjectId("6579c364f1120850414e0dc6"),
                            "type": "source",
                            "fromDate": datetime(2024, 1, 1),
                            "options": {},
                        },
                        {
                            "platformId": ObjectId("6579c364f1120850414e0dc7"),
                            "type": "source",
                            "fromDate": datetime(2024, 2, 2),
                            "options": {},
                        },
                    ]
                },
            }
        )
        self.client["Core"]["platforms"].insert_many(
            [
                {
                    "_id": ObjectId("6579c364f1120850414e0dc6"),
                    "name": "discord",
                    "metadata": {
                        "name": "TEST",
                        "channels": ["1234", "4321"],
                        "roles": ["111", "222"],
                    },
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dc7"),
                    "name": "discord",
                    "metadata": {
                        "name": "TEST2",
                        "channels": ["12312", "43221"],
                        "roles": ["1121", "2122"],
                    },
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dc8"),
                    "name": "discord",
                    "metadata": {
                        "name": "TEST3",
                        "channels": ["1234", "4321"],
                        "roles": ["111", "222"],
                    },
                    "community": ObjectId("1009c364f1120850414e0dc8"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
            ]
        )

        results = get_all_discord_communities()

        self.assertEqual(results, ["1009c364f1120850414e0dc5"])
