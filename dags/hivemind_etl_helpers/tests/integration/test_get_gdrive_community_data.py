from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.utils.get_communities_data import (
    get_google_drive_communities,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestGetDriveCommunityData(TestCase):
    def setUp(self):
        client = MongoSingleton.get_instance().client
        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        self.client = client

    def test_get_empty_data(self):
        result = get_google_drive_communities()
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
                            "type": "source",
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
                "name": "google-drive",
                "metadata": {
                    "name": "TEST",
                    "folder_id": "1RFhr3-KmOZCR5rtp4dlOM",
                    "file_id": "1RFhr3-KmOZCR5rtp12",
                    "drive_id": "1RFhr3-KmOZCR5rtp123",
                    "client_config": {},
                },
                "community": ObjectId("6579c364f1120850414e0dc5"),
                "disconnectedAt": None,
                "connectedAt": datetime(2023, 12, 1),
                "createdAt": datetime(2023, 12, 1),
                "updatedAt": datetime(2023, 12, 1),
            }
        )

        result = get_google_drive_communities()

        # Assertions
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        print(result[0])

        self.assertEqual(
            result[0],
            {
                "community_id": "6579c364f1120850414e0dc5",
                "from_date": datetime(2024, 1, 1),
                "folder_id": "1RFhr3-KmOZCR5rtp4dlOM",
                "file_id": "1RFhr3-KmOZCR5rtp12",
                "drive_id": "1RFhr3-KmOZCR5rtp123",
                "client_config": {},
            },
        )

    def test_get_gdrive_communities_data_multiple_platforms(self):
        """
        two gdrive platform for one community
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
                    "name": "google-drive",
                    "metadata": {
                        "name": "TEST",
                        "folder_id": "1RFhr3-KmOZCR5rtp4dlOMn",
                        "file_id": "1RFhr3-KmOZCR5rtp120",
                        "drive_id": "1RFhr3-KmOZCR5rtp1230",
                        "client_config": {},
                    },
                    "community": ObjectId("6579c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                {
                    "_id": ObjectId("6579c364f1120850414e0dc7"),
                    "name": "google-drive",
                    "metadata": {
                        "name": "TEST",
                        "folder_id": "1RFhr3-KmOZCR5rtp4dlOMl",
                        "file_id": "1RFhr3-KmOZCR5rtp121",
                        "drive_id": "1RFhr3-KmOZCR5rtp1231",
                        "client_config": {},
                    },
                    "community": ObjectId("6579c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
                # discord shouldn't be returned in this test case
                {
                    "_id": ObjectId("6579c364f1120850414e0dc8"),
                    "name": "discord",
                    "metadata": {"name": "TEST3", "channels": ["9000", "9001"]},
                    "community": ObjectId("1009c364f1120850414e0dc5"),
                    "disconnectedAt": None,
                    "connectedAt": datetime(2023, 12, 1),
                    "createdAt": datetime(2023, 12, 1),
                    "updatedAt": datetime(2023, 12, 1),
                },
            ]
        )

        result = get_google_drive_communities()

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "from_date": datetime(2024, 1, 1),
                "folder_id": "1RFhr3-KmOZCR5rtp4dlOMn",
                "file_id": "1RFhr3-KmOZCR5rtp120",
                "drive_id": "1RFhr3-KmOZCR5rtp1230",
                "client_config": {},
            },
        )
        self.assertEqual(
            result[1],
            {
                "community_id": "1009c364f1120850414e0dc5",
                "from_date": datetime(2024, 2, 1),
                "folder_id": "1RFhr3-KmOZCR5rtp4dlOMl",
                "file_id": "1RFhr3-KmOZCR5rtp121",
                "drive_id": "1RFhr3-KmOZCR5rtp1231",
                "client_config": {},
            },
        )
