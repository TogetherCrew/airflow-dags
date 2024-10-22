from datetime import datetime
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.db.telegram.utils import TelegramPlatform
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestTelegramPlatform(TestCase):
    def setUp(self) -> None:
        self.client = MongoSingleton.get_instance().get_client()
        self.chat_id = "1234567"
        self.chat_name = "sample_chat"
        self.telegram_platform = TelegramPlatform(self.chat_id, self.chat_name)

        # changing db and collection for just the test case
        self.telegram_platform.collection = "TempCore"
        self.telegram_platform.database = "TempPlatforms"
        self.client.drop_database(self.telegram_platform.database)

    def tearDown(self) -> None:
        self.client.drop_database(self.telegram_platform.database)

    def test_check_no_platform_available(self):
        community_id, platform_id = self.telegram_platform.check_platform_existence()
        self.assertIsNone(community_id)
        self.assertIsNone(platform_id)

    def test_single_platform_available(self):
        community_id = ObjectId()

        result = self.client[self.telegram_platform.database][
            self.telegram_platform.collection
        ].insert_one(
            {
                "name": "telegram",
                "metadata": {
                    "id": self.chat_id,
                    "name": self.chat_name,
                },
                "community": community_id,
                "disconnectedAt": None,
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
            }
        )
        created_community_id, created_platform_id = (
            self.telegram_platform.check_platform_existence()
        )
        self.assertEqual(community_id, created_community_id)
        self.assertEqual(result.inserted_id, created_platform_id)

    def test_telegram_multiple_platform_not_available(self):
        chat_id = "111111"
        chat_name = "sample_chat1"
        chat_id2 = "222222"
        chat_name2 = "sample_chat2"
        chat_id3 = "222222"
        chat_name3 = "sample_chat3"

        self.client[self.telegram_platform.database][
            self.telegram_platform.collection
        ].insert_many(
            [
                {
                    "name": "telegram",
                    "metadata": {
                        "id": chat_id,
                        "name": chat_name,
                    },
                    "community": ObjectId(),
                    "disconnectedAt": None,
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                },
                {
                    "name": "telegram",
                    "metadata": {
                        "id": chat_id2,
                        "name": chat_name2,
                    },
                    "community": ObjectId(),
                    "disconnectedAt": None,
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                },
                {
                    "name": "telegram",
                    "metadata": {
                        "id": chat_id3,
                        "name": chat_name3,
                    },
                    "community": ObjectId(),
                    "disconnectedAt": None,
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                },
            ]
        )

        community_id, platform_id = self.telegram_platform.check_platform_existence()
        self.assertIsNone(community_id)
        self.assertIsNone(platform_id)

    def test_create_platform(self):
        community_id, platform_id = self.telegram_platform.create_platform()

        self.assertIsNotNone(community_id)
        self.assertIsNotNone(platform_id)
        fetched_community_id, fetched_platform_id = (
            self.telegram_platform.check_platform_existence()
        )
        self.assertEqual(fetched_community_id, community_id)
        self.assertEqual(fetched_platform_id, platform_id)
