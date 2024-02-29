from datetime import datetime, timedelta
from unittest import TestCase

from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import (
    fetch_channels_and_from_date,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordFetchModulesChannels(TestCase):
    def setup_db(
        self,
        channels: list[str],
        create_modules: bool = True,
        create_platform: bool = True,
        guild_id: str = "1234",
    ):
        client = MongoSingleton.get_instance().client

        community_id = ObjectId("9f59dd4f38f3474accdc8f24")
        platform_id = ObjectId("063a2a74282db2c00fbc2428")

        client["Core"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        if create_modules:
            data = {
                "name": "hivemind",
                "communityId": community_id,
                "options": {
                    "platforms": [
                        {
                            "platformId": platform_id,
                            "fromDate": datetime(2024, 1, 1),
                            "options": {
                                "channels": channels,
                                "roles": ["role_id"],
                                "users": ["user_id"],
                            },
                        }
                    ]
                },
            }
            client["Core"]["modules"].insert_one(data)

        if create_platform:
            client["Core"]["platforms"].insert_one(
                {
                    "_id": platform_id,
                    "name": "discord",
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
                        "id": guild_id,
                        "isInProgress": False,
                        "period": datetime.now() - timedelta(days=35),
                        "icon": "some_icon_hash",
                        "selectedChannels": channels,
                        "name": "GuildName",
                    },
                    "community": community_id,
                    "disconnectedAt": None,
                    "connectedAt": datetime.now(),
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                }
            )

    def test_fetch_channels(self):
        guild_id = "1234"
        channels = ["111111", "22222"]
        self.setup_db(
            create_modules=True,
            create_platform=True,
            guild_id=guild_id,
            channels=channels,
        )
        channels, from_date = fetch_channels_and_from_date(guild_id="1234")

        self.assertEqual(channels, channels)
        self.assertIsInstance(from_date, datetime)
        self.assertEqual(from_date, datetime(2024, 1, 1))

    def test_fetch_channels_no_modules_available(self):
        guild_id = "12345"
        channels = ["111111", "22222"]
        self.setup_db(
            create_modules=False,
            create_platform=True,
            guild_id=guild_id,
            channels=channels,
        )
        with self.assertRaises(ValueError):
            _ = fetch_channels_and_from_date(guild_id="1234")

    def test_fetch_channels_no_platform_available(self):
        guild_id = "12345"
        channels = ["111111", "22222"]
        self.setup_db(
            create_modules=True,
            create_platform=False,
            guild_id=guild_id,
            channels=channels,
        )

        with self.assertRaises(ValueError):
            _ = fetch_channels_and_from_date(guild_id="1234")
