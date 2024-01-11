from bson import ObjectId
from datetime import datetime, timedelta
from unittest import TestCase

from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_msg_grouped
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchRawMessagesGrouped(TestCase):
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

        client["Module"].drop_collection("modules")
        client["Core"].drop_collection("platforms")

        if create_modules:
            data = {
                "name": "hivemind",
                "communityId": community_id,
                "options": {
                    "platforms": [
                        {
                            "platformId": platform_id,
                            "options": {
                                "channels": channels,
                                "roles": ["role_id"],
                                "users": ["user_id"],
                            },
                        }
                    ]
                },
            }
            client["Module"]["modules"].insert_one(data)

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

    def test_empty_data_empty_fromdate(self):
        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")

        messages = fetch_raw_msg_grouped(guild_id=guild_id, from_date=None)

        self.assertEqual(messages, [])

    def test_empty_data_non_empty_fromdate(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 9, 29)

        messages = fetch_raw_msg_grouped(guild_id=guild_id, from_date=from_date)

        self.assertEqual(messages, [])

    def test_some_data_available_empty_fromdate_single_channel_single_thread(self):
        channels = ["111111"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 9, 29)

        raw_data = []
        for i in range(3):
            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": f"11111{i}",
                "channelId": channels[0],
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        results = fetch_raw_msg_grouped(guild_id=guild_id, from_date=from_date)
        self.assertEqual(len(results), 3)

        for res in results:
            messages = res["messages"]
            print("messages", messages)

            if res["_id"]["date"] == "2023-10-01":
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0]["type"], 0)
                self.assertEqual(messages[0]["author"], "author_0")
                self.assertEqual(messages[0]["content"], "test_message 0")
                self.assertEqual(messages[0]["user_mentions"], [])
                self.assertEqual(messages[0]["role_mentions"], [])
                self.assertEqual(messages[0]["reactions"], [])
                self.assertEqual(messages[0]["replied_user"], None)
                self.assertEqual(messages[0]["createdDate"], datetime(2023, 10, 1))
                self.assertEqual(messages[0]["messageId"], "111110")
                self.assertEqual(messages[0]["channelId"], channels[0])
                self.assertEqual(messages[0]["channelName"], "general")
                self.assertEqual(messages[0]["threadId"], None)
                self.assertEqual(messages[0]["threadName"], None)
                self.assertEqual(messages[0]["isGeneratedByWebhook"], False)
            elif res["_id"]["date"] == "2023-10-02":
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0]["type"], 0)
                self.assertEqual(messages[0]["author"], "author_1")
                self.assertEqual(messages[0]["content"], "test_message 1")
                self.assertEqual(messages[0]["user_mentions"], [])
                self.assertEqual(messages[0]["role_mentions"], [])
                self.assertEqual(messages[0]["reactions"], [])
                self.assertEqual(messages[0]["replied_user"], None)
                self.assertEqual(messages[0]["createdDate"], datetime(2023, 10, 2))
                self.assertEqual(messages[0]["messageId"], "111111")
                self.assertEqual(messages[0]["channelId"], channels[0])
                self.assertEqual(messages[0]["channelName"], "general")
                self.assertEqual(messages[0]["threadId"], None)
                self.assertEqual(messages[0]["threadName"], None)
                self.assertEqual(messages[0]["isGeneratedByWebhook"], False)
            elif res["_id"]["date"] == "2023-10-03":
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0]["type"], 0)
                self.assertEqual(messages[0]["author"], "author_2")
                self.assertEqual(messages[0]["content"], "test_message 2")
                self.assertEqual(messages[0]["user_mentions"], [])
                self.assertEqual(messages[0]["role_mentions"], [])
                self.assertEqual(messages[0]["reactions"], [])
                self.assertEqual(messages[0]["replied_user"], None)
                self.assertEqual(messages[0]["createdDate"], datetime(2023, 10, 3))
                self.assertEqual(messages[0]["messageId"], "111112")
                self.assertEqual(messages[0]["channelId"], channels[0])
                self.assertEqual(messages[0]["channelName"], "general")
                self.assertEqual(messages[0]["threadId"], None)
                self.assertEqual(messages[0]["threadName"], None)
                self.assertEqual(messages[0]["isGeneratedByWebhook"], False)
            else:
                raise IndexError("Not possible, data shouldn't be here")

    def test_count_with_some_data_available_empty_fromdate_two_channel_single_thread(
        self,
    ):
        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )

        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 9, 29)

        raw_data = []
        for i in range(3):
            ch: str
            ch_id: str
            if i == 2:
                ch = "channel#2"
                ch_id = channels[1]
                day = datetime(2023, 10, 2)
            else:
                ch = "channel#1"
                ch_id = channels[0]
                day = datetime(2023, 10, 1)

            data = {
                "type": 0,
                "author": f"author_{i}",
                "content": f"test_message {i}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": day,
                "messageId": f"11111{i}",
                "channelId": ch_id,
                "channelName": ch,
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        results = fetch_raw_msg_grouped(guild_id=guild_id, from_date=from_date)

        self.assertEqual(len(results), 2)

        for res in results:
            messages = res["messages"]
            if res["_id"]["date"] == "2023-10-01":
                self.assertEqual(len(messages), 2)
            elif res["_id"]["date"] == "2023-10-02":
                self.assertEqual(len(messages), 1)
            else:
                raise IndexError("Not possible, data shouldn't be here")
