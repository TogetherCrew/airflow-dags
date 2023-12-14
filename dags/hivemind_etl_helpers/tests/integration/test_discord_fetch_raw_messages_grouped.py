from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_msg_grouped
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchRawMessagesGrouped(TestCase):
    def test_empty_data_empty_fromdate(self):
        guild_id = "1234"
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
        guild_id = "1234"
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
                "channelId": "12454123",
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
                self.assertEqual(messages[0]["channelId"], "12454123")
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
                self.assertEqual(messages[0]["channelId"], "12454123")
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
                self.assertEqual(messages[0]["channelId"], "12454123")
                self.assertEqual(messages[0]["channelName"], "general")
                self.assertEqual(messages[0]["threadId"], None)
                self.assertEqual(messages[0]["threadName"], None)
                self.assertEqual(messages[0]["isGeneratedByWebhook"], False)
            else:
                raise IndexError("Not possible, data shouldn't be here")

    def test_count_with_some_data_available_empty_fromdate_two_channel_single_thread(
        self,
    ):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 9, 29)

        raw_data = []
        for i in range(3):
            ch: str
            ch_id: str
            if i == 2:
                ch = "channel#2"
                ch_id = "112"
                day = datetime(2023, 10, 2)
            else:
                ch = "channel#1"
                ch_id = "111"
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
