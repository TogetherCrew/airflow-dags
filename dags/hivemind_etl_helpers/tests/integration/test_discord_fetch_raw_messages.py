import unittest
from datetime import datetime

import numpy as np

from hivemind_etl_helpers.src.db.discord.fetch_raw_messages import fetch_raw_messages
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestFetchRawMessages(unittest.TestCase):
    def test_fetch_raw_messages_fetch_all(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"

        # droping any previous data
        client[guild_id].drop_collection("rawinfos")

        message_count = 2

        raw_data = []
        for _ in range(message_count):
            data = {
                "type": 0,
                "author": str(np.random.randint(100000, 999999)),
                "content": f"test_message {np.random.randint(0, 10)}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime.now(),
                "messageId": str(np.random.randint(1000000, 9999999)),
                "channelId": str(np.random.randint(10000000, 99999999)),
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        messages = fetch_raw_messages(guild_id, from_date=None)

        self.assertEqual(len(messages), message_count)

        for idx, msg in enumerate(messages):
            for field in msg.keys():
                if field != "createdDate":
                    self.assertEqual(msg[field], raw_data[idx][field])
                # date in milisecond in python and mongodb are slightly different
                # so we're considering out assertion for it
                else:
                    self.assertEqual(
                        msg[field].strftime("%Y-%m-%d %H:%M:%S"),
                        raw_data[idx][field].strftime("%Y-%m-%d %H:%M:%S"),
                    )

    def test_fetch_raw_messages_fetch_all_no_data_available(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"
        # droping any previous data
        client[guild_id].drop_collection("rawinfos")

        messages = fetch_raw_messages(guild_id, from_date=None)

        self.assertEqual(len(messages), 0)
        self.assertEqual(messages, [])

    def test_fetch_raw_messages_fetch_from_date(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"

        # Dropping any previous data
        client[guild_id].drop_collection("rawinfos")

        # Insert messages with different dates
        raw_data = []
        for i in range(5):
            data = {
                "type": 0,
                "author": str(np.random.randint(100000, 999999)),
                "content": f"test_message {np.random.randint(0, 10)}",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime(
                    2023, 10, i + 1
                ),  # Different dates in October 2023
                "messageId": str(np.random.randint(1000000, 9999999)),
                "channelId": str(np.random.randint(10000000, 99999999)),
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)

        # Fetch messages from a specific date (October 3, 2023)
        from_date = datetime(2023, 10, 3)
        messages = fetch_raw_messages(guild_id, from_date=from_date)

        # Check if the fetched messages have the correct date
        for message in messages:
            self.assertTrue(message["createdDate"] >= from_date)

        # Check if the number of fetched messages is correct
        expected_messages = [
            message for message in raw_data if message["createdDate"] >= from_date
        ]
        self.assertEqual(len(messages), len(expected_messages))

        # Check if the fetched messages are equal to the expected messages
        self.assertCountEqual(messages, expected_messages)
