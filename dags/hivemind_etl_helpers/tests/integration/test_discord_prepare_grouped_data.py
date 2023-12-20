from datetime import datetime
from unittest import TestCase

from hivemind_etl_helpers.src.db.discord.summary.prepare_grouped_data import (
    prepare_grouped_data,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestDiscordGroupedDataPreparation(TestCase):
    def test_empty_data_prepare_without_per_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")

        data = prepare_grouped_data(guild_id=guild_id, from_date=None)

        self.assertEqual(data, {})

    def test_empty_data_prepare_with_from_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 8, 1)

        data = prepare_grouped_data(guild_id=guild_id, from_date=from_date)
        self.assertEqual(data, {})

    def test_some_data_prepare_with_from_date(self):
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 8, 1)

        raw_data = []
        for i in range(2):
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
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
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
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
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
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        data = prepare_grouped_data(guild_id=guild_id, from_date=from_date)

        self.assertEqual(set(data.keys()), set(["2023-10-01", "2023-10-02"]))
        for date in data.keys():
            if date == "2023-10-01":
                for channel in data[date].keys():
                    if channel == "reading":
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            [None],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel][None]), 1)
                    elif channel == "writing":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Available"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Available"]), 1)
                    elif channel == "general":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Something"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Something"]), 1)
            elif date == "2023-10-02":
                for channel in data[date].keys():
                    if channel == "reading":
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            [None],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel][None]), 1)
                    elif channel == "writing":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Available"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Available"]), 1)
                    elif channel == "general":
                        # the thread
                        self.assertEqual(len(data[date][channel].keys()), 1)
                        self.assertEqual(
                            list(data[date][channel].keys()),
                            ["Something"],
                        )
                        # 1 message were there
                        self.assertEqual(len(data[date][channel]["Something"]), 1)
            else:
                raise IndexError("Not possible, it shouldn't reach here")

    def test_some_data_prepare_after_from_date(self):
        """
        should return no data as we're getting them after the specific date
        """
        guild_id = "1234"
        client = MongoSingleton.get_instance().client
        client[guild_id].drop_collection("rawinfos")
        from_date = datetime(2023, 11, 1)

        raw_data = []
        for i in range(2):
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
                "threadName": "Something",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
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
                "channelName": "writing",
                "threadId": "123443211",
                "threadName": "Available",
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        for i in range(2):
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
                "channelName": "reading",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
            raw_data.append(data)

        client[guild_id]["rawinfos"].insert_many(raw_data)
        data = prepare_grouped_data(guild_id=guild_id, from_date=from_date)

        self.assertEqual(data, {})
