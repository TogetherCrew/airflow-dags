import unittest
from datetime import datetime

import numpy as np

from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    discord_raw_to_docuemnts,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestTransformRawMsgToDocument(unittest.TestCase):
    def test_transform_two_data(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"

        # droping any previous data
        client[guild_id].drop_collection("guildmembers")
        client[guild_id].drop_collection("rawinfos")

        messages = []
        data = {
            "type": 0,
            "author": "111",
            "content": f"test_message1",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 1),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": str(np.random.randint(10000000, 99999999)),
            "channelName": "channel1",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 19,
            "author": "112",
            "content": f"mentioning a person <@113>",
            "user_mentions": ["113", "114"],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "114",
            "createdDate": datetime(2023, 5, 2),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": str(np.random.randint(10000000, 99999999)),
            "channelName": "channel2",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 19,
            "author": "112",
            "content": f"mentioning <@113> <@114> <@101>",
            "user_mentions": ["113", "114"],
            "role_mentions": ["101"],
            "reactions": [],
            "replied_user": "114",
            "createdDate": datetime(2023, 5, 2),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": str(np.random.randint(10000000, 99999999)),
            "channelName": "channel2",
            "threadId": "88888",
            "threadName": "example_thread1",
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 0,
            "author": "111",
            "content": f"test_message1 https://www.google.com",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 8),
            "messageId": str(np.random.randint(1000000, 9999999)),
            "channelId": str(np.random.randint(10000000, 99999999)),
            "channelName": "channel1",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)
        client[guild_id]["rawinfos"].insert_many(messages)

        members_data = []
        member = {
            "discordId": "111",
            "username": f"user1",
            "roles": [],
            "joinedAt": datetime(2023, 5, 1),
            "avatar": "sample_avatar1",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user1_GlobalName",
            "nickname": None,
        }
        members_data.append(member)
        member = {
            "discordId": "112",
            "username": f"user2",
            "roles": [],
            "joinedAt": datetime(2023, 5, 2),
            "avatar": "sample_avatar2",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user2_GlobalName",
            "nickname": None,
        }
        members_data.append(member)
        member = {
            "discordId": "113",
            "username": f"user3",
            "roles": [],
            "joinedAt": datetime(2023, 5, 2),
            "avatar": "sample_avatar3",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user3_GlobalName",
            "nickname": "user3_nickname",
        }
        members_data.append(member)
        member = {
            "discordId": "114",
            "username": f"user4",
            "roles": [],
            "joinedAt": datetime(2023, 5, 3),
            "avatar": "sample_avatar4",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user4_GlobalName",
            "nickname": None,
        }
        members_data.append(member)

        client[guild_id]["guildmembers"].insert_many(members_data)
        client[guild_id]["roles"].insert_one(
            {
                "roleId": "101",
                "name": f"role1",
                "color": 3066993,
                "deletedAt": None,
            }
        )

        documents = discord_raw_to_docuemnts(guild_id, from_date=None)
        self.assertEqual(len(documents), 4)

        expected_metadata_0 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 1).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
        }

        expected_metadata_1 = {
            "channel": "channel2",
            "date": datetime(2023, 5, 2).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user2",
            "author_global_name": "user2_GlobalName",
            "mention_usernames": ["user3", "user4"],
            "mention_global_names": ["user3_GlobalName", "user4_GlobalName"],
            "mention_nicknames": ["user3_nickname"],
            "replier_username": "user4",
            "replier_global_name": "user4_GlobalName",
        }

        expected_metadata_2 = {
            "channel": "channel2",
            "date": datetime(2023, 5, 2).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user2",
            "author_global_name": "user2_GlobalName",
            "mention_usernames": ["user3", "user4"],
            "mention_global_names": ["user3_GlobalName", "user4_GlobalName"],
            "mention_nicknames": ["user3_nickname"],
            "replier_username": "user4",
            "replier_global_name": "user4_GlobalName",
            "thread_name": "example_thread1",
            "role_mentions": ["role1"],
        }

        expected_metadata_3 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 8).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
            "url_reference": {"[URL0]": "https://www.google.com"},
        }
        print(documents[3].metadata)
        self.assertDictEqual(documents[0].metadata, expected_metadata_0)
        self.assertDictEqual(documents[1].metadata, expected_metadata_1)
        self.assertDictEqual(documents[2].metadata, expected_metadata_2)
        self.assertDictEqual(documents[3].metadata, expected_metadata_3)

        # Optionally, you can also check the text separately if needed
        self.assertEqual(documents[0].text, "test_message1")
        self.assertEqual(documents[1].text, "mentioning a person user3")
        self.assertEqual(documents[2].text, "mentioning user3 user4 role1")
        self.assertEqual(documents[3].text, "test_message1 [URL0]")
