import unittest
from datetime import datetime

import numpy as np
from hivemind_etl_helpers.src.db.discord.utils.transform_discord_raw_messges import (
    transform_discord_raw_messages,
)
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestTransformRawMsgToDocument(unittest.TestCase):
    def test_transform_two_data(self):
        client = MongoSingleton.get_instance().client

        guild_id = "1234"

        # droping any previous data
        client[guild_id].drop_collection("guildmembers")

        messages = []
        data = {
            "type": 0,
            "author": "111",
            "content": "test_message1",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 1),
            "messageId": "1111111110",
            "channelId": "1313130",
            "channelName": "channel1",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 19,
            "author": "112",
            "content": "mentioning a person <@113>",
            "user_mentions": ["113", "114"],
            "role_mentions": [],
            "reactions": [],
            "replied_user": "114",
            "createdDate": datetime(2023, 5, 2),
            "messageId": "1111111111",
            "channelId": "1313131",
            "channelName": "channel2",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 19,
            "author": "112",
            "content": "mentioning <@113> <@114> <@&101>",
            "user_mentions": ["113", "114"],
            "role_mentions": ["101"],
            "reactions": [],
            "replied_user": "114",
            "createdDate": datetime(2023, 5, 2),
            "messageId": "1111111112",
            "channelId": "1313132",
            "channelName": "channel2",
            "threadId": "88888",
            "threadName": "example_thread1",
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        data = {
            "type": 0,
            "author": "111",
            "content": "test_message1 https://www.google.com",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 8),
            "messageId": "1111111113",
            "channelId": "1313133",
            "channelName": "channel1",
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        members_data = []
        member = {
            "discordId": "111",
            "username": "user1",
            "roles": [],
            "joinedAt": datetime(2023, 5, 1),
            "avatar": "sample_avatar1",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user1_GlobalName",
            "nickname": "user1_nickname",
        }
        members_data.append(member)
        member = {
            "discordId": "112",
            "username": "user2",
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
            "username": "user3",
            "roles": [],
            "joinedAt": datetime(2023, 5, 2),
            "avatar": "sample_avatar3",
            "isBot": False,
            "discriminator": "0",
            "permissions": str(np.random.randint(10000, 99999)),
            "deletedAt": None,
            "globalName": "user3_GlobalName",
            "nickname": None,
        }
        members_data.append(member)
        member = {
            "discordId": "114",
            "username": "user4",
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
                "name": "role1",
                "color": 3066993,
                "deletedAt": None,
            }
        )

        documents = transform_discord_raw_messages(guild_id, messages)

        self.assertEqual(len(documents), 4)

        expected_metadata_0 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 1).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
            "author_nickname": "user1_nickname",
            "url": f"https://discord.com/channels/{guild_id}/1313130/1111111110",
            "thread": None,
        }

        expected_metadata_1 = {
            "channel": "channel2",
            "date": datetime(2023, 5, 2).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user2",
            "author_global_name": "user2_GlobalName",
            "mention_usernames": ["user3", "user4"],
            "mention_global_names": ["user3_GlobalName", "user4_GlobalName"],
            "replier_username": "user4",
            "replier_global_name": "user4_GlobalName",
            "url": f"https://discord.com/channels/{guild_id}/1313131/1111111111",
            "thread": None,
        }

        expected_metadata_2 = {
            "channel": "channel2",
            "date": datetime(2023, 5, 2).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user2",
            "author_global_name": "user2_GlobalName",
            "mention_usernames": ["user3", "user4"],
            "mention_global_names": ["user3_GlobalName", "user4_GlobalName"],
            "replier_username": "user4",
            "replier_global_name": "user4_GlobalName",
            "url": f"https://discord.com/channels/{guild_id}/88888/1111111112",
            "thread": "example_thread1",
            "role_mentions": ["role1"],
        }

        expected_metadata_3 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 8).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
            "author_nickname": "user1_nickname",
            "url_reference": {"[URL0]": "https://www.google.com"},
            "url": f"https://discord.com/channels/{guild_id}/1313133/1111111113",
            "thread": None,
        }

        self.assertDictEqual(documents[0].metadata, expected_metadata_0)
        self.assertDictEqual(documents[1].metadata, expected_metadata_1)
        self.assertDictEqual(documents[2].metadata, expected_metadata_2)
        self.assertDictEqual(documents[3].metadata, expected_metadata_3)

        # Optionally, you can also check the text separately if needed
        self.assertEqual(documents[0].text, "test_message1")
        self.assertEqual(documents[1].text, "mentioning a person user3")
        self.assertEqual(documents[2].text, "mentioning user3 user4 role1")
        self.assertEqual(documents[3].text, "test_message1 [URL0]")
