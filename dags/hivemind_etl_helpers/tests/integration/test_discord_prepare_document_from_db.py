import unittest
from datetime import datetime, timedelta

import numpy as np
from bson import ObjectId
from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    discord_raw_to_documents,
)
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestTransformRawMsgToDocument(unittest.TestCase):
    def setup_db(
        self,
        channels: list[str],
        create_modules: bool = True,
        create_platform: bool = True,
        guild_id: str = "1234",
    ):
        client = MongoSingleton.get_instance().get_client()

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
                            "fromDate": datetime(2023, 1, 1),
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

    def test_transform_two_data(self):
        client = MongoSingleton.get_instance().get_client()

        channels = ["111111", "22222"]
        guild_id = "1234"
        self.setup_db(
            channels=channels,
            guild_id=guild_id,
        )
        # droping any previous data
        client[guild_id].drop_collection("guildmembers")
        client[guild_id].drop_collection("rawinfos")
        client[guild_id].drop_collection("channels")
        client[guild_id].drop_collection("threads")

        client[guild_id]["channels"].insert_many(
            [
                {
                    "channelId": channels[0],
                    "name": "channel1",
                    "parentId": None,

                },
                {
                    "channelId": channels[1],
                    "name": "channel2",
                    "parentId": None,
                },
            ]
        )

        client[guild_id]["threads"].insert_one(
            {
                "id": "88888",
                "name": "example_thread1",
            }
        )
        

        messages = []
        data = {
            "type": 0,
            "author": "111",
            "content": "test_message1 making it longer!",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 1),
            "messageId": "10000000000",
            "channelId": channels[0],
            "channelName": None,
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
            "messageId": "10000000001",
            "channelId": channels[1],
            "channelName": None,
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
            "messageId": "10000000002",
            "channelId": channels[1],
            "channelName": None,
            "threadId": "88888",
            "threadName": None,
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
            "messageId": "10000000003",
            "channelId": channels[0],
            "channelName": None,
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        # this data is not in our selected channels of modules
        # it shouldn't be included in documents
        data = {
            "type": 0,
            "author": "111",
            "content": "test_message1 https://www.example.com",
            "user_mentions": [],
            "role_mentions": [],
            "reactions": [],
            "replied_user": None,
            "createdDate": datetime(2023, 5, 8),
            "messageId": "10000000004",
            "channelId": "734738382",
            "channelName": None,
            "threadId": None,
            "threadName": None,
            "isGeneratedByWebhook": False,
        }
        messages.append(data)

        client[guild_id]["rawinfos"].insert_many(messages)

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
            "nickname": None,
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
            "nickname": "user3_nickname",
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

        documents = discord_raw_to_documents(
            guild_id,
            selected_channels=channels,
            from_date=datetime(2023, 1, 1),
        )
        self.assertEqual(len(documents), 4)

        expected_metadata_0 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 1).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
            "thread": None,
            "url": "https://discord.com/channels/1234/111111/10000000000",
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
            "thread": None,
            "url": "https://discord.com/channels/1234/22222/10000000001",
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
            "thread": "example_thread1",
            "role_mentions": ["role1"],
            "url": "https://discord.com/channels/1234/88888/10000000002",
        }

        expected_metadata_3 = {
            "channel": "channel1",
            "date": datetime(2023, 5, 8).strftime("%Y-%m-%d %H:%M:%S"),
            "author_username": "user1",
            "author_global_name": "user1_GlobalName",
            "thread": None,
            "url": "https://discord.com/channels/1234/111111/10000000003",
        }

        self.assertDictEqual(documents[0].metadata, expected_metadata_0)
        self.assertDictEqual(documents[1].metadata, expected_metadata_1)
        self.assertDictEqual(documents[2].metadata, expected_metadata_2)
        self.assertDictEqual(documents[3].metadata, expected_metadata_3)

        self.assertEqual(documents[0].text, "test_message1 making it longer!")
        self.assertEqual(documents[1].text, "mentioning a person user3")
        self.assertEqual(documents[2].text, "mentioning user3 user4 role1")
        self.assertEqual(documents[3].text, "test_message1 https://www.google.com")
