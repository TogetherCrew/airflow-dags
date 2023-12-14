import unittest
from datetime import datetime
from uuid import uuid1

import numpy as np

from hivemind_etl_helpers.src.db.discord.utils.id_transform import convert_user_id
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestUserIdConvert(unittest.TestCase):
    def test_convert_user_id_to_name(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("guildmembers")

        members_data = []
        user_ids = ["123322", "23543", "897390", "3209812", "791629"]
        for i, user_id in enumerate(user_ids):
            member = {
                "discordId": user_id,
                "username": f"user{i}",
                "roles": [],
                "joinedAt": datetime(2023, 5, 1 + i),
                "avatar": str(uuid1()),
                "isBot": False,
                "discriminator": "0",
                "permissions": str(np.random.randint(10000, 99999)),
                "deletedAt": None,
                "globalName": f"user{i}GlobalName",
                "nickname": None,
            }
            members_data.append(member)

        client[guild_id]["guildmembers"].insert_many(members_data)

        usernames, global_names, nicknames = convert_user_id(guild_id, user_ids)

        self.assertEqual(len(usernames), 5)
        self.assertEqual(usernames, ["user0", "user1", "user2", "user3", "user4"])
        self.assertEqual(
            global_names,
            [
                "user0GlobalName",
                "user1GlobalName",
                "user2GlobalName",
                "user3GlobalName",
                "user4GlobalName",
            ],
        )
        self.assertEqual(nicknames, [None, None, None, None, None])

    def test_convert_user_id_to_name_order_changed(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("guildmembers")

        members_data = []
        user_ids = ["123322", "23543", "897390", "3209812", "791629"]
        for i, user_id in enumerate(user_ids):
            member = {
                "discordId": user_id,
                "username": f"user{i}",
                "roles": [],
                "joinedAt": datetime(2023, 5, 1 + i),
                "avatar": str(uuid1()),
                "isBot": False,
                "discriminator": "0",
                "permissions": str(np.random.randint(10000, 99999)),
                "deletedAt": None,
                "globalName": f"user{i}GlobalName",
                "nickname": None,
            }
            members_data.append(member)

        client[guild_id]["guildmembers"].insert_many(members_data)

        # changing id orders
        user_ids_change_order = ["791629", "23543", "897390", "3209812", "123322"]
        usernames, global_names, nicknames = convert_user_id(
            guild_id, user_ids_change_order
        )

        self.assertEqual(len(usernames), 5)
        self.assertEqual(usernames, ["user4", "user1", "user2", "user3", "user0"])
        self.assertEqual(
            global_names,
            [
                "user4GlobalName",
                "user1GlobalName",
                "user2GlobalName",
                "user3GlobalName",
                "user0GlobalName",
            ],
        )
        self.assertEqual(nicknames, [None, None, None, None, None])
