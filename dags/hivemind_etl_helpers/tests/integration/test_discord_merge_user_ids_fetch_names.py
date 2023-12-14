import unittest
from datetime import datetime
from uuid import uuid1

import numpy as np

from hivemind_etl_helpers.src.db.discord.utils.merge_user_ids_fetch_names import (
    merge_user_ids_and_fetch_names,
)
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TestMergeUserIdsFetchNames(unittest.TestCase):
    def test_merge_ids_fetch_names_one_input(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("guildmembers")

        user_cat1_ids = ["111", "222", "333", "444", "555"]

        all_users = []
        all_users.extend(user_cat1_ids)

        members_data = []
        for i, user_id in enumerate(all_users):
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
                "globalName": f"user{i}_GlobalName",
                "nickname": None,
            }
            members_data.append(member)

        client[guild_id]["guildmembers"].insert_many(members_data)

        (
            user_names,
            global_names,
            nicknames,
        ) = merge_user_ids_and_fetch_names(guild_id, user_cat1_ids)

        # 1 array was inserted
        self.assertEqual(len(user_names), 1)
        self.assertEqual(user_names[0], ["user0", "user1", "user2", "user3", "user4"])

        self.assertEqual(len(global_names), 1)
        self.assertEqual(
            global_names[0],
            [
                "user0_GlobalName",
                "user1_GlobalName",
                "user2_GlobalName",
                "user3_GlobalName",
                "user4_GlobalName",
            ],
        )

        self.assertEqual(len(nicknames), 1)
        self.assertEqual(nicknames[0], [None, None, None, None, None])

    def test_merge_ids_fetch_names_three_inputs(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("guildmembers")

        user_cat1_ids = ["111", "222", "333", "444", "555"]
        user_cat2_ids = ["123", "321", "132", "1123"]
        user_cat3_ids = ["54321", "45312", "125", "521", "115"]

        all_users = []
        all_users.extend(user_cat1_ids)
        all_users.extend(user_cat2_ids)
        all_users.extend(user_cat3_ids)
        members_data = []
        for i, user_id in enumerate(all_users):
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
                "globalName": f"user{i}_GlobalName",
                "nickname": None,
            }
            members_data.append(member)

        client[guild_id]["guildmembers"].insert_many(members_data)

        (
            (
                user_names_cat1,
                user_names_cat2,
                user_names_cat3,
            ),
            (
                global_names_cat1,
                global_names_cat2,
                global_names_cat3,
            ),
            (
                nicknames_cat1,
                nicknames_cat2,
                nicknames_cat3,
            ),
        ) = merge_user_ids_and_fetch_names(
            guild_id, user_cat1_ids, user_cat2_ids, user_cat3_ids
        )

        self.assertEqual(user_names_cat1, ["user0", "user1", "user2", "user3", "user4"])
        self.assertEqual(user_names_cat2, ["user5", "user6", "user7", "user8"])
        self.assertEqual(
            user_names_cat3, ["user9", "user10", "user11", "user12", "user13"]
        )

        self.assertEqual(
            global_names_cat1,
            [
                "user0_GlobalName",
                "user1_GlobalName",
                "user2_GlobalName",
                "user3_GlobalName",
                "user4_GlobalName",
            ],
        )
        self.assertEqual(
            global_names_cat2,
            [
                "user5_GlobalName",
                "user6_GlobalName",
                "user7_GlobalName",
                "user8_GlobalName",
            ],
        )
        self.assertEqual(
            global_names_cat3,
            [
                "user9_GlobalName",
                "user10_GlobalName",
                "user11_GlobalName",
                "user12_GlobalName",
                "user13_GlobalName",
            ],
        )

        self.assertEqual(nicknames_cat1, [None, None, None, None, None])
        self.assertEqual(nicknames_cat2, [None, None, None, None])
        self.assertEqual(nicknames_cat3, [None, None, None, None, None])

    def test_merge_ids_fetch_names_no_input(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("guildmembers")

        user_names, global_name, nicknames = merge_user_ids_and_fetch_names(guild_id)
        # 1 array was inserted
        self.assertEqual(len(user_names), 0)
        self.assertEqual(len(global_name), 0)
        self.assertEqual(len(nicknames), 0)
