import unittest

from hivemind_etl_helpers.src.db.discord.utils.id_transform import convert_role_id
from tc_hivemind_backend.db.mongo import MongoSingleton


class TestRoleIdConvert(unittest.TestCase):
    def test_convert_role_id_to_name(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("roles")

        roles_data = []
        role_ids = ["123322", "23543", "897390", "3209812", "791629"]
        for i, role_id in enumerate(role_ids):
            role = {
                "roleId": role_id,
                "name": f"role{i}",
                "color": 3066993,
                "deletedAt": None,
            }
            roles_data.append(role)

        client[guild_id]["roles"].insert_many(roles_data)

        usernames = convert_role_id(guild_id, role_ids)

        self.assertEqual(len(usernames), 5)
        self.assertEqual(usernames, ["role0", "role1", "role2", "role3", "role4"])

    def test_convert_role_id_to_name_order_changed(self):
        client = MongoSingleton.get_instance().client
        guild_id = "1234"

        client[guild_id].drop_collection("roles")

        roles_data = []
        role_ids = ["123322", "23543", "897390", "3209812", "791629"]
        for i, role_id in enumerate(role_ids):
            role = {
                "roleId": role_id,
                "name": f"role{i}",
                "color": 3066993,
                "deletedAt": None,
            }
            roles_data.append(role)

        client[guild_id]["roles"].insert_many(roles_data)

        # changing id orders
        user_ids_change_order = ["791629", "23543", "897390", "3209812", "123322"]
        usernames = convert_role_id(guild_id, user_ids_change_order)

        self.assertEqual(len(usernames), 5)
        self.assertEqual(usernames, ["role4", "role1", "role2", "role3", "role0"])
