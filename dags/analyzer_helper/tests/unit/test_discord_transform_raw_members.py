from datetime import datetime
import unittest

from dags.analyzer_helper.discord.discord_transform_raw_members import DiscordTransformRawMembers


class TestDiscordTransformRawMembers(unittest.TestCase):

    def test_transform_empty_list(self):
        """
        Tests that transform returns an empty list for an empty raw_members list
        """
        transformer = DiscordTransformRawMembers()
        result = transformer.transform([])
        self.assertEqual(result, [])

    def test_transform_single_member(self):
        """
        Tests that transform correctly transforms a single member
        """
        transformer = DiscordTransformRawMembers()
        raw_member = {
            "discordId": "123456789012345678",
            "isBot": False,
            "joinedAt": datetime(2023, 1, 1),
            "username": "test_user",
            "avatar": "avatar_url",
            "roles": ["role1", "role2"],
            "discriminator": "1234",
            "permissions": "some_permissions",
            "globalName": "Global Username",
            "nickname": "Test Nickname"
        }
        expected_result = {
            "id": 123456789012345678,
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 1, 1),
            "options": {
                "username": "test_user",
                "avatar": "avatar_url",
                "roles": ["role1", "role2"],
                "discriminator": "1234",
                "permissions": "some_permissions",
                "global_name": "Global Username",
                "nickname": "Test Nickname"
            }
        }
        result = transformer.transform([raw_member])
        self.assertEqual(result, [expected_result])

    def test_transform_multiple_members(self):
        """
        Tests that transform correctly transforms multiple members
        """
        transformer = DiscordTransformRawMembers()
        raw_member1 = {
            "discordId": "123456789012345678",
            "isBot": True,
            "joinedAt": datetime(2022, 12, 31),
            "username": "bot_user",
            "avatar": None,
            "roles": ["admin"],
            "discriminator": "9999",
            "permissions": "all",
            "globalName": None,
            "nickname": None
        }
        raw_member2 = {
            "discordId": "987654321098765432",
            "isBot": False,
            "joinedAt": datetime(2023, 1, 2),
            "username": "regular_user",
            "avatar": "user_avatar.jpg",
            "roles": ["member", "vip"],
            "discriminator": "0000",
            "permissions": "read_messages",
            "globalName": "GlobalUser",
            "nickname": "Regular"
        }
        expected_result1 = {
            "id": 123456789012345678,
            "is_bot": True,
            "left_at": None,
            "joined_at": datetime(2022, 12, 31),
            "options": {
                "username": "bot_user",
                "avatar": None,
                "roles": ["admin"],
                "discriminator": "9999",
                "permissions": "all",
                "global_name": None,
                "nickname": None
            }
        }
        expected_result2 = {
            "id": 987654321098765432,
            "is_bot": False,
            "left_at": None,
            "joined_at": datetime(2023, 1, 2),
            "options": {
                "username": "regular_user",
                "avatar": "user_avatar.jpg",
                "roles": ["member", "vip"],
                "discriminator": "0000",
                "permissions": "read_messages",
                }
        }
        result = transformer.transform([raw_member1, raw_member2])
        self.assertEqual(result, [expected_result1, expected_result2])
