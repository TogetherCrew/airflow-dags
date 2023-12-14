import unittest

from hivemind_etl_helpers.src.db.discord.utils.prepare_raw_message_ids import (
    prepare_raw_message_ids,
)


class TestPrepareRawMessage(unittest.TestCase):
    def test_normal_raw_message_preparation(self):
        msg_content = "Hello <@1234> and <@4321>"
        roles_mention = {
            "1234": "Everyone",
        }
        user_mention = {"4321": "user1_name"}
        msg = prepare_raw_message_ids(
            message=msg_content, roles=roles_mention, users=user_mention
        )
        self.assertEqual(msg, "Hello Everyone and user1_name")

    def test_raw_message_no_id(self):
        """
        test a raw message with no id within it
        """
        msg_content = "Good morning!"
        roles_mention = {
            "1234": "Everyone",
        }
        user_mention = {"4321": "user1_name"}
        msg = prepare_raw_message_ids(
            message=msg_content, roles=roles_mention, users=user_mention
        )
        self.assertEqual(msg, "Good morning!")

    def test_empty_raw_message(self):
        """
        test a raw message with no id within it
        """
        msg_content = ""
        roles_mention = {
            "1234": "Everyone",
        }
        user_mention = {"4321": "user1_name"}
        msg = prepare_raw_message_ids(
            message=msg_content, roles=roles_mention, users=user_mention
        )
        self.assertEqual(msg, "")
