import unittest
from datetime import datetime
import spacy

from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    update_raw_messages,
)


class TestUpdateRawMessages(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Load spacy model once for all tests"""
        try:
            cls.nlp = spacy.load("en_core_web_lg")
        except OSError as exp:
            raise OSError(f"Model spacy `en_core_web_lg` is not installed!") from exp

    def test_empty_list(self):
        """Test function handles empty input list correctly"""
        result = update_raw_messages([])
        self.assertEqual(result, [], "Empty list should return empty list")

    def test_with_content(self):
        """Test function processes valid content correctly"""
        test_data = [
            {
                "type": 0,
                "author": "123456",
                "content": "test_message <@123456> Hello",
                "user_mentions": [],
                "role_mentions": [],
                "reactions": [],
                "replied_user": None,
                "createdDate": datetime.now(),
                "messageId": "1234567",
                "channelId": "89101112",
                "channelName": "general",
                "threadId": None,
                "threadName": None,
                "isGeneratedByWebhook": False,
            }
        ]

        result = update_raw_messages(test_data)

        self.assertEqual(len(result), 1, "Should return one processed message")
        self.assertIn("content", result[0], "Processed message should contain content")
        # The actual content will depend on spaCy's processing, but should not contain Discord IDs
        self.assertNotIn(
            "<@123456>", result[0]["content"], "Discord IDs should be removed"
        )
        self.assertNotEqual(result[0]["content"], "", "Content should not be empty")

    def test_without_content(self):
        """Test function handles messages without content field"""
        test_data = [
            {
                "type": 0,
                "author": "123456",
                "user_mentions": [],
                "role_mentions": [],
            }
        ]

        result = update_raw_messages(test_data)
        self.assertEqual(result, [], "Message without content should be filtered out")

    def test_with_discord_ids(self):
        """Test function removes Discord user and role IDs correctly"""
        test_data = [
            {
                "content": "Hello <@123456> and <@&789012> how are you?",
                "type": 0,
            }
        ]

        result = update_raw_messages(test_data)
        self.assertEqual(len(result), 1, "Should return one processed message")
        # Check that Discord IDs are removed
        self.assertNotIn("<@123456>", result[0]["content"], "User ID should be removed")
        self.assertNotIn(
            "<@&789012>", result[0]["content"], "Role ID should be removed"
        )
        # Check that meaningful content remains
        self.assertIn(
            "hello", result[0]["content"].lower(), "Regular words should be preserved"
        )

    def test_consecutive_calls(self):
        """Test function handles consecutive calls correctly"""
        test_data_1 = [{"content": "Apples are falling from trees!"}]
        test_data_2 = [{"content": "second message"}]

        result_1 = update_raw_messages(test_data_1)
        result_2 = update_raw_messages(test_data_2)

        self.assertEqual(len(result_1), 1, "First call should return one message")
        self.assertEqual(len(result_2), 1, "Second call should return one message")
        self.assertNotEqual(
            result_1[0]["content"],
            result_2[0]["content"],
            "Different inputs should produce different outputs",
        )

    def test_multiple_messages(self):
        """Test function processes multiple messages correctly"""
        test_data = [
            {"content": "first message about cats"},
            {"content": "second message about dogs"},
            {"content": "third message about birds"},
        ]

        result = update_raw_messages(test_data)
        self.assertEqual(len(result), 3, "Should return three processed messages")

        # Check that each message has been processed
        for i, msg in enumerate(result):
            self.assertIn("content", msg, f"Message {i} should have content")
            self.assertNotEqual(
                msg["content"], "", f"Message {i} content should not be empty"
            )

    def test_special_characters(self):
        """Test function handles special characters correctly"""
        test_data = [
            {
                "content": "Hello! This is a test... With some punctuation!?! @#$%",
                "type": 0,
            }
        ]

        result = update_raw_messages(test_data)
        self.assertEqual(len(result), 1, "Should return one processed message")
        # The processed content should contain meaningful words but not punctuation
        processed_content = result[0]["content"].lower()
        self.assertIn("hello", processed_content, "Common words should be preserved")
        self.assertIn("test", processed_content, "Common words should be preserved")
        self.assertNotIn(
            "!?!", processed_content, "Multiple punctuation should be removed"
        )
