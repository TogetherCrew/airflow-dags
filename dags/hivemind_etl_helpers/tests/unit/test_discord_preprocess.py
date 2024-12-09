import unittest

from hivemind_etl_helpers.src.db.discord.preprocessor import DiscordPreprocessor


class TestDiscordPreprocessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.preprocessor = DiscordPreprocessor()

    # Tests for remove_ids method
    def test_remove_user_ids(self):
        """Test removal of user IDs from text"""
        input_text = "Hello <@123456> how are you?"
        expected = "Hello how are you?"
        self.assertEqual(self.preprocessor.remove_ids(input_text), expected)

    def test_remove_role_ids(self):
        """Test removal of role IDs from text"""
        input_text = "Welcome <@&789012> to the server!"
        expected = "Welcome to the server!"
        self.assertEqual(self.preprocessor.remove_ids(input_text), expected)

    def test_remove_multiple_ids(self):
        """Test removal of multiple IDs from text"""
        input_text = "Hey <@123456> please notify <@&789012> and <@345678>"
        expected = "Hey please notify and"
        self.assertEqual(self.preprocessor.remove_ids(input_text), expected)

    def test_text_without_ids(self):
        """Test text that doesn't contain any IDs"""
        input_text = "Regular message without any IDs"
        self.assertEqual(self.preprocessor.remove_ids(input_text), input_text)

    def test_empty_string(self):
        """Test empty string input"""
        self.assertEqual(self.preprocessor.remove_ids(""), "")

    def test_only_whitespace(self):
        """Test string with only whitespace"""
        input_text = "   \t   \n   "
        expected = ""
        self.assertEqual(self.preprocessor.remove_ids(input_text), expected)
