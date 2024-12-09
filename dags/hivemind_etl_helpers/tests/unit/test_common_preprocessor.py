import unittest
from unittest.mock import patch
import spacy

from hivemind_etl_helpers.src.db.common.base_preprocessor import BasePreprocessor


class TestDiscordPreprocessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.preprocessor = BasePreprocessor()

    # Tests for extract_main_content method
    @unittest.skipIf(
        not spacy.util.is_package("en_core_web_lg"), "requires en_core_web_lg model"
    )
    def test_extract_main_content_basic(self):
        """Test basic content extraction"""
        input_text = "The quick brown fox jumps over the lazy dog!"
        result = self.preprocessor.extract_main_content(input_text)
        # Note: Expected result will contain lemmatized words without stop words
        self.assertIn("quick brown fox jump lazy dog", result.lower())

    @unittest.skipIf(
        not spacy.util.is_package("en_core_web_lg"), "requires en_core_web_lg model"
    )
    def test_extract_main_content_with_numbers(self):
        """Test content extraction with numbers"""
        input_text = "I have 5 apples and 3 oranges."
        result = self.preprocessor.extract_main_content(input_text)
        self.assertIn("apple orange", result.lower())
        self.assertNotIn("5", result)
        self.assertNotIn("3", result)

    @unittest.skipIf(
        not spacy.util.is_package("en_core_web_lg"), "requires en_core_web_lg model"
    )
    def test_extract_main_content_with_urls(self):
        """Test content extraction with URLs"""
        input_text = "Check this link: https://example.com for more information"
        result = self.preprocessor.extract_main_content(input_text)
        self.assertNotIn("https://example.com", result)
        self.assertIn("check link information", result.lower())

    @unittest.skipIf(
        not spacy.util.is_package("en_core_web_lg"), "requires en_core_web_lg model"
    )
    def test_extract_main_content_empty_string(self):
        """Test content extraction with empty string"""
        self.assertEqual(self.preprocessor.extract_main_content(""), "")

    def test_extract_main_content_missing_model(self):
        """Test handling of missing spacy model"""
        with patch("spacy.load") as mock_load:
            mock_load.side_effect = OSError("Model not found")

            with self.assertRaises(OSError) as context:
                self.preprocessor.extract_main_content("Test text")

            self.assertIn(
                "Model spacy `en_core_web_lg` is not installed!", str(context.exception)
            )
