import unittest
from unittest.mock import Mock

from hivemind_etl_helpers.src.db.mediawiki.extractor import MediaWikiExtractor
from llama_index.core import Document


class TestMediaWikiExtractor(unittest.TestCase):
    def setUp(self):
        """
        Setup for each test case with a direct Mock of MediaWikiReader.
        """
        self.api_url = "https://en.wikipedia.org/w/api.php"
        self.extractor = MediaWikiExtractor(api_url=self.api_url)
        self.mock_reader = Mock(spec=self.extractor.wikimedia_reader)
        self.extractor.wikimedia_reader = self.mock_reader

    def test_initialization_with_api_url(self):
        """
        Test that the extractor is initialized with the correct API URL.
        """
        self.assertEqual(self.extractor.api_url, self.api_url)

    def test_extract_from_valid_pages(self):
        """
        Test extracting from valid pages.
        """
        mock_response = [Mock(spec=Document), Mock(spec=Document)]
        self.mock_reader.load_data.return_value = mock_response

        test_pages = ["Python_(programming_language)", "OpenAI"]
        documents = self.extractor.extract(page_ids=test_pages)
        self.assertEqual(len(documents), len(mock_response))
        self.mock_reader.load_data.assert_called_once_with(pages=test_pages,
                                                           auto_suggest=False)

    def test_extract_no_pages(self):
        """
        Test extracting with no pages provided.
        Expecting empty results.
        """
        documents = self.extractor.extract()
        self.assertEqual(len(documents), 0)
        self.mock_reader.load_data.assert_not_called()

    def test_handle_invalid_page_titles(self):
        """
        Test handling of invalid page titles.
        Expecting empty results.
        """
        invalid_pages = ["Non_existent_page"]
        self.mock_reader.load_data.return_value = []

        documents = self.extractor.extract(page_ids=invalid_pages)
        self.assertEqual(len(documents), 0)
        self.mock_reader.load_data.assert_called_with(pages=invalid_pages,
                                                      auto_suggest=False)
