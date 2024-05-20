import unittest
from unittest.mock import Mock

from hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


class TestNotionExtractorLive(unittest.TestCase):
    def setUp(self):
        """
        Setup for each test case with a direct Mock of NotionPageReader.
        """
        self.notion_token = "mock_token"
        self.extractor = NotionExtractor(self.notion_token)
        self.mock_reader = Mock(spec=self.extractor.notion_page_reader)
        self.extractor.notion_page_reader = self.mock_reader

    def test_initialization(self):
        """
        Test that the extractor is initialized with the correct token.
        """
        self.assertEqual(self.extractor.integration_token, self.notion_token)

    def test_extract_from_valid_pages(self):
        """
        Test extracting from valid pages.
        """
        mock_response = [Mock(), Mock()]
        self.mock_reader.load_data.return_value = mock_response

        test_page_ids = ["6a3c20b6861145b29030292120aa03e6"]
        documents = self.extractor.extract(page_ids=test_page_ids)
        self.assertEqual(len(documents), len(mock_response))
        self.mock_reader.load_data.assert_called_once_with(page_ids=test_page_ids)

    def test_handle_invalid_page_id(self):
        """
        Test handling of invalid page IDs.
        Expecting empty results.
        """
        invalid_page_ids = ["non_existent_page"]
        self.mock_reader.load_data.return_value = []

        documents = self.extractor.extract(page_ids=invalid_page_ids)
        self.assertEqual(len(documents), 0)
        self.mock_reader.load_data.assert_called_with(page_ids=invalid_page_ids)

    def test_handle_invalid_database_id(self):
        """
        Test handling of invalid database IDs.
        Expecting empty results.
        """
        invalid_database_ids = ["non_existent_database"]
        self.mock_reader.load_data.return_value = []

        documents = self.extractor.extract(database_ids=invalid_database_ids)
        self.assertEqual(len(documents), 0)
        self.mock_reader.load_data.assert_called_with(
            database_id=invalid_database_ids[0]
        )

    def test_extract_from_valid_database(self):
        """
        Test extracting from a valid database.
        """
        test_database_ids = ["dadd27f1dc1e4fa6b5b9dea76858dabe"]
        mock_response = [Mock(), Mock()]
        self.mock_reader.load_data.return_value = mock_response

        documents = self.extractor.extract(database_ids=test_database_ids)
        self.assertEqual(len(documents), len(mock_response))
        self.mock_reader.load_data.assert_called_once_with(
            database_id=test_database_ids[0]
        )
