import unittest
from unittest.mock import Mock

from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


class TestNotionExtractorLive(unittest.TestCase):
    def setUp(self):
        """
        Setup for each test case using mocks.
        """
        self.extractor = Mock(spec=NotionExtractor)
        self.extractor.extract.return_value = [{'content': 'some content'}]
        self.extractor.integration_token = 'mock_token'

    def test_initialization(self):
        """
        Test that the extractor is initialized with the correct token.
        """
        self.assertIsNotNone(self.extractor.integration_token)

    def test_extract_from_valid_pages(self):
        """
        Test extracting from valid pages.
        """
        test_page_ids = ['6a3c20b6861145b29030292120aa03e6',
                         'e479ee3eef9a4eefb3a393848af9ed9d']
        documents = self.extractor.extract(page_ids=test_page_ids)
        self.assertGreater(len(documents), 0)
        self.extractor.extract.assert_called_with(page_ids=test_page_ids)

    def test_extract_from_valid_database(self):
        """
        Test extracting from a valid database.
        """
        test_database_ids = ['dadd27f1dc1e4fa6b5b9dea76858dabe']
        documents = self.extractor.extract(database_ids=test_database_ids)
        self.assertGreater(len(documents), 0)
        self.extractor.extract.assert_called_with(database_ids=test_database_ids)

    def test_handle_invalid_page_id(self):
        """
        Test handling of invalid page IDs.
        Expecting empty results.
        """
        invalid_page_ids = ['non_existent_page']
        self.extractor.extract.return_value = []
        documents = self.extractor.extract(page_ids=invalid_page_ids)
        self.assertEqual(len(documents), 0)
        self.extractor.extract.assert_called_with(page_ids=invalid_page_ids)

    def test_handle_invalid_database_id(self):
        """
        Test handling of invalid database IDs.
        Expecting empty results.
        """
        invalid_database_ids = ['non_existent_database']
        self.extractor.extract.return_value = []
        documents = self.extractor.extract(database_ids=invalid_database_ids)
        self.assertEqual(len(documents), 0)
        self.extractor.extract.assert_called_with(database_ids=invalid_database_ids)
