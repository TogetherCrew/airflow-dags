import unittest

from llama_index_client import NotionPageReader

from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


class TestNotionExtractorLive(unittest.TestCase):
    def setUp(self):
        """
        Setup for each test case.
        """
        self.extractor = NotionExtractor()

    def test_initialization(self):
        """
        Test that the extractor is initialized with the correct token.
        """
        self.assertIsNotNone(self.extractor.integration_token)
        self.assertIsInstance(self.extractor.notion_page_reader,
                              NotionPageReader)

    def test_extract_from_valid_pages(self):
        """
        Test extracting from valid pages.
        """
        test_page_ids = ['6a3c20b6861145b29030292120aa03e6',
                         'e479ee3eef9a4eefb3a393848af9ed9d']
        documents = self.extractor.extract(page_ids=test_page_ids)
        self.assertGreater(len(documents), 0)

    def test_extract_from_valid_database(self):
        """
        Test extracting from a valid database.
        """
        test_database_ids = ['dadd27f1dc1e4fa6b5b9dea76858dabe']
        documents = self.extractor.extract(database_ids=test_database_ids)
        self.assertGreater(len(documents), 0)

    def test_handle_invalid_page_id(self):
        """
        Test handling of invalid page IDs.
        Expecting empty results.
        """
        invalid_page_ids = ['non_existent_page']
        documents = self.extractor.extract(page_ids=invalid_page_ids)
        self.assertEqual(len(documents), 0)

    def test_handle_invalid_database_id(self):
        """
        Test handling of invalid database IDs.
        Expecting empty results.
        """
        invalid_database_ids = ['non_existent_database']
        documents = self.extractor.extract(database_ids=invalid_database_ids)
        self.assertEqual(len(documents), 0)
