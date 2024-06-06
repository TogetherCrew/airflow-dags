import unittest
from dotenv import load_dotenv
import os
from typing import List
from llama_index.core import Document
from extractor import NotionExtractor


class TestNotionExtractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load environment variables from a .env file if available
        load_dotenv()
        cls.integration_token = os.getenv("NOTION_INTEGRATION_TOKEN")

        if not cls.integration_token:
            raise ValueError("No Notion integration token found. Please set the NOTION_INTEGRATION_TOKEN environment variable.")

        cls.extractor = NotionExtractor("secret_03j1uD58DLopsglzQV0UpUMl4qd371h3L50zVTiB3Dm")

    def test_extract_from_pages(self):
        # Replace with actual page IDs from your Notion workspace
        page_ids = ['e479ee3eef9a4eefb3a393848af9ed9d']

        documents = self.extractor.extract_from_pages(page_ids)

        # Print the extracted documents
        for doc in documents:
            print(f"Page Document: {doc.text}")

        # Check if the result is a list of Document objects
        self.assertIsInstance(documents, List)
        self.assertTrue(all(isinstance(doc, Document) for doc in documents))

    def test_extract_from_database(self):
        # Replace with an actual database ID from your Notion workspace
        database_id = 'c04ddb387dc14d6c855e23dcbbfe3ce7'

        documents = self.extractor.extract_from_database(database_id)

        # Print the extracted documents
        for doc in documents:
            print(f"Database Document: {doc.text}")

        # Check if the result is a list of Document objects
        self.assertIsInstance(documents, List)
        self.assertTrue(all(isinstance(doc, Document) for doc in documents))

    def test_extract(self):
        # Replace with actual page and database IDs from your Notion workspace
        page_ids = ['e479ee3eef9a4eefb3a393848af9ed9d']
        database_ids = ['f3a684f01769467080f37c4e2d741c18']

        documents = self.extractor.extract(page_ids=page_ids, database_ids=database_ids)

        # Print the extracted documents
        for doc in documents:
            print(f"Extracted Document: {doc.text}")

        # Check if the result is a list of Document objects
        self.assertIsInstance(documents, List)
        self.assertTrue(all(isinstance(doc, Document) for doc in documents))

if __name__ == '__main__':
    unittest.main()
