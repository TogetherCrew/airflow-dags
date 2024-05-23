import os
from typing import List, Optional

from dotenv import load_dotenv
from llama_index.core import Document
from llama_index.readers.notion import NotionPageReader


class NotionExtractor:
    def __init__(self, notion_token: Optional[str] = None):
        """
        Initializes the NotionExtractor with an integration
        token to authenticate API requests.
        If no token is provided,
        it tries to load it from an environment variable.

        Args:
        notion_token (Optional[str]): The Notion API integration token.
        If None, the token
        is loaded from the 'NOTION_TEST_INTEGRATION_TOKEN' environment variable.
        """
        if notion_token is None:
            load_dotenv()
            self.integration_token = os.getenv("NOTION_INTEGRATION_TOKEN")
        else:
            self.integration_token = notion_token

        self.notion_page_reader = NotionPageReader(self.integration_token)

    def extract(
        self,
        page_ids: Optional[List[str]] = None,
        database_ids: Optional[List[str]] = None,
    ) -> List[Document]:
        """
        Extracts documents from Notion pages and databases,
        specified by their IDs.

        Args:
        page_ids (Optional[List[str]]): List of page IDs to extract documents from.
        database_ids (Optional[List[str]]): List of database IDs to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified Notion pages and databases.
        """
        documents = []
        if page_ids:
            documents.extend(self.extract_from_pages(page_ids))
        if database_ids:
            for db_id in database_ids:
                documents.extend(self.extract_from_database(db_id))
        return documents

    def extract_from_database(self, database_id: str) -> List[Document]:
        """
        Extracts documents from a specific Notion database by its ID.

        Args:
        database_id (str): The ID of the database to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified database.
        """
        response = self.notion_page_reader.load_data(database_id=database_id)
        return response

    def extract_from_pages(self, page_ids: List[str]) -> List[Document]:
        """
        Extracts documents from specific Notion pages by their IDs.

        Args:
        page_ids (List[str]): The list of page IDs to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified pages.
        """
        response = self.notion_page_reader.load_data(page_ids=page_ids)
        return response
