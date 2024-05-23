from typing import List, Optional

from hivemind_etl_helpers.src.db.mediawiki.mediawiki_reader import MediaWikiReader
from llama_index.core import Document


class MediaWikiExtractor:
    def __init__(self, api_url: Optional[str] = "https://en.wikipedia.org/w/api.php"):
        """
        Initializes the MediaWikiExtractor with an API URL for Wikimedia.
        If no URL is provided, it tries to load it from an environment variable.

        Args:
        api_url (Optional[str]): The Wikimedia API URL.
        If None, the URL is loaded from the 'WIKIMEDIA_API_URL' environment variable.
        """
        self.api_url = api_url
        self.wikimedia_reader = MediaWikiReader(api_url=self.api_url)

    def extract(self, page_ids: Optional[List[str]] = None) -> List[Document]:
        """
        Extracts documents from Wikimedia page_ids (their titles).

        Args:
        pages (Optional[List[str]]): List of page_ids to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified Wikimedia pages.
        """
        if page_ids:
            return self.extract_from_pages(page_ids)
        return []

    def extract_from_pages(self, pages: List[str]) -> List[Document]:
        """
        Extracts documents from specific Wikimedia pages by their titles.

        Args:
        pages (List[str]): The list of page titles to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified pages.
        """
        response = self.wikimedia_reader.load_data(pages=pages)
        return response