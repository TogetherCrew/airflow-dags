import os
from typing import List, Optional

from dotenv import load_dotenv
from llama_index.core import Document

from hivemind_etl_helpers.src.db.mediawiki.mediawiki_reader import MediaWikiReader


class MediaWikiExtractor:
    def __init__(self, api_url: Optional[str] = "https://en.wikipedia.org/w/api.php"):
        """
        Initializes the MediaWikiExtractor with an API URL for Wikimedia.
        If no URL is provided, it tries to load it from an environment variable.

        Args:
        api_url (Optional[str]): The Wikimedia API URL.
        If None, the URL is loaded from the 'WIKIMEDIA_API_URL' environment variable.
        """
        if api_url is None:
            load_dotenv()
            self.api_url = os.getenv("WIKIMEDIA_API_URL")
        else:
            self.api_url = api_url

        self.wikimedia_reader = MediaWikiReader(api_url=self.api_url)

    def extract(self, pages: Optional[List[str]] = None) -> List[Document]:
        """
        Extracts documents from Wikimedia pages specified by their titles.

        Args:
        pages (Optional[List[str]]): List of page titles to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified Wikimedia pages.
        """
        if pages:
            return self.extract_from_pages(pages)
        return []

    def extract_from_pages(self, pages: List[str]) -> List[Document]:
        """
        Extracts documents from specific Wikimedia pages by their titles.

        Args:
        pages (List[str]): The list of page titles to extract documents from.

        Returns:
        List[Document]: A list of Document objects extracted from the specified pages.
        """
        try:
            response = self.wikimedia_reader.load_data(pages=pages)
            return response
        except Exception as e:
            print(f"Failed to extract from pages {pages}: {str(e)}")
            return []


# # Example usage
# if __name__ == "__main__":
#     # Example of initializing and using the extractor
#     extractor = MediaWikiExtractor(api_url="https://en.wikipedia.org/w/api.php")
#     documents = extractor.extract(pages=["Python (programming language)", "OpenAI"])
#     for doc in documents:
#         print(doc)
