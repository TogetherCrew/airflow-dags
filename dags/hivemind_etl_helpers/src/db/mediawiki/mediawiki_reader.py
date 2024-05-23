"""Simple reader that reads mediawiki."""

from typing import Any, List, Optional

import wikipedia
from llama_index.core import Document
from llama_index.legacy.readers.base import BasePydanticReader


class MediaWikiReader(BasePydanticReader):
    """WikiMedia reader.

    Reads a page.

    """

    is_remote: bool = True

    def __init__(self, api_url: Optional[str] = None) -> None:
        """Initialize with parameters."""
        if api_url:
            wikipedia.set_api_url(api_url)

    @classmethod
    def class_name(cls) -> str:
        return "WikipediaReader"

    def load_data(self, pages: List[str], **load_kwargs: Any) -> List[Document]:
        """Load data from the input directory.

        Args:
            pages (List[str]): List of pages to read.

        """
        import wikipedia

        results = []
        for page in pages:
            wiki_page = wikipedia.page(page, **load_kwargs)
            page_content = wiki_page.content
            page_id = wiki_page.pageid
            doc = Document(
                id_=page_id,
                text=page_content,
                metadata={
                    "url": wiki_page.url,
                },
                excluded_embed_metadata_keys=["url"],
                excluded_llm_metadata_keys=["url"],
            )
            results.append(doc)
        return results
