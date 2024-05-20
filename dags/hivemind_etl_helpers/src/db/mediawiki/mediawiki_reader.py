"""Simple reader that reads mediawiki."""

from typing import Any, List, Optional

from llama_index.legacy.readers.base import BasePydanticReader
from llama_index.legacy.schema import Document


class MediaWikiReader(BasePydanticReader):
    """WikiMedia reader.

    Reads a page.

    """

    is_remote: bool = True

    def __init__(self, api_url: Optional[str] = None) -> None:
        """Initialize with parameters."""
        try:
            import wikipedia  # noqa
        except ImportError:
            raise ImportError(
                "`wikipedia` package not found, please run `pip install wikipedia`"
            )

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
            results.append(Document(id_=page_id, text=page_content))
        return results
