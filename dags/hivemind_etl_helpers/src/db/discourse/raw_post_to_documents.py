from datetime import datetime

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import fetch_raw_posts
from hivemind_etl_helpers.src.db.discourse.utils.transform_raw_to_documents import (
    transform_raw_to_documents,
)
from llama_index.core import Document


def fetch_discourse_documents(
    forum_endpoint: str, from_date: datetime | None
) -> list[Document]:
    """
    get the raw messages of a community and then convert them to llama_index documents

    Parameters
    -----------
    forum_endpoint : str
        the discourse forum we want to process its data
    from_date : datetime | None
        the posts to retrieve from a specific date
        default is `None` meaning to fetch all posts
    """
    raw_posts = fetch_raw_posts(forum_endpoint, from_date)
    documents = transform_raw_to_documents(raw_data=raw_posts)

    return documents
