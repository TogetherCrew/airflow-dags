from datetime import datetime

from llama_index import Document

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import fetch_raw_posts
from hivemind_etl_helpers.src.db.discourse.utils.transform_raw_to_documents import (
    transform_raw_to_documents,
)


def fetch_discourse_documents(
    forum_id: str, from_date: datetime | None
) -> list[Document]:
    """
    get the raw messages of a community and then convert them to llama_index documents

    Parameters
    -----------
    forum_id : str
        the id of the forum we want to process its data
    from_date : datetime | None
        the posts to retrieve from a specific date
        default is `None` meaning to fetch all posts
    """
    raw_posts = fetch_raw_posts(forum_id, from_date)
    documents = transform_raw_to_documents(raw_data=raw_posts)

    return documents
