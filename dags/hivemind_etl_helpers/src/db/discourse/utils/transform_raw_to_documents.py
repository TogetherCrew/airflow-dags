from dateutil.parser import parse

from llama_index.core import Document
from neo4j import Record


def transform_raw_to_documents(
    raw_data: list[Record] | list[dict[str, str]], exclude_metadata: bool = False
) -> list[Document]:
    """
    transform the raw messages to llama_index documents

    Parameters
    -----------
    raw_data : list[neo4j.Record] | list[dict[str, str]]
        a list of retrieved data from neo4j
        can be list of dictionaries
    exclude_metadata : bool
        `False` do not exclude any metadata
        `True` exclude all metadata

    Returns
    --------
    documents : list[llama_index.Document]
        a list of llama_index documents to be used for a LLM
    """
    documents: list[Document] = []

    for record in raw_data:
        if isinstance(record, Record):
            post = record.data()
        else:
            post = record

        doc: Document

        if not exclude_metadata:
            forum_endpoint = post["forum_endpoint"]
            topic_id = post["topic_id"]
            post_number = post["post_number"]

            link = f"https://{forum_endpoint}/t/{topic_id}/{post_number}"

            doc = Document(
                text=post["raw"],
                metadata={
                    "author_name": post["author_name"],
                    "author_username": post["author_username"],
                    "forum_endpoint": forum_endpoint,
                    "date": parse(post["createdAt"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "updatedAt": parse(post["updatedAt"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "postId": post["postId"],
                    "topic": post["topic"],
                    "category": post["category"],
                    "authorTrustLevel": post["authorTrustLevel"],
                    "liker_usernames": post["liker_usernames"],
                    "liker_names": post["liker_names"],
                    "replier_usernames": post["replier_usernames"],
                    "replier_names": post["replier_names"],
                    "link": link,
                },
            )
        else:
            doc = Document(text=post["raw"])

        documents.append(doc)

    return documents
