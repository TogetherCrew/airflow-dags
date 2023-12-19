from llama_index import Document
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
            doc = Document(
                text=post["raw"],
                metadata={
                    "author_name": post["author_name"],
                    "author_username": post["author_username"],
                    "forum_endpoint": post["forum_endpoint"],
                    "createdAt": post["createdAt"],
                    "updatedAt": post["updatedAt"],
                    "postId": post["postId"],
                    "topic": post["topic"],
                    "category": post["category"],
                    "authorTrustLevel": post["authorTrustLevel"],
                    "liker_usernames": post["liker_usernames"],
                    "liker_names": post["liker_names"],
                    "replier_usernames": post["replier_usernames"],
                    "replier_names": post["replier_names"],
                },
            )
        else:
            doc = Document(text=post["raw"])

        documents.append(doc)

    return documents
