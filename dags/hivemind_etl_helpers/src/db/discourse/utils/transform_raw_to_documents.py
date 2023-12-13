from llama_index import Document
from neo4j import Record


def transform_raw_to_documents(raw_data: list[Record]) -> list[Document]:
    """
    transform the raw messages to llama_index documents

    Parameters
    -----------
    raw_data : list[neo4j.Record]
        a list of retrieved data from neo4j

    Returns
    --------
    documents : list[llama_index.Document]
        a list of llama_index documents to be used for a LLM
    """
    documents: list[Document] = []

    for record in raw_data:
        post = record.data()

        documents.append(
            Document(
                text=post["raw"],
                metadata={
                    "author_name": post["author_name"],
                    "author_username": post["author_username"],
                    "forum_endpoint": post["forum_endpoint"],
                    "createdAt": post["createdAt"],
                    "updatedAt": post["updatedAt"],
                    "postId": post["postId"],
                    "topic": post["topic"],
                    "categories": post["categories"],
                    "authorTrustLevel": post["authorTrustLevel"],
                    "liker_usernames": post["liker_usernames"],
                    "liker_names": post["liker_names"],
                    "replier_usernames": post["replier_usernames"],
                    "replier_names": post["replier_names"],
                },
            )
        )

    return documents
