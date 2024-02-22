from llama_index import Document
from dags.hivemind_etl_helpers.src.db.github.utils.schema import GitHubComment


def transform_comments(data: list[GitHubComment]) -> list[Document]:
    """
    transform the github comments data to a list of llama_index documents

    Parameters
    -----------
    data : list[GitHubComment]
        a list of github comments raw data

    Returns
    ---------
    transformed_comments : list[llama_index.Document]
        a list of llama index documents to be saved
    """
    transformed_comments: list[Document] = []

    for sample in data:
        metadata = sample.to_dict()
        # text would not be in metadata
        del metadata["text"]
        document = Document(
            text=sample.text,
            metadata=metadata,
            # all metadata to be excluded from embedding model
            excluded_embed_metadata_keys=list(metadata.keys()),
            excluded_llm_metadata_keys=["id", "url", "reactions"],
        )
        transformed_comments.append(document)

    return transformed_comments
