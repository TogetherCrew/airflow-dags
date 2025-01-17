from hivemind_etl_helpers.src.db.github.schema import GitHubCommit
from llama_index.core import Document


def transform_commits(data: list[GitHubCommit]) -> list[Document]:
    """
    transform the github commits data to a list of llama_index documents

    Parameters
    -----------
    data : list[GitHubCommit]
        a list of github commits raw data

    Returns
    ---------
    transformed_commits : list[llama_index.Document]
        a list of llama index documents to be saved
    """
    transformed_commits: list[Document] = []

    for sample in data:
        metadata = sample.to_dict()
        del metadata["message"]
        document = Document(
            id_=sample.sha,
            text=sample.message,
            metadata=metadata,
            # all metadata to be excluded from embedding model
            excluded_embed_metadata_keys=list(metadata.keys()),
            excluded_llm_metadata_keys=["sha", "api_url", "url", "verification"],
        )
        transformed_commits.append(document)

    return transformed_commits
