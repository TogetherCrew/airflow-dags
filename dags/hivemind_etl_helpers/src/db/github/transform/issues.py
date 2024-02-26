from llama_index import Document
from hivemind_etl_helpers.src.db.github.utils.schema import GitHubIssue


def transform_issues(data: list[GitHubIssue]) -> list[Document]:
    """
    transform the github issues data to a list of llama_index documents

    Parameters
    -----------
    data : list[GitHubIssue]
        a list of github issues raw data

    Returns
    ---------
    transformed_issues : list[llama_index.Document]
        a list of llama index documents to be saved
    """
    transformed_issues: list[Document] = []

    for sample in data:
        metadata = sample.to_dict()
        del metadata["title"]

        exclude_embed_metadata = list(metadata.keys())

        # NOTE: the text can be very long causing the
        # metadata longer than chunk size (so we're skipping it)

        # including the first issue comment for embedding
        # as it can be an explanation to the issue
        # exclude_embed_metadata.remove("text")

        # title of the issue is the text for document
        document = Document(
            text=sample.title,
            metadata=metadata,
            excluded_embed_metadata_keys=exclude_embed_metadata,
            excluded_llm_metadata_keys=["url", "repository_id", "id", "text"],
        )
        transformed_issues.append(document)

    return transformed_issues
