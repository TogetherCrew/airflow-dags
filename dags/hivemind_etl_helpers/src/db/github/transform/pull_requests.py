from llama_index import Document
from dags.hivemind_etl_helpers.src.db.github.utils.schema import GitHubPullRequest


def transform_prs(data: list[GitHubPullRequest]) -> list[Document]:
    """
    transform the github pull request data to a list of llama_index documents

    Parameters
    -----------
    data : list[GitHubPullRequest]
        a list of github pull request raw data

    Returns
    ---------
    transformed_prs : list[llama_index.Document]
        a list of llama index documents to be saved
    """
    transformed_prs: list[Document] = []

    for sample in data:
        metadata = sample.to_dict()
        del metadata["title"]

        exclude_embed_metadata = list(metadata.keys())
        # `state` property is representative of PR being completed or open
        exclude_embed_metadata.remove("state")

        document = Document(
            text=sample.title,
            metadata=metadata,
            excluded_embed_metadata_keys=exclude_embed_metadata,
            excluded_llm_metadata_keys=[
                "id",
                "url",
                "repository_id",
                "issues_url",
                "id",
            ],
        )
        transformed_prs.append(document)

    return transformed_prs
