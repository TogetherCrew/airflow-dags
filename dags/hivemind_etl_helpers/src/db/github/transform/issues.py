from llama_index import Document
from hivemind_etl_helpers.src.db.github.schema import GitHubIssue


def transform_issues(data: list[GitHubIssue]) -> tuple[list[Document], list[Document]]:
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
    transformed_issue_comments : list[llama_index.Document]
        the first comment of issues
    """
    transformed_issues: list[Document] = []
    transformed_issue_comments: list[Document] = []

    for sample in data:
        metadata = sample.to_dict()
        del metadata["title"]

        exclude_embed_metadata = list(metadata.keys())

        # title of the issue is the text for document
        document = Document(
            text=sample.title,
            metadata=metadata,
            excluded_embed_metadata_keys=exclude_embed_metadata,
            excluded_llm_metadata_keys=["url", "repository_id", "id", "text"],
        )
        transformed_issues.append(document)
        # if the first comment if issue had some text
        if sample.text is not None:
            issue_first_comment = transform_comment_of_issue(sample)
            transformed_issue_comments.append(issue_first_comment)

    return transformed_issues, transformed_issue_comments


def transform_comment_of_issue(data: GitHubIssue) -> Document:
    """
    the first comment of a issue can be long, so it might not be fitted
    into metadata (avoiding metadata longer than chunk size error).
    So we're preparing the comment into another document

    Parameters
    ------------
    data : GitHubIssue
        the related github issue having the first comment within it

    Returns
    ---------
    document : llama_index.Document
        the comment document within the github issue
    """
    # since there's no way we could have the 
    # first comment's id, we're creating one manually
    # note: no ids before had 9 in front of them
    # so this id would be unique
    manual_comment_id = int("111" + f"{data.id}"[3:])
    metadata = {
        "author_name": data.author_name,
        "id": manual_comment_id,
        "repository_name": data.repository_name,
        "url": data.url,
        "created_at": data.created_at,
        "updated_at": data.updated_at,
        "related_node": "Issue",
        "related_title": data.title,
        "latest_saved_at": data.latest_saved_at,
        "reactions": {},
        "type": "comment",
    }
    document = Document(
        text=data.text,
        metadata=metadata,
        excluded_embed_metadata_keys=list(metadata.keys()),
        excluded_llm_metadata_keys=["id", "url", "reactions"],
    )
    return document
