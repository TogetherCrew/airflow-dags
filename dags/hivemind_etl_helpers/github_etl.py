import logging
from llama_index import Document

from hivemind_etl_helpers.src.db.github.extract import (
    fetch_comments,
    fetch_commits,
    fetch_issues,
    fetch_pull_requests,
)
from hivemind_etl_helpers.src.db.github.transform import (
    transform_comments,
    transform_commits,
    transform_issues,
    transform_prs,
)
from hivemind_etl_helpers.src.db.github.load import (
    load_documents_into_pg_database,
    PrepareDeletion,
)
from tc_hivemind_backend.db.pg_db_utils import setup_db


def process_github_vectorstore(community_id: str) -> None:
    """
    ETL process for github raw data

    Parameters
    ------------
    community_id : str
        the community to save github's data
    """
    dbname = f"community_{community_id}"
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(prefix)

    table_name = "github"

    repository_ids = [
        634791780,
        635638754,
    ]
    from_date = None

    # EXTRACT
    github_comments = fetch_comments(repository_id=repository_ids, from_date=from_date)
    github_commits = fetch_commits(repository_id=repository_ids, from_date=from_date)
    github_issues = fetch_issues(repository_id=repository_ids, from_date=from_date)
    github_prs = fetch_pull_requests(repository_id=repository_ids, from_date=from_date)

    # TRANSFORM
    # llama-index documents
    logging.debug(f"{prefix}Transforming comments!")
    docs_comment = transform_comments(github_comments)
    logging.debug(f"{prefix}Transforming commits!")
    docs_commit = transform_commits(github_commits)
    logging.debug(f"{prefix}Transforming issues!")
    docs_issue = transform_issues(github_issues)
    logging.debug(f"{prefix}Transforming pull requests!")
    docs_prs = transform_prs(github_prs)

    all_documents: list[Document] = docs_commit.copy()
    delete_docs = PrepareDeletion(community_id)
    docs_to_save, deletion_query = delete_docs.prepare(
        pr_documents=docs_prs,
        issue_documents=docs_issue,
        comment_documents=docs_comment,
    )
    all_documents.extend(docs_to_save)

    logging.debug(f"{len(all_documents)} prepared to be saved!")
    if len(all_documents) == 0:
        logging.info("No new documents to save!")

    logging.info(f"deletion_query: {deletion_query}")

    # LOAD
    logging.info(f"{prefix}Setting up database")
    setup_db(community_id=community_id, dbname=dbname)
    logging.info(f"{prefix}Loading data into postgres db")
    load_documents_into_pg_database(
        documents=all_documents,
        community_id=community_id,
        table_name=table_name,
        deletion_query=deletion_query,
    )
