import logging
from datetime import datetime

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.github.extract import (
    GithubExtraction,
    fetch_comments,
    fetch_issues,
    fetch_pull_requests,
)
from hivemind_etl_helpers.src.db.github.github_organization_repos import (
    get_github_organization_repos,
)
from hivemind_etl_helpers.src.db.github.load import (
    PrepareDeletion,
    load_documents_into_pg_database,
)
from hivemind_etl_helpers.src.db.github.transform import GitHubTransformation
from llama_index.core import Document
from tc_hivemind_backend.db.pg_db_utils import setup_db


def process_github_vectorstore(
    community_id: str, github_org_id: str, from_starting_date: datetime | None = None
) -> None:
    """
    ETL process for github raw data

    Parameters
    ------------
    community_id : str
        the community to save github's data
    """
    load_dotenv()
    dbname = f"community_{community_id}"
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(prefix)

    table_name = "github"

    logging.info(f"{prefix}Setting up database")
    latest_date_query = f"""
            SELECT (metadata_->> 'created_at')::timestamp
            AS latest_date
            FROM data_{table_name}
            ORDER BY (metadata_->>'created_at')::timestamp DESC
            LIMIT 1;
    """
    from_date_saved_data = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )
    from_date: datetime | None
    if from_date_saved_data:
        from_date = from_date_saved_data
    else:
        from_date = from_starting_date

    logging.info(f"Fetching data from date: {from_date}")

    repository_ids = get_github_organization_repos(github_organization_id=github_org_id)
    logging.info(f"{len(repository_ids)} repositories to fetch data from!")

    # EXTRACT
    github_extractor = GithubExtraction()

    github_comments = fetch_comments(repository_id=repository_ids, from_date=from_date)
    github_commits = github_extractor.fetch_commits(
        repository_id=repository_ids, from_date=from_date
    )
    github_issues = fetch_issues(repository_id=repository_ids, from_date=from_date)
    github_prs = fetch_pull_requests(
        repository_id=repository_ids,
        from_date_created=from_starting_date,
        from_date_updated=from_date_saved_data,
    )

    # TRANSFORM
    # llama-index documents
    github_transformation = GitHubTransformation()
    logging.debug(f"{prefix}Transforming comments!")
    docs_comment = github_transformation.transform_comments(github_comments)
    logging.debug(f"{prefix}Transforming commits!")
    docs_commit = github_transformation.transform_commits(github_commits)
    logging.debug(f"{prefix}Transforming issues!")
    docs_issue, docs_issue_comments = github_transformation.transform_issues(
        github_issues
    )
    logging.debug(f"{prefix}Transforming pull requests!")
    docs_prs = github_transformation.transform_pull_requests(github_prs)

    # there's no update on commits
    all_documents: list[Document] = docs_commit.copy()

    # checking for updates on prs, issues, and comments
    delete_docs = PrepareDeletion(community_id)
    docs_to_save, deletion_query = delete_docs.prepare(
        pr_documents=docs_prs,
        issue_documents=docs_issue,
        comment_documents=docs_comment + docs_issue_comments,
    )
    all_documents.extend(docs_to_save)

    logging.debug(f"{len(all_documents)} prepared to be saved!")
    if len(all_documents) == 0:
        logging.info("No new documents to save!")

    logging.info(f"deletion_query: {deletion_query}")

    # LOAD
    logging.info(f"{prefix}Loading data into postgres db")
    load_documents_into_pg_database(
        documents=all_documents,
        community_id=community_id,
        table_name=table_name,
        deletion_query=deletion_query,
    )
