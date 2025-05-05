import logging
from datetime import datetime

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.github.extract import (
    GithubExtraction,
    fetch_issues,
    fetch_pull_requests,
)
from hivemind_etl_helpers.src.db.github.github_organization_repos import (
    get_github_organization_repos,
)
from hivemind_etl_helpers.src.db.github.transform import GitHubTransformation
from llama_index.core import Document
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline


def process_github_vectorstore(
    community_id: str,
    github_org_ids: list[str],
    repo_ids: list[str],
    platform_id: str,
    from_starting_date: datetime | None = None,
) -> None:
    """
    ETL process for github raw data

    Parameters
    ------------
    community_id : str
        the community to save github's data
    github_org_ids : list[str]
        a list of github organization ids to process their data
    repo_ids : list[str]
        a list of github repositories to process their data
    platform_id : str
        the platform id to save the data under qdrant collection
    from_starting_date : datetime | None
        the date to start processing data from
    """
    load_dotenv()
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(f"{prefix}Processing data!")

    org_repository_ids = get_github_organization_repos(
        github_organization_ids=github_org_ids
    )
    repository_ids = list(set(repo_ids + org_repository_ids))
    logging.info(f"{len(repository_ids)} repositories to fetch data from!")

    # EXTRACT
    github_extractor = GithubExtraction()
    github_comments = github_extractor.fetch_comments(repository_id=repository_ids)
    github_commits = github_extractor.fetch_commits(repository_id=repository_ids)
    github_issues = fetch_issues(repository_id=repository_ids)
    github_prs = fetch_pull_requests(repository_id=repository_ids)

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

    all_documents: list[Document] = (
        docs_commit + docs_comment + docs_issue_comments + docs_prs + docs_issue
    )

    logging.debug(f"{len(all_documents)} prepared to be saved!")

    # LOAD
    logging.info(f"{prefix}Loading data into postgres db")
    ingestion_pipeline = CustomIngestionPipeline(
        community_id=community_id, collection_name=platform_id
    )
    ingestion_pipeline.run_pipeline(docs=all_documents)
