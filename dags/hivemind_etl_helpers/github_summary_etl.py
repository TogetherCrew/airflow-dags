import logging

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.github.aggregator import (
    CommentAggregator,
    CommitAggregator,
    IssueAggregator,
    PullRequestAggregator,
)
from hivemind_etl_helpers.src.db.github.extract import (
    GithubExtraction,
    fetch_issues,
    fetch_pull_requests,
)
from hivemind_etl_helpers.src.db.github.github_organization_repos import (
    get_github_organization_repos,
)
from hivemind_etl_helpers.src.db.github.summary import GitHubSummary
from hivemind_etl_helpers.src.db.github.summary.type import SummaryType
from hivemind_etl_helpers.src.db.github.transform import GitHubTransformation
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from llama_index.core import Document, Settings
from llama_index.core.response_synthesizers import get_response_synthesizer
from llama_index.llms.openai import OpenAI
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline


def process_github_summary_vectorstore(
    community_id: str,
    github_org_ids: list[str],
    repo_ids: list[str],
    platform_id: str,
) -> None:
    """
    ETL process for github summary data

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
    """
    load_dotenv()
    prefix = f"COMMUNITYID: {community_id} "
    ingestion_pipeline = CustomIngestionPipeline(
        community_id=community_id, collection_name=f"{platform_id}_summary"
    )
    date = ingestion_pipeline.get_latest_document_date(field_name="date")
    logging.info(f"Latest Document date available: {date}")

    Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

    logging.info(f"{prefix}Processing data!")

    chunk_size, _ = load_model_hyperparams()
    node_parser = configure_node_parser(chunk_size=chunk_size)

    Settings.node_parser = node_parser
    Settings.embed_model = CohereEmbedding()
    Settings.chunk_size = chunk_size

    org_repository_ids = get_github_organization_repos(
        github_organization_ids=github_org_ids
    )
    repository_ids = list(set(repo_ids + org_repository_ids))
    logging.info(f"{len(repository_ids)} repositories to fetch data from!")

    # EXTRACT
    github_extractor = GithubExtraction()
    comments = github_extractor.fetch_comments(
        repository_id=repository_ids, from_date=date
    )
    commits = github_extractor.fetch_commits(
        repository_id=repository_ids, from_date=date
    )
    issues = fetch_issues(repository_id=repository_ids, from_date=date)
    prs = fetch_pull_requests(repository_id=repository_ids, from_date_created=date)

    comment_aggregator = CommentAggregator()
    commit_aggregator = CommitAggregator()
    pull_request_aggregator = PullRequestAggregator()
    issue_aggregator = IssueAggregator()

    comment_aggregator.add_multiple_comments(comments)
    commit_aggregator.add_multiple_commits(commits=commits)
    pull_request_aggregator.add_multiple_prs(prs=prs)
    issue_aggregator.add_multiple_issues(issues=issues)

    aggregated_comments = comment_aggregator.get_daily_comments()
    aggregated_commits = commit_aggregator.get_daily_commits()
    aggregated_prs = pull_request_aggregator.get_daily_prs()
    aggregated_issues = issue_aggregator.get_daily_issues()

    github_transformation = GitHubTransformation()

    # TRANSFORM
    logging.debug(f"{prefix}Transforming commits!")
    aggregated_commit_docs: dict[str, list[Document]] = {
        date: github_transformation.transform_commits(aggregated_commits[date])
        for date in aggregated_commits.keys()
    }

    logging.debug(f"{prefix}Transforming comments!")
    aggregated_comment_docs: dict[str, list[Document]] = {
        date: github_transformation.transform_comments(aggregated_comments[date])
        for date in aggregated_comments.keys()
    }

    logging.debug(f"{prefix}Transforming issues!")
    aggregated_issue_docs: dict[str, list[Document]] = {
        date: github_transformation.transform_issues(aggregated_issues[date])
        for date in aggregated_issues.keys()
    }

    logging.debug(f"{prefix}Transforming pull requests!")
    aggregated_pr_docs: dict[str, list[Document]] = {
        date: github_transformation.transform_pull_requests(aggregated_prs[date])
        for date in aggregated_prs.keys()
    }

    summarizer = GitHubSummary(
        response_synthesizer=get_response_synthesizer(response_mode="tree_summarize"),
    )

    commits_summarized: list[Document] = [
        summarizer.transform_summary(
            date=date,
            summary=summarizer.process_commits(
                date=date, documents=aggregated_commit_docs[date]
            ),
            type=SummaryType.COMMIT,
        )
        for date in aggregated_commit_docs.keys()
    ]

    comments_summarized: list[Document] = [
        summarizer.transform_summary(
            date=date,
            summary=summarizer.process_comments(
                date=date, documents=aggregated_comment_docs[date]
            ),
            type=SummaryType.COMMENT,
        )
        for date in aggregated_comment_docs.keys()
    ]

    prs_summarized: list[Document] = [
        summarizer.transform_summary(
            date=date,
            summary=summarizer.process_prs(
                date=date, documents=aggregated_pr_docs[date]
            ),
            type=SummaryType.PR,
        )
        for date in aggregated_pr_docs.keys()
    ]

    issues_summarized: list[Document] = [
        summarizer.transform_summary(
            date=date,
            summary=summarizer.process_issues(
                date=date, documents=aggregated_issue_docs[date]
            ),
            type=SummaryType.ISSUE,
        )
        for date in aggregated_issue_docs.keys()
    ]

    all_documents: list[Document] = (
        issues_summarized + comments_summarized + prs_summarized + commits_summarized
    )

    logging.debug(f"{len(all_documents)} prepared to be saved!")

    # LOAD
    logging.info(f"{prefix}Loading data into qdrant db")

    ingestion_pipeline.run_pipeline(docs=all_documents)
