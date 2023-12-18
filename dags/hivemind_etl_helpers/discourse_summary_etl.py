import argparse
import logging

from llama_index import Document
from llama_index.response_synthesizers import get_response_synthesizer
from neo4j._data import Record

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import (
    fetch_raw_posts_grouped,
)
from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from hivemind_etl_helpers.src.db.discourse.utils.get_forums import get_forums
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from hivemind_etl_helpers.src.utils.cohere_embedding import CohereEmbedding
from hivemind_etl_helpers.src.utils.pg_db_utils import setup_db
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess


def process_discourse_summary(community_id: str) -> None:
    """
    process discourse messages and save the per-channel/per-topic/daily summaries into postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    """
    dbname = f"community_{community_id}"
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(prefix + "Processing summaries")

    # forums = get_forums(community_id=community_id)

    # The below commented lines are for debugging
    forums = [
        {
            "uuid": "851d8069-fc3a-415a-b684-1261d4404092",
            "endpoint": "gov.optimism.io",
        }
    ]
    for forum in forums:
        forum_id = forum["uuid"]
        forum_endpoint = forum["endpoint"]
        process_forum(
            forum_id=forum_id,
            community_id=community_id,
            dbname=dbname,
            log_prefix=f"{prefix}ForumId: {forum}",
            forum_endpoint=forum_endpoint,
        )


def process_forum(
    forum_id: str,
    community_id: str,
    dbname: str,
    log_prefix: str,
    forum_endpoint: str,
):
    """
    process forum data


    Parameters
    ------------
    forum_id : str
        the forum that the community has
    community_id : str
        the community that the forum relates to
    dbname : str
        the data of the community saved within the postgres database `dbname`
    log_predix : str
        the logging prefix to print out
    forum_endpoint : str
        the DiscourseForum endpoint for document checking
    """
    table_name = "discourse_summary"

    latest_date_query = f"""
        SELECT (metadata_->> 'date')::timestamp
        AS latest_date
        FROM data_{table_name}
        WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
        ORDER BY (metadata_->>'date')::timestamp DESC
        LIMIT 1;
    """
    print("latest_date_query", latest_date_query)
    from_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )

    logging.info(
        f"{log_prefix} Fetching raw data and converting to llama_index.Documents"
    )

    raw_data_grouped = fetch_raw_posts_grouped(forum_id=forum_id, from_date=from_date)

    (
        topic_summary_documents,
        category_summary_documenets,
        daily_summary_documents,
    ) = get_summary_documents(
        forum_id=forum_id,
        raw_data_grouped=raw_data_grouped,
    )

    logging.info("Getting the summaries embedding and saving within database!")

    node_parser = configure_node_parser(chunk_size=256)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    embed_model = CohereEmbedding()
    embed_dim = 1024

    # saving thread summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=topic_summary_documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embed_dim,
        request_per_minute=10000,
    )

    # saving channel summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=daily_summary_documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embed_dim,
        request_per_minute=10000,
    )

    # saving daily summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=category_summary_documenets,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embed_dim,
        request_per_minute=10000,
    )


def get_summary_documents(
    forum_id: str, raw_data_grouped: list[Record], forum_endpoint: str
) -> tuple[list[Document], list[Document], list[Document],]:
    """
    prepare the summary documents for discourse based on given raw data

    Parameters
    ------------
    forum_id : str
        the forum uuid just for logging
    raw_data_grouped : list[Record]
        a list of neo4j records
    forum_endpoint : str
        the endpoint of the forum id

    Returns
    --------
    topic_summary_documents : list[llama_index.Document]
        list of topic summaries converted to llama_index documents
    category_summary_documenets : list[llama_index.Document]
        list of category summaries converted to llama_index documents
    daily_summary_documents : list[llama_index.Document]
        list of daily summaries converted to llama_index documents
    """

    discourse_summary = DiscourseSummary(
        forum_id=forum_id,
        response_synthesizer=get_response_synthesizer(response_mode="tree_summarize"),
        forum_endpoint=forum_endpoint,
    )

    summarization_query = (
        "Please make a concise summary based only on the provided text "
    )
    topic_summaries = discourse_summary.prepare_topic_summaries(
        raw_data_grouped=raw_data_grouped,
        summarization_query=summarization_query + "from this discourse topic",
    )

    (
        category_summaries,
        topic_summary_documents,
    ) = discourse_summary.prepare_category_summaries(
        topic_summaries=topic_summaries,
        summarization_query=summarization_query
        + " from the selection of discourse topic summaries",
    )

    (
        daily_summaries,
        category_summary_documenets,
    ) = discourse_summary.prepare_daily_summaries(
        category_summaries=category_summaries,
        summarization_query=summarization_query
        + " from the selection of discourse category summaries",
    )
    daily_summary_documents = discourse_summary.prepare_daily_summary_documents(
        daily_summaries=daily_summaries
    )

    return (
        topic_summary_documents,
        category_summary_documenets,
        daily_summary_documents,
    )
