import logging
from datetime import timedelta

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import (
    fetch_raw_posts_grouped,
)
from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from hivemind_etl_helpers.src.db.discourse.utils.get_forums import get_forums
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from llama_index import Document
from llama_index.response_synthesizers import get_response_synthesizer
from neo4j._data import Record
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


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

    forums = get_forums(community_id=community_id)

    # The below commented lines are for debugging
    # forums = [
    #     {
    #         "uuid": "851d8069-fc3a-415a-b684-1261d4404092",
    #         "endpoint": "gov.optimism.io",
    #     }
    # ]
    for forum in forums:
        forum_id = forum["uuid"]
        forum_endpoint = forum["endpoint"]
        process_forum(
            forum_id=forum_id,
            community_id=community_id,
            dbname=dbname,
            log_prefix=f"{prefix}ForumId: {forum_id}",
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
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "discourse_summary"

    latest_date_query = f"""
        SELECT (metadata_->> 'date')::timestamp
        AS latest_date
        FROM data_{table_name}
        WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
        ORDER BY (metadata_->>'date')::timestamp DESC
        LIMIT 1;
    """
    from_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )
    # increasing 1 day since we've saved the summaries of the last day
    # e.g.: we would have the summaries of date 2023.12.15
    # and we should start from the 16th Dec.
    if from_date is not None:
        from_date += timedelta(days=1)

    logging.info(
        f"{log_prefix} Fetching raw data and converting to llama_index.Documents"
    )

    raw_data_grouped = fetch_raw_posts_grouped(forum_id=forum_id, from_date=from_date)

    if raw_data_grouped != []:
        (
            topic_summary_documents,
            category_summary_documenets,
            daily_summary_documents,
        ) = get_summary_documents(
            forum_id=forum_id,
            raw_data_grouped=raw_data_grouped,
            forum_endpoint=forum_endpoint,
        )

        logging.info("Getting the summaries embedding and saving within database!")

        node_parser = configure_node_parser(chunk_size=chunk_size)
        pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

        embed_model = CohereEmbedding()

        logging.info(
            f"{log_prefix} Saving the topic summaries (and extracting the embedding to save)"
        )
        # saving topic summaries
        pg_vector.save_documents_in_batches(
            community_id=community_id,
            documents=topic_summary_documents,
            batch_size=100,
            node_parser=node_parser,
            max_request_per_minute=None,
            embed_model=embed_model,
            embed_dim=embedding_dim,
            request_per_minute=10000,
        )

        logging.info(
            f"{log_prefix} Saving the category summaries (and extracting the embedding to save)"
        )
        # saving category summaries
        pg_vector.save_documents_in_batches(
            community_id=community_id,
            documents=daily_summary_documents,
            batch_size=100,
            node_parser=node_parser,
            max_request_per_minute=None,
            embed_model=embed_model,
            embed_dim=embedding_dim,
            request_per_minute=10000,
        )

        logging.info(
            f"{log_prefix} Saving the daily summaries (and extracting the embedding to save)"
        )
        # saving daily summaries
        pg_vector.save_documents_in_batches(
            community_id=community_id,
            documents=category_summary_documenets,
            batch_size=100,
            node_parser=node_parser,
            max_request_per_minute=None,
            embed_model=embed_model,
            embed_dim=embedding_dim,
            request_per_minute=10000,
        )
    else:
        logging.info(f"No data to process. from_date: {from_date}")


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
