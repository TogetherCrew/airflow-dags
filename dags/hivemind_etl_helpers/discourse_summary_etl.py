import logging
from datetime import datetime, timedelta

from hivemind_etl_helpers.src.db.discourse.fetch_raw_posts import (
    fetch_raw_posts_grouped,
)
from hivemind_etl_helpers.src.db.discourse.summary.prepare_summary import (
    DiscourseSummary,
)
from hivemind_etl_helpers.src.db.discourse.utils.get_forums import (
    get_forum_uuid,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.sort_summary_docs import (
    sort_summaries_daily,
)
from llama_index.core import Document, Settings
from llama_index.core.response_synthesizers import get_response_synthesizer
from llama_index.llms.openai import OpenAI
from neo4j._data import Record
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import (
    load_model_hyperparams,
)
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


def process_discourse_summary(
    community_id: str, forum_endpoint: str, from_starting_date: datetime
) -> None:
    """
    process discourse messages and save the per-channel/per-topic/daily
    summaries into postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    forum_endpoint : str
        the forum endpoint that is related to a community
    from_starting_date : datetime
        the starting date of the ETL
    """
    dbname = f"community_{community_id}"
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(prefix + "Processing summaries")

    forum_uuid = get_forum_uuid(forum_endpoint=forum_endpoint)

    # The below commented lines are for debugging
    # forum_uuid = [
    #     {
    #         "uuid": "851d8069-fc3a-415a-b684-1261d4404092",
    #     }
    # ]
    forum_id = forum_uuid[0]["uuid"]
    forum_endpoint = forum_endpoint
    process_forum(
        forum_id=forum_id,
        community_id=community_id,
        dbname=dbname,
        log_prefix=f"{prefix}ForumId: {forum_id}",
        forum_endpoint=forum_endpoint,
        from_starting_date=from_starting_date,
    )


def process_forum(
    forum_id: str,
    community_id: str,
    dbname: str,
    log_prefix: str,
    forum_endpoint: str,
    from_starting_date: datetime,
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
    from_starting_date : datetime
        the time to start processing documents
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "discourse_summary"

    # getting the query of latest date
    latest_date_query = f"""
        SELECT (metadata_->> 'date')::timestamp AS latest_date
        FROM data_{table_name}
        WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
        AND (metadata_ ->> 'channel' IS NULL AND metadata_ ->> 'thread' IS NULL)
        ORDER BY (metadata_->>'date')::timestamp DESC
        LIMIT 1;
    """
    from_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )

    if from_date and from_date >= from_starting_date:
        # deleting any in-complete saved summaries
        deletion_query = f"""
            DELETE FROM data_{table_name}
            WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
            AND (metadata_ ->> 'date')::timestamp > '{from_date.strftime("%Y-%m-%d")}';
        """
        # increasing 1 day since we've saved the summaries of the last day
        from_date += timedelta(days=1)
    else:
        deletion_query = ""

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

        node_parser = configure_node_parser(chunk_size=chunk_size)
        pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

        sorted_daily_docs = sort_summaries_daily(
            level1_docs=topic_summary_documents,
            level2_docs=category_summary_documenets,
            daily_docs=daily_summary_documents,
        )

        logging.info(
            f"{log_prefix} Saving discourse summaries (extracting the embedding and saving)"
        )

        Settings.node_parser = node_parser
        Settings.embed_model = CohereEmbedding()
        Settings.chunk_size = chunk_size
        Settings.llm = OpenAI(model="gpt-3.5-turbo")

        pg_vector.save_documents_in_batches(
            community_id=community_id,
            documents=sorted_daily_docs,
            batch_size=100,
            max_request_per_minute=None,
            embed_dim=embedding_dim,
            request_per_minute=10000,
            deletion_query=deletion_query,
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
