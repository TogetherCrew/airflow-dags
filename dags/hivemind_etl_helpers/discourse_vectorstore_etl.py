import logging
from datetime import datetime

from hivemind_etl_helpers.src.db.discourse.raw_post_to_documents import (
    fetch_discourse_documents,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from llama_index.core import Settings
from llama_index.llms.openai import OpenAI
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.pg_vector_access import PGVectorAccess


def process_discourse_vectorstore(
    community_id: str, forum_endpoint: str, from_starting_date: datetime
) -> None:
    """
    process discourse messages and save them in postgresql

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
    logging.info(prefix)

    process_forum(
        forum_endpoint=forum_endpoint,
        community_id=community_id,
        dbname=dbname,
        log_prefix=f"{prefix}ForumId: {forum_endpoint}",
        from_starting_date=from_starting_date,
    )


def process_forum(
    forum_endpoint: str,
    community_id: str,
    dbname: str,
    log_prefix: str,
    from_starting_date: datetime,
):
    """
    process the discourse forum data
    since the postIds across communities are duplicate
    we need to process data per forum

    Parameters
    ------------
    forum_endpoint : str
        the DiscourseForum endpoint for document checking
    community_id : str
        the community that the forum relates to
    dbname : str
        the data of the community saved within the postgres database `dbname`
    log_predix : str
        the logging prefix to print out
    from_starting_date : datetime
        the time to start processing documents
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    table_name = "discourse"

    latest_date_query = f"""
        SELECT (metadata_->> 'date')::timestamp
        AS latest_date
        FROM data_discourse
        WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
        ORDER BY (metadata_->>'date')::timestamp DESC
        LIMIT 1;
    """
    from_last_saved_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )

    logging.info(
        f"{log_prefix} Fetching raw data and converting to llama_index.Documents"
    )

    if from_last_saved_date is None:
        from_date = from_starting_date
    else:
        from_date = from_last_saved_date

    documents = fetch_discourse_documents(
        forum_endpoint=forum_endpoint, from_date=from_date
    )

    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    documents, doc_file_ids_to_delete = check_documents(
        documents,
        community_id=community_id,
        identifier="postId",
        identifier_type="::float",
        date_field="updatedAt",
        table_name=table_name,
        metadata_condition={"forum_endpoint": forum_endpoint},
    )

    deletion_query: str = ""
    if len(doc_file_ids_to_delete) != 0:
        deletion_ids = tuple([float(item) for item in doc_file_ids_to_delete])
        deletion_query = f"""
            DELETE FROM data_discourse
            WHERE (metadata_->>'postId')::float IN {deletion_ids};
        """

    Settings.node_parser = node_parser
    Settings.embed_model = CohereEmbedding()
    Settings.chunk_size = chunk_size
    Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        max_request_per_minute=None,
        embed_dim=embedding_dim,
        doc_file_ids_to_delete=deletion_query,
    )
