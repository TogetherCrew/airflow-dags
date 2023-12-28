import argparse
import logging

from hivemind_etl_helpers.src.db.discourse.raw_post_to_documents import (
    fetch_discourse_documents,
)
from hivemind_etl_helpers.src.db.discourse.utils.get_forums import get_forums
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from tc_hivemind_backend.pg_vector_access import PGVectorAccess
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding


def process_discourse_vectorstore(community_id: str) -> None:
    """
    process discourse messages and save them in postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    """
    dbname = f"community_{community_id}"
    prefix = f"COMMUNITYID: {community_id} "
    logging.info(prefix)

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
    process the discourse forum data
    since the postIds across communities are duplicate
    we need to process data per forum

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
    table_name = "discourse"

    latest_date_query = f"""
        SELECT (metadata_->> 'updatedAt')::timestamp
        AS latest_date
        FROM data_discourse
        WHERE (metadata_ ->> 'forum_endpoint') = '{forum_endpoint}'
        ORDER BY (metadata_->>'updatedAt')::timestamp DESC
        LIMIT 1;
    """
    from_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )

    logging.info(
        f"{log_prefix} Fetching raw data and converting to llama_index.Documents"
    )
    documents = fetch_discourse_documents(forum_id=forum_id, from_date=from_date)

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

    embed_model = CohereEmbedding()

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        doc_file_ids_to_delete=deletion_query,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "community_id", help="the community to save the discourse data for it"
    )

    args = parser.parse_args()

    process_discourse_vectorstore(args.community_id)
