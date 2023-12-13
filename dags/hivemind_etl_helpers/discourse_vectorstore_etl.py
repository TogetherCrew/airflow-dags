import argparse
import logging

from hivemind_etl_helpers.src.db.discourse.raw_post_to_documents import (
    fetch_discourse_documents,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.check_documents import check_documents
from hivemind_etl_helpers.src.utils.cohere_embedding import CohereEmbedding
from hivemind_etl_helpers.src.utils.neo4j import Neo4jConnection
from hivemind_etl_helpers.src.utils.pg_db_utils import delete_data, setup_db
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess


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

    neo4j = Neo4jConnection()

    # query = """
    #     MATCH (f:DiscourseForum) -[:IS_WITHIN]->(c:Community {id: $communityId})
    #     RETURN f.uuid as uuid, f.endpoint as endpoint
    # """
    # forums, _, _ = neo4j.neo4j_ops.neo4j_driver.execute_query(
    #     query, communityId=community_id
    # )

    forums = [
        {
            "uuid": "10392ca2-c721-4aba-a9c4-dcf112df4f03",
            "endpoint": "community.singularitynet.io",
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
    table_name = "discourse"

    latest_date_query = f"""
        SELECT (metadata_->> 'updatedAt')::timestamp
        AS latest_date
        FROM data_discourse
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

    node_parser = configure_node_parser(chunk_size=512)
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
    if len(doc_file_ids_to_delete) != 0:
        deletion_ids = tuple([float(item) for item in doc_file_ids_to_delete])
        deletion_query = f"""
            DELETE FROM data_discourse
            WHERE (metadata_->>'postId')::float IN {deletion_ids};
        """
        delete_data(deletion_query=deletion_query, dbname=dbname)

    embed_model = CohereEmbedding()
    embed_dim = 1024

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embed_dim,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "community_id", help="the community to save the gdrive data for it"
    )

    args = parser.parse_args()

    process_discourse_vectorstore(args.community_id)
