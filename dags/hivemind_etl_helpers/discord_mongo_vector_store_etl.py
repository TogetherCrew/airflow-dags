import argparse
import logging
from datetime import timedelta

from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    discord_raw_to_docuemnts,
)
from hivemind_etl_helpers.src.db.discord.find_guild_id import (
    find_guild_id_by_community_id,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.db.pg_db_utils import setup_db
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess


def process_discord_guild_mongo(community_id: str) -> None:
    """
    process the discord guild messages from mongodb
    and save the processed data within postgres

    Parameters
    -----------
    community_id : str
        the community id to create or use its database
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    guild_id = find_guild_id_by_community_id(community_id)
    logging.info(f"COMMUNITYID: {community_id}, GUILDID: {guild_id}")
    table_name = "discord"
    dbname = f"community_{community_id}"

    latest_date_query = f"""
            SELECT (metadata_->> 'date')::timestamp
            AS latest_date
            FROM data_{table_name}
            ORDER BY (metadata_->>'date')::timestamp DESC
            LIMIT 1;
    """
    from_date = setup_db(
        community_id=community_id, dbname=dbname, latest_date_query=latest_date_query
    )

    # because postgresql does not support miliseconds
    # we might get duplicate messages
    # so adding just a second after
    if from_date is not None:
        from_date += timedelta(seconds=1)

    documents = discord_raw_to_docuemnts(guild_id=guild_id, from_date=from_date)
    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    embed_model = CohereEmbedding()

    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        request_per_minute=10000,
        # max_request_per_day=REQUEST_PER_DAY,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "community_id", type=str, help="the Community that the guild is related to"
    )
    args = parser.parse_args()

    process_discord_guild_mongo(community_id=args.community_id)
