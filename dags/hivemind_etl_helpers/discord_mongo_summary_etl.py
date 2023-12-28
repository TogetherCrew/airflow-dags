import argparse
import logging

from hivemind_etl_helpers.src.db.discord.discord_summary import DiscordSummary
from hivemind_etl_helpers.src.db.discord.find_guild_id import (
    find_guild_id_by_community_id,
)
from hivemind_etl_helpers.src.document_node_parser import configure_node_parser
from hivemind_etl_helpers.src.utils.pg_vector_access import PGVectorAccess
from llama_index.response_synthesizers import get_response_synthesizer
from tc_hivemind_backend.db.pg_db_utils import setup_db
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding


def process_discord_summaries(community_id: str, verbose: bool = False) -> None:
    """
    prepare the discord data by grouping it into thread, channel and day
    and save the processed summaries into postgresql

    Note: This will always process the data until 1 day ago.

    Parameters
    ------------
    community_id : str
        the community id to process its guild data
    verbose : bool
        verbose the process of summarization or not
        if `True` the summarization process will be printed out
        default is `False`
    """
    chunk_size, embedding_dim = load_model_hyperparams()
    guild_id = find_guild_id_by_community_id(community_id)
    logging.info(f"COMMUNITYID: {community_id}, GUILDID: {guild_id}")
    table_name = "discord_summary"
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

    discord_summary = DiscordSummary(
        response_synthesizer=get_response_synthesizer(response_mode="tree_summarize"),
        verbose=verbose,
    )

    (
        thread_summaries_documents,
        channel_summary_documenets,
        daily_summary_documenets,
    ) = discord_summary.prepare_summaries(
        guild_id=guild_id,
        from_date=from_date,
        summarization_prefix="Please make a concise summary based only on the provided text from this",
    )

    logging.info("Getting the summaries embedding and saving within database!")

    node_parser = configure_node_parser(chunk_size=chunk_size)
    pg_vector = PGVectorAccess(table_name=table_name, dbname=dbname)

    embed_model = CohereEmbedding()

    # saving thread summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=thread_summaries_documents,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        request_per_minute=10000,
    )

    # saving daily summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=daily_summary_documenets,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        request_per_minute=10000,
    )

    # saving channel summaries
    pg_vector.save_documents_in_batches(
        community_id=community_id,
        documents=channel_summary_documenets,
        batch_size=100,
        node_parser=node_parser,
        max_request_per_minute=None,
        embed_model=embed_model,
        embed_dim=embedding_dim,
        request_per_minute=10000,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "community_id", type=str, help="the Community that the guild is related to"
    )
    args = parser.parse_args()
    process_discord_summaries(community_id=args.community_id)
