"""
This DAG is used just for research purposes. and not for production environment
"""
import logging
import json
from datetime import datetime
from dateutil.parser import parse
from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import discord_raw_to_documents
from hivemind_etl_helpers.discord_cleanup_collection import (
    cleanup_discord_vector_collections,
)
from tc_hivemind_backend.embeddings import CohereEmbedding
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline

from llama_index.core import Settings
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.llms.openai import OpenAI


with DAG(
    dag_id="discord_vector_store_etl_research",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    params={
        "from_start": False,
        "community_id": None,
        "platform_id": None,
        "cleanup_collections": False,
        "from_date": None,
        "to_date": None, 
        "guild_id": None,
        "selected_channels": None,
        "collection_name_postfix": None,
    },
) as dag:

    @task
    def get_discord_communities(**kwargs) -> list[dict[str, str | datetime | list]]:
        """
        Getting all communities having discord from database
        """
        dag_params = kwargs["dag_run"].conf

        # from_start = dag_params.get("from_start", False)
        community_id = dag_params.get("community_id")
        platform_id = dag_params.get("platform_id")
        collection_name_postfix = dag_params.get("collection_name_postfix")
        if collection_name_postfix is None:
            collection_name_postfix = ""

        if platform_id is None:
            raise ValueError("platform_id is required")

        if community_id is None:
            raise ValueError("community_id is required")

        cleanup_collections = dag_params.get("cleanup_collections", False)

        # from a specific date
        from_date = dag_params.get("from_date")
        from_date = parse(from_date) if from_date else None

        # to a specific date
        to_date = dag_params.get("to_date")
        to_date = parse(to_date) if to_date else None


        selected_channels = dag_params.get("selected_channels")
        if selected_channels is None:
            raise ValueError("selected_channels is required")

        selected_channels = json.loads(selected_channels)

        # the specific guild id of a discord server
        guild_id = dag_params.get("guild_id")
        if guild_id is None:
            raise ValueError("guild_id is required")

        logging.info(
            f"community_id: {community_id} | platform_id: {platform_id} | guild_id: {guild_id}"
        )
        logging.info(
            f"From date: {from_date} | To date: {to_date}"
        )

        communities = [
            {
                "community_id": community_id,
                "platform_id": platform_id,
                "selected_channels": selected_channels,
                "from_date": from_date,
                "to_date": to_date,
                "cleanup_collections": cleanup_collections,
                "guild_id": guild_id,
                "collection_name_postfix": collection_name_postfix,
            }
        ]
        
        return communities

    @task
    def start_discord_vectorstore(community_info: dict[str, str | datetime | list]):
        load_dotenv()
        community_id = community_info["community_id"]
        platform_id = community_info["platform_id"]
        selected_channels = community_info["selected_channels"]
        from_date = community_info["from_date"]
        to_date = community_info["to_date"]
        guild_id = community_info["guild_id"]
        collection_name_postfix = community_info["collection_name_postfix"]
        cleanup_collections = community_info.get("cleanup_collections", False)
        Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

        logging.info(
            f"Processing community_id: {community_id} | platform_id: {platform_id}"
        )

        # Cleanup collections if requested
        if cleanup_collections:
            logging.info(f"Cleaning up collections for community_id: {community_id} | platform_id: {platform_id + '_research'}")
            cleanup_discord_vector_collections(community_id, platform_id + collection_name_postfix + "_research")
        
        logging.info(f"Extracting messages for community_id: {community_id} | platform_id: {platform_id + '_research'}")
        docs = discord_raw_to_documents(
            guild_id=guild_id,
            selected_channels=selected_channels,
            from_date=from_date,
            to_date=to_date,
        )
        logging.info(f"Extracted {len(docs)} messages for community_id: {community_id} | platform_id: {platform_id + '_research'}")

        embedding_model = CohereEmbedding(model_name="embed-v4.0")

        ingestion_pipeline = CustomIngestionPipeline(
            community_id=community_id,
            collection_name=platform_id + collection_name_postfix + "_research",
            use_cache=False,
            embedding_model=embedding_model,
            embed_dim=1536,
        )

        transformations_pipeline = [
            SemanticSplitterNodeParser(
                embed_model=embedding_model,
                buffer_size=5,
            ),
            embedding_model,
        ]

        for i in range(0, len(docs), 50):
            batch = docs[i:i+50]
            logging.info(f"Processing batch {i//50}/{len(docs)//50}")
            ingestion_pipeline.run_pipeline(
                docs=batch,
                transformations_pipeline=transformations_pipeline,
            )
        logging.info("Finished embedding the documents!")

    communities_info = get_discord_communities()
    # `start_discord_vectorstore` will be called multiple times
    # with the length of the list
    start_discord_vectorstore.expand(community_info=communities_info)

