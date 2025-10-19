"""
This DAG is used just for research purposes and not for production environment.
It generates daily/channel/thread Discord summaries and ingests them into a
separate research collection, allowing experimentation without touching prod data.
"""

import logging
import json
from datetime import datetime
from dateutil.parser import parse

from airflow import DAG
from airflow.decorators import task
from dotenv import load_dotenv

from llama_index.core import Settings
from llama_index.llms.openai import OpenAI

from hivemind_etl_helpers.discord_cleanup_collection import (
    cleanup_discord_summary_collections,
)
from hivemind_etl_helpers.src.db.discord.find_guild_id import (
    find_guild_id_by_platform_id,
)
from hivemind_etl_helpers.src.db.discord.discord_summary import DiscordSummary
from llama_index.core.response_synthesizers import get_response_synthesizer
from qdrant_client.http import models
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline
from tc_hivemind_backend.embeddings import CohereEmbedding

from llama_index.core.node_parser import SemanticSplitterNodeParser


with DAG(
    dag_id="discord_summary_vector_store_research",
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
        Extract required parameters for a single Discord platform run in research mode.
        """
        dag_params = kwargs["dag_run"].conf

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
        from_start = dag_params.get("from_start", False)

        # from and to dates are optional here; summaries DAG normally advances by latest date
        from_date = dag_params.get("from_date")
        from_date = parse(from_date) if from_date else None

        to_date = dag_params.get("to_date")
        to_date = parse(to_date) if to_date else None

        selected_channels = dag_params.get("selected_channels")
        if selected_channels is None:
            raise ValueError("selected_channels is required")
        selected_channels = json.loads(selected_channels)

        guild_id = dag_params.get("guild_id")
        if guild_id is None:
            raise ValueError("guild_id is required")

        logging.info(
            f"community_id: {community_id} | platform_id: {platform_id} | guild_id: {guild_id}"
        )
        logging.info(f"From date: {from_date} | To date: {to_date}")

        return [
            {
                "community_id": community_id,
                "platform_id": platform_id,
                "selected_channels": selected_channels,
                "from_date": from_date,
                "to_date": to_date,
                "cleanup_collections": cleanup_collections,
                "from_start": from_start,
                "guild_id": guild_id,
                "collection_name_postfix": collection_name_postfix,
            }
        ]

    @task
    def start_discord_summary_vectorstore(community_info: dict[str, str | datetime | list]):
        load_dotenv()
        Settings.llm = OpenAI(model="gpt-4o-mini-2024-07-18")

        community_id = community_info["community_id"]
        platform_id = community_info["platform_id"]
        selected_channels = community_info["selected_channels"]
        from_date = community_info["from_date"]
        from_start = community_info.get("from_start", False)
        cleanup_collections = community_info.get("cleanup_collections", False)
        collection_name_postfix = community_info.get("collection_name_postfix", "")
        to_date = community_info.get("to_date")

        # Map platform_id to the Discord guild
        guild_id = find_guild_id_by_platform_id(platform_id)

        # Research collection name uses a postfix to isolate from prod
        collection_name = f"{platform_id}_summary{collection_name_postfix}_research"

        logging.info(
            f"Processing (research) community_id: {community_id} | platform_id: {platform_id} | collection: {collection_name}"
        )

        # Cleanup collections if requested
        if cleanup_collections:
            logging.info(
                f"Cleaning up (research) collections for community_id: {community_id} | platform_id: {platform_id}"
            )
            # Use the base cleanup and rely on research suffix for isolation in Qdrant naming
            try:
                cleanup_discord_summary_collections(community_id, platform_id)
            except Exception as cleanup_error:  # pragma: no cover - defensive
                logging.warning(f"Cleanup failed (non-fatal): {cleanup_error}")

        embedding_model = CohereEmbedding(model_name="embed-v4.0")
        # Set up ingestion pipeline with research collection name
        ingestion_pipeline = CustomIngestionPipeline(
            community_id=community_id,
            collection_name=collection_name,
            use_cache=False,
            embed_model=embedding_model,
            embed_dim=1536,
        )

        # Resolve latest date from research collection for daily summaries
        latest_date = ingestion_pipeline.get_latest_document_date(
            field_name="date",
            field_schema=models.PayloadSchemaType.DATETIME,
        )

        if latest_date is not None and not from_start:
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date, "%Y-%m-%d")
            # Start from 1 day before to ensure completeness
            effective_from_date = latest_date
            logging.info(f"Continuing from latest (research): {effective_from_date}")
        else:
            effective_from_date = from_date
            logging.info(
                "Started extracting summaries (research) from provided from_date or scratch!"
            )

        # Prepare summarizer and stream batches
        discord_summary = DiscordSummary(
            response_synthesizer=get_response_synthesizer(response_mode="tree_summarize"),  # type: ignore
            verbose=False,
        )

        transformations_pipeline = [
            SemanticSplitterNodeParser(
                embed_model=embedding_model,
                buffer_size=5,
            ),
            embedding_model,
        ]

        logging.info("Preparing (research) summaries and streaming batches into Qdrant!")
        batch_index = 0
        for batch in discord_summary.stream_summary_documents(
            guild_id=guild_id,
            selected_channels=selected_channels,
            from_date=effective_from_date,
            batch_size=50,
            to_date=to_date,
        ):
            logging.info(f"Processing (research) streamed batch {batch_index} | size={len(batch)}")
            batch.sort(key=lambda d: d.metadata.get("date", "0000-00-00"))
            ingestion_pipeline.run_pipeline(
                docs=batch,
                transformations=transformations_pipeline,
            )
            batch_index += 1

        logging.info("Finished streaming (research) summaries into Qdrant database!")

    communities_info = get_discord_communities()
    start_discord_summary_vectorstore.expand(community_info=communities_info)


