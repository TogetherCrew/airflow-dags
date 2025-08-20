import logging
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from hivemind_etl_helpers.src.db.discord.discord_summary import DiscordSummary
from hivemind_etl_helpers.src.db.discord.find_guild_id import (
    find_guild_id_by_platform_id,
)
from llama_index.core.response_synthesizers import get_response_synthesizer
from qdrant_client.http import models
from tc_hivemind_backend.db.mongo import MongoSingleton
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline
from traceloop.sdk import Traceloop


def cleanup_discord_summary_collections(community_id: str, platform_id: str) -> None:
    """
    Delete Qdrant collection and MongoDB collections for discord summaries.
    
    Parameters
    -----------
    community_id : str
        the community id to delete collections for
    platform_id : str
        discord platform id to delete collections for
    """
    logging.info(f"Starting cleanup for community_id: {community_id}, platform_id: {platform_id}")
    
    # Delete Qdrant collection
    qdrant_collection_name = f"{community_id}_{platform_id}_summary"
    logging.info(f"Deleting Qdrant collection: {qdrant_collection_name}")
    
    try:
        # Create a CustomIngestionPipeline instance to access the Qdrant client
        temp_pipeline = CustomIngestionPipeline(
            community_id=community_id, 
            collection_name=f"{platform_id}_summary",
            use_cache=False,
        )
        # Access the Qdrant client and delete the collection
        temp_pipeline.qdrant_client.delete_collection(collection_name=qdrant_collection_name)
        logging.info(f"Successfully deleted Qdrant collection: {qdrant_collection_name}")
    except Exception as e:
        logging.warning(f"Failed to delete Qdrant collection {qdrant_collection_name}: {e}")
    
    # Delete MongoDB collections
    mongo_db_name = f"docstore_{community_id}"
    logging.info(f"Deleting MongoDB collections in database: {mongo_db_name}")
    
    try:
        mongo_client = MongoSingleton.get_instance().client
        db = mongo_client[mongo_db_name]
        
        # Delete metadata collection
        metadata_collection_name = f"{platform_id}/metadata"
        if metadata_collection_name in db.list_collection_names():
            db.drop_collection(metadata_collection_name)
            logging.info(f"Successfully deleted MongoDB collection: {metadata_collection_name}")
        else:
            logging.info(f"MongoDB collection {metadata_collection_name} does not exist")
        
        # Delete data collection  
        data_collection_name = f"{platform_id}/data"
        if data_collection_name in db.list_collection_names():
            db.drop_collection(data_collection_name)
            logging.info(f"Successfully deleted MongoDB collection: {data_collection_name}")
        else:
            logging.info(f"MongoDB collection {data_collection_name} does not exist")
            
    except Exception as e:
        logging.warning(f"Failed to delete MongoDB collections in {mongo_db_name}: {e}")
    
    logging.info(f"Cleanup completed for community_id: {community_id}, platform_id: {platform_id}")


def process_discord_summaries(
    community_id: str,
    platform_id: str,
    selected_channels: list[str],
    default_from_date: datetime,
    from_start: bool = False,
    verbose: bool = False,
    cleanup_collections: bool = False,
) -> None:
    """
    prepare the discord data by grouping it into thread, channel and day
    and save the processed summaries into qdrant

    Note: This will always process the data until 1 day ago.

    Parameters
    -----------
    community_id : str
        the community id to create or use its database
    platform_id : str
        discord platform id
    selected_channels : list[str]
        a list of channels to start processing the data
    default_from_date : datetime
        the default from_date set in db
    from_start : bool
        whether to start from the beginning of the data or not
        default is `False`
    verbose : bool
        verbose the process of summarization or not
        if `True` the summarization process will be printed out
        default is `False`
    cleanup_collections : bool
        whether to delete existing Qdrant and MongoDB collections before processing
        if `True` will delete the collections, default is `False`
    """
    load_dotenv()
    otel_endpoint = os.getenv("TRACELOOP_BASE_URL")
    if otel_endpoint:
        logging.info(f"Initializing Traceloop with endpoint: {otel_endpoint}")
        Traceloop.init(app_name="hivemind-discord-summary", api_endpoint=otel_endpoint)

    guild_id = find_guild_id_by_platform_id(platform_id)
    logging.info(f"COMMUNITYID: {community_id}, GUILDID: {guild_id} | from_start: {from_start} | cleanup_collections: {cleanup_collections}")
    
    # Cleanup collections if requested
    if cleanup_collections:
        cleanup_discord_summary_collections(community_id, platform_id)
    
    collection_name = f"{platform_id}_summary"
    
    # Set up ingestion pipeline
    ingestion_pipeline = CustomIngestionPipeline(
        community_id=community_id,
        collection_name=collection_name,
        use_cache=False,
    )

    # Get latest date from Qdrant for daily summaries only
    # We filter for daily summaries by checking the type field
    latest_date = ingestion_pipeline.get_latest_document_date(
        field_name="date",
        field_schema=models.PayloadSchemaType.DATETIME,
    )

    if latest_date is not None and not from_start:
        # Convert string date to datetime if needed
        if isinstance(latest_date, str):
            latest_date = datetime.strptime(latest_date, "%Y-%m-%d")
        # Start from 1 day before so to catch all the last day data
        from_date = latest_date - timedelta(days=1)
        logging.info(f"Started extracting summaries from date: {from_date}!")
    else:
        # if no data was saved, start pre-processing from the given date on modules document
        from_date = default_from_date
        logging.info("Started extracting summaries from scratch!")

    discord_summary = DiscordSummary(
        response_synthesizer=get_response_synthesizer(response_mode="tree_summarize"),  # type: ignore
        verbose=verbose,
    )

    logging.info("Preparing summaries and streaming batches into Qdrant!")

    batch_index = 0
    for batch in discord_summary.stream_summary_documents(
        guild_id=guild_id,
        selected_channels=selected_channels,
        from_date=from_date,
        batch_size=50,
    ):
        logging.info(
            f"Processing streamed batch {batch_index} | size={len(batch)}"
        )
        # Ensure final sort in case upstream changed batch boundaries
        batch.sort(key=lambda d: d.metadata.get("date", "0000-00-00"))
        ingestion_pipeline.run_pipeline(docs=batch)
        batch_index += 1

    logging.info("Finished streaming summaries into Qdrant database!")
