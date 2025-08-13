import logging
from datetime import datetime, timedelta

from hivemind_etl_helpers.src.db.discord.discord_raw_message_to_document import (
    discord_raw_to_documents,
)
from hivemind_etl_helpers.src.db.discord.find_guild_id import (
    find_guild_id_by_platform_id,
)
from qdrant_client.http import models
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline


def process_discord_guild_mongo(
    community_id: str,
    platform_id: str,
    selected_channels: list[str],
    default_from_date: datetime,
    from_start: bool = False,
) -> None:
    """
    process the discord guild messages from mongodb
    and save the processed data within qdrant

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
    from_date : bool
        whether to start from the beginning of the data or not
        default is `False`
    """
    guild_id = find_guild_id_by_platform_id(platform_id)
    logging.info(
        f"COMMUNITYID: {community_id}, GUILDID: {guild_id} | from_start: {from_start}"
    )
    
    collection_name = platform_id
    
    # Set up ingestion pipeline
    ingestion_pipeline = CustomIngestionPipeline(
        community_id=community_id, collection_name=collection_name
    )

    # Get latest date from Qdrant
    latest_date = ingestion_pipeline.get_latest_document_date(
        field_name="date",
        field_schema=models.PayloadSchemaType.FLOAT,
    )

    # because qdrant might have precision differences 
    # we might get duplicate messages
    # so adding just a second after
    if latest_date is not None and not from_start:
        from_date = latest_date + timedelta(seconds=1)
        logging.info(f"Started extracting from date: {from_date}!")
    else:
        # if no data was processed
        # start from the time set in database
        from_date = default_from_date
        logging.info("Started extracting data from scratch!")

    documents = discord_raw_to_documents(
        guild_id=guild_id,
        from_date=from_date,
        selected_channels=selected_channels,
    )
    
    logging.info(f"Extracted {len(documents)} messages!")

    # Process and load data into Qdrant
    # do a batch of 50
    for i in range(0, len(documents), 50):
        batch = documents[i:i+50]
        logging.info(f"Processing batch {i//50}/{len(documents)//50}")
        ingestion_pipeline.run_pipeline(docs=batch)

    logging.info("Finished loading into Qdrant database!")
