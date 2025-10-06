import logging
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline
from tc_hivemind_backend.db.mongo import MongoSingleton


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
        metadata_collection_name = f"{platform_id}_summary/metadata"
        if metadata_collection_name in db.list_collection_names():
            db.drop_collection(metadata_collection_name)
            logging.info(f"Successfully deleted MongoDB collection: {metadata_collection_name}")
        else:
            logging.info(f"MongoDB collection {metadata_collection_name} does not exist")
        
        # Delete data collection
        data_collection_name = f"{platform_id}_summary/data"
        if data_collection_name in db.list_collection_names():
            db.drop_collection(data_collection_name)
            logging.info(f"Successfully deleted MongoDB collection: {data_collection_name}")
        else:
            logging.info(f"MongoDB collection {data_collection_name} does not exist")
            
    except Exception as e:
        logging.warning(f"Failed to delete MongoDB collections in {mongo_db_name}: {e}")
    
    logging.info(f"Cleanup completed for community_id: {community_id}, platform_id: {platform_id}")


def cleanup_discord_vector_collections(community_id: str, platform_id: str) -> None:
    """
    Delete Qdrant collection and MongoDB collections for discord base vector store.

    Parameters
    -----------
    community_id : str
        the community id to delete collections for
    platform_id : str
        discord platform id to delete collections for
    """
    logging.info(
        f"Starting vector cleanup for community_id: {community_id}, platform_id: {platform_id}"
    )

    # Delete Qdrant collection (base vectors use collection name without suffix)
    qdrant_collection_name = f"{community_id}_{platform_id}"
    logging.info(f"Deleting Qdrant collection: {qdrant_collection_name}")

    try:
        temp_pipeline = CustomIngestionPipeline(
            community_id=community_id,
            collection_name=platform_id,
            use_cache=False,
        )
        temp_pipeline.qdrant_client.delete_collection(
            collection_name=qdrant_collection_name
        )
        logging.info(
            f"Successfully deleted Qdrant collection: {qdrant_collection_name}"
        )
    except Exception as e:
        logging.warning(
            f"Failed to delete Qdrant collection {qdrant_collection_name}: {e}"
        )

    # Delete MongoDB collections (shared naming without suffix)
    mongo_db_name = f"docstore_{community_id}"
    logging.info(f"Deleting MongoDB collections in database: {mongo_db_name}")

    try:
        mongo_client = MongoSingleton.get_instance().client
        db = mongo_client[mongo_db_name]

        metadata_collection_name = f"{platform_id}/metadata"
        if metadata_collection_name in db.list_collection_names():
            db.drop_collection(metadata_collection_name)
            logging.info(
                f"Successfully deleted MongoDB collection: {metadata_collection_name}"
            )
        else:
            logging.info(
                f"MongoDB collection {metadata_collection_name} does not exist"
            )

        data_collection_name = f"{platform_id}/data"
        if data_collection_name in db.list_collection_names():
            db.drop_collection(data_collection_name)
            logging.info(
                f"Successfully deleted MongoDB collection: {data_collection_name}"
            )
        else:
            logging.info(
                f"MongoDB collection {data_collection_name} does not exist"
            )
    except Exception as e:
        logging.warning(
            f"Failed to delete MongoDB collections in {mongo_db_name}: {e}"
        )

    logging.info(
        f"Vector cleanup completed for community_id: {community_id}, platform_id: {platform_id}"
    )
