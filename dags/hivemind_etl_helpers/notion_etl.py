import logging

from dags.hivemind_etl_helpers.notion_ingestion_etl import (
    NotionIngestionPipeline
)
from dags.hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


def process_notion_etl(
    community_id: str, database_ids: list[str] | None = None,
    page_ids: list[str] | None = None
) -> None:
    """
    process the notion files
    and save the processed data within postgresql

    Parameters
    -----------
    community_id : str
        the community to save its data
    database_ids : list[str] | None
        the database ids to process its data
        default is None
    page_ids : list[str] | None
        the page ids to process their data
        default is None

    Note: One of `database_ids` or `page_ids` should be given.
    """
    if database_ids is None and page_ids is None:
        raise ValueError("At least one of the `database_ids` or `page_ids` must be given!")
    try:
        notion_extractor = NotionExtractor()
        documents = notion_extractor.extract(page_ids=page_ids,
                                             database_ids=database_ids)
    except TypeError as exp:
        logging.info(f"No documents retrieved from notion! exp: {exp}")

    ingestion_pipeline = NotionIngestionPipeline(community_id)
    try:
        ingestion_pipeline.run_pipeline(documents=documents)
    except Exception as e:
        logging.info(f"Error while trying to run NotionIngestionPipeline! exp: {e}")