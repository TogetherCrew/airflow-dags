import logging

from dags.hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from dags.hivemind_etl_helpers.src.db.mediawiki.extractor import MediaWikiExtractor


def process_mediawiki_etl(
    community_id: str,
    api_url: str,
    page_titles: list[str],
) -> None:
    """
    Process the MediaWiki pages or categories
    and save the processed data within PostgreSQL

    Parameters
    -----------
    community_id : str
        the community to save its data
    page_titles : list[str] | None
        the page titles to process their data
        default is None

    Note: The `page_titles` should be given.
    """
    if page_titles is None:
        raise ValueError("The `page_titles` must be given!")
    try:
        mediawiki_extractor = MediaWikiExtractor(api_url)
        documents = mediawiki_extractor.extract(
            page_titles=page_titles,
        )
    except TypeError as exp:
        logging.info(f"No documents retrieved from MediaWiki! exp: {exp}")

    table_name = "mediawiki"
    ingestion_pipeline = CustomIngestionPipeline(community_id, table_name=table_name)
    try:
        ingestion_pipeline.run_pipeline(docs=documents)
    except Exception as e:
        logging.info(f"Error while trying to run MediaWikiIngestionPipeline! exp: {e}")
