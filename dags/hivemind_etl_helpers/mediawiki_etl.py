import logging

from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.mediawiki.extractor import MediaWikiExtractor


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

    logging.info(f"Processing community id: {community_id}")
    mediawiki_extractor = MediaWikiExtractor(api_url)
    documents = mediawiki_extractor.extract(
        page_ids=page_titles,
    )

    ingestion_pipeline = CustomIngestionPipeline(
        community_id=community_id, collection_name="mediawiki"
    )
    ingestion_pipeline.run_pipeline(docs=documents)
