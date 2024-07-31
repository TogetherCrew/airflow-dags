import logging

from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor


class NotionProcessor:
    def __init__(
        self,
        community_id: str,
        access_token: str | None = None,
    ) -> None:
        """
        Parameters
        -----------
        community_id : str
            the community to save its data
        access_token : str | None
            notion ingegration access token
        """
        self.community_id = community_id
        collection_name = "notion"

        # preparing the data extractor and ingestion pipelines
        self.notion_extractor = NotionExtractor(notion_token=access_token)
        self.ingestion_pipeline = CustomIngestionPipeline(
            self.community_id, collection_name=collection_name
        )

    def process(
        self,
        database_ids: list[str] | None = None,
        page_ids: list[str] | None = None,
    ) -> None:
        """
        extract the notion pages and database
        finally, load into a db ingestion pipeline

        Parameters
        ------------
        database_ids : list[str] | None
            the database ids to process its data
            default is None
        page_ids : list[str] | None
            the page ids to process their data
            default is None

        Note: One of `database_ids` or `page_ids` should be given.
        """
        if database_ids is None and page_ids is None:
            raise ValueError(
                "At least one of the `database_ids` or `page_ids` must be given!"
            )

        logging.info(f"Processing community id: {self.community_id}")
        documents = self.notion_extractor.extract(
            page_ids=page_ids, database_ids=database_ids
        )
        self.ingestion_pipeline.run_pipeline(docs=documents)

    def process_page(self, page_id: str) -> None:
        """
        extract just one notion page and load into a db ingestion pipeline

        Parameters
        ------------
        page_id : str
            the notion page id
        """
        logging.info(
            f"Processing page_id: {page_id}, of community id: {self.community_id}"
        )
        documents = self.notion_extractor.extract_from_pages(page_ids=[page_id])
        self.ingestion_pipeline.run_pipeline(docs=documents)

    def process_database(self, database_id: str) -> None:
        """
        extract just one notion database and load into a db ingestion pipeline

        Parameters
        ------------
        database_id : str
            the notion database id
        """
        logging.info(
            f"Processing database id: {database_id}, of community id: {self.community_id}"
        )
        documents = self.notion_extractor.extract_from_database(database_id=database_id)
        self.ingestion_pipeline.run_pipeline(docs=documents)
