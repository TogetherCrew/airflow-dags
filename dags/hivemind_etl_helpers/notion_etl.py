import copy
import logging

from llama_index.core import Document
from hivemind_etl_helpers.src.db.notion.extractor import NotionExtractor
from tc_hivemind_backend.ingest_qdrant import CustomIngestionPipeline


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
        transformed_docs = self._transform_documents(documents=documents)
        self.ingestion_pipeline.run_pipeline(docs=transformed_docs)

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
        transformed_docs = self._transform_documents(documents=documents)
        self.ingestion_pipeline.run_pipeline(docs=transformed_docs)

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
        transformed_docs = self._transform_documents(documents=documents)
        self.ingestion_pipeline.run_pipeline(docs=transformed_docs)

    def _transform_documents(self, documents: list[Document]) -> list[Document]:
        """
        transform notion extracted documents by inserting their metadata a url

        Parameters
        ------------
        documents : list[Document]
            a list of notion extracted pages

        Returns
        ---------
        documents : list[Document]
            a list of documents each inlcuded with url in its metadata
        """
        # Copying
        transformed_docs: list[Document] = copy.deepcopy(documents)

        for doc in transformed_docs:
            page_id: str | None = doc.metadata.get("page_id")
            if page_id is None:
                doc.metadata["url"] = None
            else:
                doc.metadata["url"] = f"https://www.notion.so/{page_id}"

        return transformed_docs
