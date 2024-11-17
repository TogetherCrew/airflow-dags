from typing import Any

import hashlib
from hivemind_etl_helpers.ingestion_pipeline import CustomIngestionPipeline
from llama_index.core import Document
from hivemind_etl_helpers.src.db.website.crawlee_client import CrawleeClient


class WebsiteETL:
    def __init__(
        self,
        community_id: str,
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
        collection_name = "website"

        # preparing the data extractor and ingestion pipelines
        self.crawlee_client = CrawleeClient()
        self.ingestion_pipeline = CustomIngestionPipeline(
            self.community_id, collection_name=collection_name
        )

    async def extract(
          self, urls: list[str],
    ) -> list[dict[str, Any]]:
        """
        Extract given urls

        Parameters
        -----------
        urls : list[str]
            a list of urls

        Returns
        ---------
        extracted_data : list[dict[str, Any]]
            The crawled data from urls
        """
        extracted_data = await self.crawlee_client.crawl(urls)

        return extracted_data
    
    def transform(self, raw_data: list[dict[str, Any]]) -> list[Document]:
        """
        transform raw data to llama-index documents

        Parameters
        ------------
        raw_data : list[dict[str, Any]]
            crawled data

        Returns
        ---------
        documents : list[llama_index.Document]
            list of llama-index documents
        """
        documents: list[Document] = []

        for data in raw_data:
            doc_id = data["url"]
            doc = Document(
                doc_id=doc_id,
                text=data["inner_text"],
                metadata={
                    "title": data["title"],
                    "url": data["url"],
                }
            )
            documents.append(doc)

        return documents

    def load(self, documents: list[Document]) -> None:
        """
        load the documents into the vector db

        Parameters
        -------------
        documents: list[llama_index.Document]
            the llama-index documents to be ingested
        """
        # loading data into db
        self.ingestion_pipeline.run_pipeline(docs=documents)

    
    def _prepare_id(data: str) -> str:
        """
        hash the given data to prepare an id for the document

        Parameters
        -----------
        data : str
            the string data to be hashed

        Returns
        --------
        hashed_data : str
            data hashed using sha256 algorithm
        """
        hashed_data = hashlib.sha256(data.encode('utf-8'))
        return hashed_data.hexdigest()
