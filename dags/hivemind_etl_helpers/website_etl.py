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

    def extract(
          self, urls: list[str],
    ) -> list:
       """
       extract data
       """
       extracted_data = self.crawlee_client.crawl(urls)

       return extracted_data
    
    def transform(self, raw_data: list) -> list[Document]:
       # transforming
       pass

    def load(self, documents: list[Document]) -> None:
       # loading data into db
       self.ingestion_pipeline.run_pipeline(docs=documents)
