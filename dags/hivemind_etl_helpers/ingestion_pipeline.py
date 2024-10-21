import logging
from datetime import datetime

from dateutil.parser import parse
from hivemind_etl_helpers.src.utils.credentials import load_redis_credentials
from hivemind_etl_helpers.src.utils.mongo import get_mongo_uri
from hivemind_etl_helpers.src.utils.redis import RedisSingleton
from llama_index.core import Document, MockEmbedding
from llama_index.core.ingestion import (
    DocstoreStrategy,
    IngestionCache,
    IngestionPipeline,
)
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.core.schema import BaseNode
from llama_index.storage.docstore.mongodb import MongoDocumentStore
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from qdrant_client.conversions import common_types as qdrant_types
from qdrant_client.http import models
from tc_hivemind_backend.db.credentials import load_postgres_credentials
from tc_hivemind_backend.db.qdrant import QdrantSingleton
from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams
from tc_hivemind_backend.embeddings.cohere import CohereEmbedding
from tc_hivemind_backend.qdrant_vector_access import QDrantVectorAccess


class CustomIngestionPipeline:
    def __init__(self, community_id: str, collection_name: str, testing: bool = False):
        self.community_id = community_id
        self.qdrant_client = QdrantSingleton.get_instance().client

        _, self.embedding_dim = load_model_hyperparams()
        self.pg_creds = load_postgres_credentials()
        self.redis_cred = load_redis_credentials()
        self.collection_name = f"{community_id}_{collection_name}"
        self.platform_name = collection_name

        self.embed_model = (
            CohereEmbedding()
            if not testing
            else MockEmbedding(embed_dim=self.embedding_dim)
        )
        self.redis_client = RedisSingleton.get_instance().get_client()

    def run_pipeline(self, docs: list[Document]) -> list[BaseNode]:
        """
        vectorize and ingest data into a qdrant collection

        Note: This will handle duplicate documents by doing an upsert operation.

        Parameters
        ------------
        docs : list[llama_index.Document]
            list of llama-index documents

        Returns
        ---------
        nodes : list[BaseNode]
            The set of transformed and loaded Nodes/Documents
            (transformation is chunking and embedding of data)
        """
        # qdrant is just collection based and doesn't have any database
        logging.info(
            f"{len(docs)} docuemnts was extracted and now loading into QDrant DB!"
        )
        vector_access = QDrantVectorAccess(collection_name=self.collection_name)
        vector_store = vector_access.setup_qdrant_vector_store()

        pipeline = IngestionPipeline(
            transformations=[
                SemanticSplitterNodeParser(embed_model=self.embed_model),
                self.embed_model,
            ],
            docstore=MongoDocumentStore.from_uri(
                uri=get_mongo_uri(),
                db_name=f"docstore_{self.community_id}",
                namespace=self.platform_name,
            ),
            vector_store=vector_store,
            cache=IngestionCache(
                cache=RedisCache.from_redis_client(self.redis_client),
                collection=f"{self.collection_name}_ingestion_cache",
                docstore_strategy=DocstoreStrategy.UPSERTS,
            ),
            docstore_strategy=DocstoreStrategy.UPSERTS,
        )
        logging.info("Pipeline created, now inserting documents into pipeline!")

        nodes = pipeline.run(documents=docs, show_progress=True)
        return nodes

    def _create_payload_index(
        self,
        field_name: str,
        field_schema: qdrant_types.PayloadSchemaType,
    ) -> qdrant_types.UpdateResult:
        """
        Creates an index on a field under the payload of points in qdrant db

        Note: this could be used for payload fields that we want to scroll for after

        Parameters
        ------------
        field_name : str
            the field name under points' payload to create the index for
        field_schema : qdrant_client.conversions.common_types.PayloadSchemaType
            the schema type of the field

        Returns
        -----------
        operation_result : qdrant_client.conversions.common_types.UpdateResult
            the payload index creation type
        """
        operation_result = self.qdrant_client.create_payload_index(
            collection_name=self.collection_name,
            field_name=field_name,
            field_schema=field_schema,
        )

        return operation_result

    def get_latest_document_date(self, field_name: str) -> datetime | None:
        """
        get the latest date for the most recent available document

        NOTE: the given `field_name` under the points' schema MUST CONTAIN A VALUE HAVING DATE FORMAT (or a string format). If not, errors might raise in result of this function

        Parameters
        ------------
        field_name : str
            the datetime field name in qdrant points' payload

        Returns
        ---------
        latest_date : datetime.datetime | None
            the datetime for the document containing the latest date
            if no document or any errors raised, we would return `None`
        """
        latest_date: datetime | None = None
        try:
            result = self._create_payload_index(
                field_name=field_name,
                field_schema=models.PayloadSchemaType.DATETIME,
            )
            if result.status.name == "COMPLETED":
                latest_document = self.qdrant_client.scroll(
                    collection_name=self.collection_name,
                    limit=1,
                    with_payload=True,
                    order_by=models.OrderBy(
                        key=field_name,
                        direction=models.Direction.DESC,
                    ),
                )

                if not latest_document[0]:
                    logging.info("No documents found in the collection.")
                    latest_date = None
                else:
                    latest_date = parse(latest_document[0][0].payload[field_name])

            else:
                raise ValueError(
                    f"Index not created successfully! index creation result: {result}"
                )
        except Exception as exp:
            logging.error(f"Error: {exp} while loading latest point!")
            latest_date = None

        return latest_date
